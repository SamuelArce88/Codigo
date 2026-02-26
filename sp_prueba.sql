PROCEDURE SP_ORDEN_PETICIONES (
    PNI_NUM_TRANSACCIONES IN NUMBER,
    PNI_ESTATUS           IN NUMBER,
    PNO_CDG_MSG           OUT NUMBER,
    PSO_DESC_MSG          OUT VARCHAR2
) IS
    TYPE t_tab_ascc IS TABLE OF ASCC.ASCC_BIT_DISPATCHER%ROWTYPE;
    l_registros t_tab_ascc := t_tab_ascc();

    vNumeroOrden    NUMBER;
    vCdgMsgHist     NUMBER;
    vDesMsgHist     VARCHAR2(150);
    v_error_msg     VARCHAR2(4000);
    V_NUM_ORD_EX    NUMBER;

BEGIN

    PNO_CDG_MSG  := 0; -- 0 = sin registros (CODIGO_EXITO en Java → dormir)
    PSO_DESC_MSG := '';

    -- PASO 1: Bloquear, actualizar y capturar datos en un solo cursor.
    -- FOR UPDATE SKIP LOCKED solo es válido al final de un SELECT de nivel superior;
    -- no puede usarse dentro de un UPDATE ni combinarse con RETURNING BULK COLLECT.
    -- Solución: cursor FOR LOOP con FOR UPDATE SKIP LOCKED + UPDATE por ID + colección manual.
    FOR rec IN (
        SELECT * FROM (
            SELECT * FROM ASCC.ASCC_BIT_DISPATCHER
            WHERE ESTATUS = PNI_ESTATUS
              AND SUBSCRIBER_ID IS NOT NULL
              AND CANCEL_DATE <= (SYSDATE - 1)
            ORDER BY CANCEL_DATE ASC
        )
        WHERE ROWNUM <= PNI_NUM_TRANSACCIONES
        FOR UPDATE SKIP LOCKED
    ) LOOP
        -- Marcar como "En Proceso"
        UPDATE ASCC.ASCC_BIT_DISPATCHER
        SET ESTATUS = 2,
            PROCESS_DATE = SYSDATE
        WHERE ID = rec.ID;

        -- Agregar fila a la colección para procesarla en PASO 2
        l_registros.EXTEND;
        l_registros(l_registros.COUNT) := rec;
    END LOOP;

    -- Sin registros disponibles → señal para que Java duerme
    IF l_registros.COUNT = 0 THEN
        COMMIT;
        RETURN;
    END IF;

    -- PASO 2: Procesar registros capturados
    FOR j IN 1..l_registros.COUNT LOOP

        SAVEPOINT sp_registro_individual;

        BEGIN
            -- PASO 2.1: Historial
            SP_INS_REG_HIST(
                l_registros(j).SUBSCRIBER_ID,
                l_registros(j).OBJETO_ID,
                l_registros(j).CANCEL_DATE,
                l_registros(j).MDN,
                l_registros(j).OFFER_ID,
                l_registros(j).OFFER_NAME,
                vCdgMsgHist,
                vDesMsgHist);

            PSO_DESC_MSG := PSO_DESC_MSG || vDesMsgHist;

            -- FIX: MERGE reemplaza OB_TIENE_REG_ORDEN + INSERT
            -- Elimina la race condition TOCTOU: el check y el insert son una operación atómica
            SELECT ASCC.SEQ_ASCC_NUMERO_ORDEN.NEXTVAL INTO vNumeroOrden FROM DUAL;

            MERGE INTO ASCC.ASCC_ORDEN tgt
            USING (SELECT l_registros(j).MDN        AS dn,
                          l_registros(j).SUBSCRIBER_ID AS sub_id
                   FROM DUAL) src
            ON (tgt.DN = src.dn AND tgt.SUBSCRIBER_ID = src.sub_id)
            WHEN NOT MATCHED THEN
                INSERT (
                    NUMERO_ORDEN, USUARIO_ALTA, FECHA_CAPTURA, SUBCATEGORIA_ID,
                    CATEGORIA_ID, DN, FECHA_MODIFICA, USUARIO_MODIFICA,
                    ESTATUS_ORDEN, CANTIDAD_EQUIPOS, SUBSCRIBER_ID
                ) VALUES (
                    vNumeroOrden, 'm13537', SYSDATE, 15,
                    11, l_registros(j).MDN, SYSDATE, 'm13537',
                    'Abierta', 0, l_registros(j).SUBSCRIBER_ID
                );

            IF SQL%ROWCOUNT = 0 THEN
                -- La orden ya existía (otra sesión la insertó concurrentemente)
                SELECT NUMERO_ORDEN INTO V_NUM_ORD_EX
                FROM ASCC.ASCC_ORDEN
                WHERE DN = l_registros(j).MDN
                  AND SUBSCRIBER_ID = l_registros(j).SUBSCRIBER_ID;

                UPDATE ASCC.ASCC_BIT_DISPATCHER
                SET ESTATUS = 4,
                    DESC_ESTATUS = 'ERROR: YA EXISTE ORDEN EN LA TABLA ASCC_ORDEN ' || V_NUM_ORD_EX,
                    PROCESS_DATE = SYSDATE
                WHERE ID = l_registros(j).ID;

                PNO_CDG_MSG := 1;
                PSO_DESC_MSG := 'LAS ORDENES YA EXISTEN: ' || V_NUM_ORD_EX ||
                                ' DN: ' || l_registros(j).MDN ||
                                ' SUBSCRIBER ID: ' || l_registros(j).SUBSCRIBER_ID ||
                                chr(13) || ' ' || PSO_DESC_MSG;
            ELSE
                -- PASO 2.2: Inserta peticiones 18-24
                FOR k IN 18..24 LOOP
                    INSERT INTO ASCC.ASCC_PETICION (
                        PETICION_ID, NUMERO_ORDEN, ID_PROCESO, ID_SERVICIO,
                        ID_ESTATUS, FECHA_CREACION, PARAMETROS_IN, DN
                    ) VALUES (
                        ASCC.SEQ_ASCC_PETICION.NEXTVAL, vNumeroOrden, k, k,
                        11, SYSDATE,
                        '{"statusChangeTime":"' || TO_CHAR(l_registros(j).CANCEL_DATE, 'YYYY-MM-DD"T"HH24:mi:SS') ||
                        '","subscriberId":"' || l_registros(j).SUBSCRIBER_ID || '"}',
                        l_registros(j).MDN
                    );
                END LOOP;

                UPDATE ASCC.ASCC_BIT_DISPATCHER
                SET ESTATUS = 3,
                    DESC_ESTATUS = SUBSTR(TRIM('EXITO: SE INSERTO Y ACTUALIZO DE MANERA CORRECTA LA TABLA ORDEN Y PETICIONES, CON NUMERO DE ORDEN : ' ||
                                              vNumeroOrden || ' EN AMBAS TABLAS. ' || DESC_ESTATUS), 1, 499),
                    PROCESS_DATE = SYSDATE
                WHERE ID = l_registros(j).ID;

                -- FIX: retorna 3 (procesado con éxito) en lugar de 0
                -- En Java: 3 ≠ CODIGO_EXITO → continúa procesando sin dormir
                -- Agregar Constantes.CODIGO_PROCESADO = 3 en la clase Constantes
                PNO_CDG_MSG := 3;
                PSO_DESC_MSG := 'EXITO: AL INSERTAR EN LA TABLA DE ORDEN y PETICIONES. ' ||
                                l_registros(j).MDN || ' CON SUBSCRIBER_ID ' ||
                                l_registros(j).SUBSCRIBER_ID || chr(13) || PSO_DESC_MSG;
            END IF;

        EXCEPTION
            WHEN OTHERS THEN
                ROLLBACK TO sp_registro_individual;
                v_error_msg := SQLERRM;

                UPDATE ASCC.ASCC_BIT_DISPATCHER
                SET ESTATUS = 4,
                    DESC_ESTATUS = SUBSTR('ERROR AL INS. REG. EN LA TABLA ORDEN Y/O PETICION: ' || v_error_msg, 1, 499),
                    PROCESS_DATE = SYSDATE
                WHERE ID = l_registros(j).ID;

                PNO_CDG_MSG := 1;
                PSO_DESC_MSG := SUBSTR(PSO_DESC_MSG || 'Error en ID ' || l_registros(j).ID ||
                                       ': ' || v_error_msg || '| ', 1, 3900);
        END;

    END LOOP;

    COMMIT;

EXCEPTION
    WHEN OTHERS THEN
        ROLLBACK;
        PNO_CDG_MSG := -1;
        PSO_DESC_MSG := 'Error: ' || SQLERRM;
        RAISE;
END SP_ORDEN_PETICIONES;