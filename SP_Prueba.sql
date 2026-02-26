PROCEDURE SP_ORDEN_PETICIONES (
    PNI_NUM_TRANSACCIONES IN NUMBER,
    PNI_ESTATUS           IN NUMBER,
    PNO_CDG_MSG           OUT NUMBER,
    PSO_DESC_MSG          OUT VARCHAR2
) IS
    -- Usamos una tabla para los registros capturados
    TYPE t_tab_ascc IS TABLE OF ASCC.ASCC_BIT_DISPATCHER%ROWTYPE;
    l_registros t_tab_ascc;
    
    vNumeroOrden    NUMBER;
    vCdgMsgHist     NUMBER;
    vDesMsgHist     VARCHAR2(150);
	v_error_msg     VARCHAR2(4000);
    V_VALIDA        NUMBER(1);
    V_NUM_ORD_EX    NUMBER;
	
BEGIN

    -- Inicialización de variables de salida
    PNO_CDG_MSG  := 0;
    PSO_DESC_MSG := '';
	
    -- PASO 1: Captura atómica de registros (Nadie más puede tocar estos IDs)
    UPDATE ASCC.ASCC_BIT_DISPATCHER
    SET ESTATUS = 2, -- Marcamos como "En Proceso" 
        PROCESS_DATE = SYSDATE
    WHERE ID IN (
        SELECT ID FROM (
            SELECT ID FROM ASCC.ASCC_BIT_DISPATCHER
            WHERE ESTATUS = PNI_ESTATUS
              AND SUBSCRIBER_ID IS NOT NULL
              AND CANCEL_DATE <= (SYSDATE - 1)
			  ORDER BY CANCEL_DATE ASC
        )
        WHERE ROWNUM <= PNI_NUM_TRANSACCIONES
        FOR UPDATE SKIP LOCKED -- Solo toma los que nadie más tenga bloqueados
    )
    RETURNING 
        ID, SUBSCRIBER_ID, OBJETO_ID, CANCEL_DATE, MDN, 
        OFFER_ID, OFFER_NAME, PROCESS_DATE, ESTATUS, 
        DESC_ESTATUS, TIPO_CANCEL, ORIGINAL
    BULK COLLECT INTO l_registros;

    -- PASO 2: Procesar lo capturado
    IF l_registros.COUNT > 0  THEN
        FOR j IN 1..l_registros.COUNT LOOP
		
			-- Creamos un punto de recuperación por cada registro del ciclo
			SAVEPOINT sp_registro_individual;
            
			BEGIN
				-- PASO 2.1: Inserta registro en TBL MTX_CANCELATION_HIST
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
	
				-- PASO 2.1: Validación de existencia en TBL ASCC_ORDEN
				OB_TIENE_REG_ORDEN(l_registros(j).MDN, l_registros(j).SUBSCRIBER_ID, V_VALIDA, V_NUM_ORD_EX);
	
				IF (V_VALIDA = 1) THEN
					UPDATE ASCC.ASCC_BIT_DISPATCHER 
					SET ESTATUS = 4, 
						DESC_ESTATUS = 'ERROR: YA EXISTE ORDEN EN LA TABLA ASCC_ORDEN ' || V_NUM_ORD_EX,
						PROCESS_DATE = SYSDATE
					WHERE ID = l_registros(j).ID;
					
					PNO_CDG_MSG := 1;
					PSO_DESC_MSG := 'LAS ORDENES YA EXISTEN: ' || V_NUM_ORD_EX || ' DN: ' || l_registros(j).MDN || ' SUBSCRIBER ID: '|| l_registros(j).SUBSCRIBER_ID || chr(13) || ' ' || PSO_DESC_MSG;
				ELSE
					-- PASO 2.2: Inserta registro en TBL ASCC_ORDEN                
					INSERT INTO ASCC.ASCC_ORDEN (
							NUMERO_ORDEN, USUARIO_ALTA, FECHA_CAPTURA, SUBCATEGORIA_ID,
							CATEGORIA_ID, DN, FECHA_MODIFICA, USUARIO_MODIFICA,                                                                       
							ESTATUS_ORDEN, CANTIDAD_EQUIPOS, SUBSCRIBER_ID                                                                   
					)VALUES (
							ASCC.SEQ_ASCC_NUMERO_ORDEN.NEXTVAL, 'm13537', SYSDATE, 15,
							11, l_registros(j).MDN, SYSDATE, 'm13537',
							'Abierta', 0, l_registros(j).SUBSCRIBER_ID
					)RETURNING NUMERO_ORDEN INTO vNumeroOrden;                
					
						
					-- PASO 2.3: Inserta proceso del 18 al 24 en TBL ASCC_PETICION					
					FOR k IN 18..24 LOOP
						INSERT INTO ASCC.ASCC_PETICION (
								PETICION_ID, NUMERO_ORDEN, ID_PROCESO, ID_SERVICIO,
								ID_ESTATUS, FECHA_CREACION, PARAMETROS_IN, DN                                                                              
						)VALUES(
								ASCC.SEQ_ASCC_PETICION.NEXTVAL, vNumeroOrden, k, k,
								11, SYSDATE,
								'{"statusChangeTime":"' || TO_CHAR(l_registros(j).CANCEL_DATE, 'YYYY-MM-DD"T"HH24:MI:SS') || 
								'","subscriberId":"' || l_registros(j).SUBSCRIBER_ID || '"}',
								l_registros(j).MDN
						);
					END LOOP;
						
					-- PASO 3: Inserta proceso del 18 al 24 en TBL ASCC_PETICION
					UPDATE ASCC.ASCC_BIT_DISPATCHER 
					SET ESTATUS=3,
						DESC_ESTATUS=SUBSTR(TRIM('EXITO: SE INSERTO Y ACTUALIZO DE MANERA CORRECTA LA TABLA ORDEN Y PETICIONES, CON NUMERO DE ORDEN : ' || vNumeroOrden || ' EN AMBAS TABLAS. ' || DESC_ESTATUS),1,499),
						PROCESS_DATE = SYSDATE
					WHERE ID=l_registros(j).ID;
					
					PNO_CDG_MSG := 0;
					PSO_DESC_MSG := 'EXITO: AL INSERTAR EN LA TABLA DE ORDEN y PETICIONES. ' || l_registros(j).MDN || ' CON SUBSCRIBER_ID ' || l_registros(j).SUBSCRIBER_ID || chr(13) || PSO_DESC_MSG; 					
					
				END IF;
				
			EXCEPTION
                WHEN OTHERS THEN
                    -- Si algo falla, regresamos al savepoint de este registro solamente
                    ROLLBACK TO sp_registro_individual;
                    
                    v_error_msg := SQLERRM;
                    
                    -- Registramos el error en la tabla dispatcher para que no se quede en estatus 2
                    UPDATE ASCC.ASCC_BIT_DISPATCHER
                    SET ESTATUS = 4,
                        DESC_ESTATUS = SUBSTR('ERROR AL INS. REG. EN LA TABLA ORDEN Y/O PETICION: ' || v_error_msg, 1, 499),
                        PROCESS_DATE = SYSDATE
                    WHERE ID = l_registros(j).ID;
                    
                    PNO_CDG_MSG := 1;
                    PSO_DESC_MSG := SUBSTR(PSO_DESC_MSG || 'Error en ID ' || l_registros(j).ID || ': ' || v_error_msg || '| ', 1, 3900);
            END;
        END LOOP;    
    END IF;
	
	COMMIT; -- Un solo COMMIT por lote para eficiencia y consistencia

EXCEPTION
    WHEN OTHERS THEN
        ROLLBACK;
        PNO_CDG_MSG := -1;
        PSO_DESC_MSG := 'Error: ' || SQLERRM;
		RAISE;
END SP_ORDEN_PETICIONES; 
