package att.com.mx.dispatcherCancellation.therad;
​
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
​
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;
​
import att.com.mx.dispatcherCancellation.repository.OrdenPeticionRepository;
import att.com.mx.dispatcherCancellation.util.Constantes;
import att.com.mx.dispatcherCancellation.util.UtilStackTrace;
​
@Service
public class ejecutaCancellationToASCC implements Runnable {
	private static final Logger LOGs = LoggerFactory.getLogger(ejecutaCancellationToASCC.class);
​
	// FIX: No necesita ser static; @Service es singleton por defecto en Spring
	private final AtomicBoolean registroPorProcesar = new AtomicBoolean(true);
	private static final DateTimeFormatter FORMATO_FECHA = DateTimeFormatter.ofPattern("dd/MM/yyyy HH:mm:ss");
​
	@Autowired
	private OrdenPeticionRepository ordenPeticionesRepository;
​
	public boolean isRegistro() {
		return registroPorProcesar.get();
	}
​
	public void setRegistroPorCancelar(boolean registro) {
		registroPorProcesar.set(registro);
	}
​
	@Override
	public void run() {
		ExecutorService executor = Executors.newFixedThreadPool(
				Constantes.NUM_HILOS,
				r -> {
					Thread t = new Thread(r);
					t.setName("ASCC-Pool-" + t.getId());
					return t;
				});
​
		LOGs.info("Ejecutando el proceso de inyecciones a las tablas ORDEN y PETICION.");
​
		try {
			while (true) {
				processRecords(executor);
				if (!isRegistro()) {
					setRegistroPorCancelar(true);
					LOGs.info("Sin registros por procesar en la base de datos.");
					try {
						Thread.sleep(1000 * 60);
					} catch (InterruptedException e) {
						Thread.currentThread().interrupt();
						LOGs.warn("Sleep interrumpido en el ciclo principal.");
					}
				}
			}
		} catch (Exception e) {
			LOGs.error("Exception - BTDispatcherCancellation: " + UtilStackTrace.recortarStackTrace(this, e));
		} finally {
			shutdownExecutor(executor);
		}
	}
​
	private void processRecords(ExecutorService executor) {
		List<Callable<ResultadoSP>> tareas = new ArrayList<>();
		String fechaInicio = LocalDateTime.now().format(FORMATO_FECHA);
​
		for (int i = 0; i < Constantes.NUM_HILOS; i++) {
			final int numeroTarea = i + 1;
			tareas.add(() -> {
				// FIX: captura el nombre del hilo del pool, no del hilo principal
				String threadName = Thread.currentThread().getName();
​
				Integer[] localCdgMsg = new Integer[1];
				String[] localDescMsg = new String[1];
​
				LOGs.info("Hilo {} ejecutando SP... Tarea # {}", threadName, numeroTarea);
				ordenPeticionesRepository.executeSpOrdenPeticiones(
						1,
						Constantes.CDG_ESTATUS,
						localCdgMsg,
						localDescMsg
				);
​
				return new ResultadoSP(localCdgMsg[0], localDescMsg[0], threadName);
			});
		}
​
		try {
			List<Future<ResultadoSP>> futuros = executor.invokeAll(tareas, 60, TimeUnit.SECONDS);
​
			for (Future<ResultadoSP> future : futuros) {
				try {
					if (future.isCancelled()) {
						LOGs.error("Una tarea fue cancelada por timeout.");
						continue;
					}
​
					ResultadoSP resultado = future.get();
					procesarResultadoIndividual(resultado);
					// FIX: usa resultado.nombreHilo (hilo real del pool)
					LOGs.info("Tarea terminada por: {}, inicio: {}", resultado.nombreHilo, fechaInicio);
​
				} catch (ExecutionException e) {
					LOGs.error("Error en la lógica de la tarea: ", e.getCause());
				}
			}
		} catch (InterruptedException e) {
			Thread.currentThread().interrupt();
			LOGs.error("El proceso principal fue interrumpido.");
		}
	}
​
	private void procesarResultadoIndividual(ResultadoSP resultado) {
		if (resultado.codigo == null) {
			LOGs.warn("Hilo {}: Sin código. Mensaje={}", resultado.nombreHilo, resultado.mensaje);
			return;
		}
​
		// FIX: condición corregida — CODIGO_EXITO significa "sin registros pendientes → dormir"
		if (Constantes.CODIGO_EXITO.equals(resultado.codigo)) {
			setRegistroPorCancelar(false);
		} else {
			LOGs.info("Éxito en {}. Código={}, Mensaje={}", resultado.nombreHilo, resultado.codigo, resultado.mensaje);
			LOGs.info("Finalizado: {}", LocalDateTime.now().format(FORMATO_FECHA));
		}
	}
​
	class ResultadoSP {
		Integer codigo;
		String mensaje;
		String nombreHilo;
​
		ResultadoSP(Integer codigo, String mensaje, String nombreHilo) {
			this.codigo = codigo;
			this.mensaje = mensaje;
			this.nombreHilo = nombreHilo;
		}
	}
​
	private void shutdownExecutor(ExecutorService executor) {
		try {
			executor.shutdown();
			if (!executor.awaitTermination(60, TimeUnit.SECONDS)) {
				executor.shutdownNow();
				if (!executor.awaitTermination(60, TimeUnit.SECONDS)) {
					LOGs.error("Los hilos no se detuvieron correctamente en ProcessFile");
				}
			}
		} catch (InterruptedException e) {
			Thread.currentThread().interrupt();
			LOGs.error("Error al esperar la terminación de los hilos en ProcessFile: "
					+ UtilStackTrace.recortarStackTrace(this, e));
		}
	}
}