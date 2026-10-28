# Sistema y calidad

Sustituye la vista Sistema por un panel independiente de observabilidad y comprobación.
No crea conexiones al histórico completo al abrirlo.

## Lectura de resultados

- El estado de los servicios se obtiene de systemd al consultar la vista. Un servicio activo no garantiza el éxito de todas las peticiones.
- La edad del snapshot se obtiene de la marca temporal del último original de posiciones conservado. No es latencia extremo a extremo.
- El calendario usa el inventario de originales, después de deduplicación. No transforma los recuentos de archivos en disponibilidad porcentual.
- La continuidad examina posiciones y actualizaciones de viajes. Registra intervalos mayores de 120 segundos, incluidos cruces de medianoche, y los recorta al período mostrado. No atribuye la causa a Renfe. Las alertas no siguen la misma interpretación.
- Los días sin capturas se calculan sobre el calendario completo del período elegido, incluyendo fechas sin carpeta.

## Comprobación de un día

El botón **Comprobar el día seleccionado** evalúa solo sus particiones y conserva el resultado
con fecha de ejecución. Pulsar **Actualizar datos** actualiza el inventario y la continuidad;
no modifica los informes de días ya comprobados.

Se comprueba la lectura de metadatos de Parquet, campos ausentes, límites de coordenadas,
posiciones 0,0 y retrasos fuera de −300 a 7.200 segundos (rango exploratorio, no descarte automático).
Las repeticiones de claves se cuentan como filas adicionales sobre la primera de cada grupo:

- Posiciones: `snapshot_ts, entity_id`.
- Actualizaciones: `snapshot_ts, entity_id, stop_id, stop_sequence`.

Esas repeticiones no afirman que todos los campos de las filas sean iguales. Las reglas pueden
solaparse. Una tabla sin registros se marca como **Sin registros**, no como prueba superada.
Los fallos de consulta se conservan en el informe y se muestran como comprobación incompleta.

El cruce de GTFS compara los mismos IDs distintos de posiciones, excluyendo IDs vacíos,
contra la copia estática conservada: coincidencia exacta y tras normalizar espacios en ambos lados.
La población no cambia entre esos porcentajes. No es cobertura de circulaciones planificadas,
ni demuestra reconciliación con versiones históricas. La fecha de conservación del GTFS se muestra.

## Evidencias y actualización

- `data/inventory/continuity.json`: resumen incremental por carpeta de originales.
- `data/inventory/quality-YYYY-MM-DD.json`: informe descargable de un día.
- Publicación atómica y bloqueo para impedir dos análisis simultáneos del mismo tipo.
- Los archivos originales y procesados no se modifican ni se eliminan durante las comprobaciones.
- Las copias de recuperación incluyen archivos reconstruidos: su número no mide pérdidas definitivas.

Las pruebas están en `tests/test_quality_data.py` y `tests/test_quality_view.py`.

## Ajustes de la demo (28/10/2026)

El estado de la última descarga correcta se conserva en data/status por fuente, separado de la fecha del proveedor. La continuidad muestra los intervalos completos y la intersección con 05:30–00:30 de Madrid como referencia, sin asumir horarios oficiales comunes. La comprobación de un día abre los originales sin Parquet y distingue mensajes sin registros, con registros y no legibles. Los informes anteriores requieren repetir la comprobación para incluir esa auditoría.
