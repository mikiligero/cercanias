# Del dato al indicador

Vista guiada sobre una captura real de `trip_updates`: original Protobuf, fila conservada,
cruce con GTFS y resumen de la captura con la misma función SQL de la vista en vivo.

## Ejemplo de la demo

El ejemplo utiliza la captura del 27/10/2026 a las 07:30:10 UTC, con 362 entidades y 395
observaciones de paradas. Se conserva bajo `data/demo/del-dato-al-indicador`, fuera de Git,
junto a la copia GTFS utilizada. `demo.json` identifica los archivos, huellas SHA-256 y filas
que ilustran coincidencia exacta del viaje y necesidad de normalizar espacios.
Actualizar la página no cambia el ejemplo ni reemplaza su horario de referencia.

Para preparar otro ejemplo desde el entorno del proyecto:

```python
from pathlib import Path
from src.config import load
from src.trace_data import freeze_demo

cfg = load()
freeze_demo(cfg, Path('ruta/al/original.pb'), Path('ruta/al/part-snapshot.parquet'))
```

La preparación exige que el original reproduzca el Parquet. Conserva copias por versión
identificada por fecha y hashes. No sustituye una copia existente cuyo contenido difiera.
El horario es el conservado al preparar el ejemplo; no se presenta como GTFS histórico
versionado cuando no hay evidencia de ello.

## Exploración y comprobación

La exploración usa el inventario para elegir día y solo lee la captura seleccionada.
Se vuelve a ejecutar el parser sobre el original sin escribir un nuevo Parquet. Se compara
número de filas, columnas y valores, en el mismo orden. Se excluye `ingested_at`: documenta
el procesamiento original y sería distinto al repetirlo. Las discrepancias bloquean la
presentación de una trazabilidad verificada. Una entidad con varias paradas conserva el
vínculo por índice de entidad y de actualización de parada, no solo por ID.

El original se muestra decodificado como JSON. La presencia de `arrival.delay` se comprueba
en el Protobuf: un campo no informado no se presenta como un cero explícito de Renfe. Si el
parser existente guarda un cero predeterminado, se identifica en la vista. El panel no
reescribe el histórico para corregirlo.

El cruce con GTFS conserva los identificadores de entrada. Prueba primero coincidencia
exacta y después normalización de espacios en ambos lados. Los casos ambiguos no se
resuelven eligiendo arbitrariamente la primera fila.

## Indicadores y exportación

El resumen se calcula sobre este archivo, no sobre el estado actual de toda la red.
Se cuentan observaciones con `arrival_delay` no nulo. La media positiva utiliza únicamente
valores mayores que cero, divididos por su número de observaciones y por 60. Son estimaciones
de llegada publicadas por Renfe, no llegadas consumadas ni predicciones de Anticipa.

El ZIP contiene original completo, Parquet, CSV procesado, observación, reglas y referencias
GTFS utilizadas. El horario completo queda conservado en el servidor e identificado por
SHA-256. El JSON permite seguir el índice de fila y contrastar cada campo con su origen.

Pruebas: `tests/test_trace_data.py` y `tests/test_trace_view.py`.

Los casos preparados priorizan observaciones de Madrid y valores cercanos a tres minutos
cuando existen en la captura, para facilitar la explicación. Ilustran el proceso; no son
una muestra representativa del servicio. Los indicadores se calculan sobre toda la captura,
incluidas las observaciones sin correspondencia GTFS.

## Comparación con el filtro analítico (28/10/2026)

El resumen de la captura reproduce el archivo sin filtrar. Debajo se muestra el efecto del intervalo −5 a 120 minutos aplicado por los indicadores del panel, con los recuentos incluidos, fuera del intervalo y sin valor. El JSON de evidencia incluye analytical_filter dentro de capture_indicator. Los originales y la captura fijada permanecen iguales.
