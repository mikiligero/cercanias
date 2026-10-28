# Descarga y colocación del histórico

## Elegir y obtener los archivos

Abre [Descargar datos almacenados](https://cercanias.mikines.es/?pagina=descargas), la opción «Descargar datos» de la barra lateral o el enlace del menú ⋮ → About.

1. Elige bruto `.pb`, Parquet o ambos.
2. Selecciona la fecha inicial y final. Las fechas corresponden a días UTC, igual que las carpetas conservadas; el día en curso es parcial.
3. Pulsa **Descargar ZIP**. Recibirás un único archivo con todos los días seleccionados, directamente desde el navegador.

Por defecto se incluyen todas las fuentes y una copia del GTFS estático disponible. El apartado «Elegir fuentes y referencia GTFS» permite modificarlo. Esa referencia tiene una fecha propia y puede ser posterior al período elegido. Las instrucciones de colocación están en otro desplegable y dentro del ZIP.

Los tamaños de la página proceden del inventario y se muestran antes de comprimir. Cada descarga fija un corte temporal al iniciarse y recorre los archivos por particiones, omitiendo los modificados después del corte o en los cinco segundos anteriores. Añade `manifest.json` con las rutas, tamaños y SHA-256 reales, y `LEEME-DATOS.md` con instrucciones.

Se conserva el contenido original: mensajes vacíos, anomalías y retrasos fuera del filtro analítico permanecen en los paquetes. Los paquetes no están recortados a Madrid. La falta de avisos en un período no equivale a ausencia de incidencias.

## Colocarlos en el proyecto

Clona o descarga este repositorio. Descomprime el ZIP **en la carpeta principal**, junto a `dashboard.py` y `config.yaml`. El archivo ya contiene todas las carpetas de los días elegidos; conserva sus nombres internos.

```text
cercanias/
  dashboard.py
  config.yaml
  data/
    raw/
      vehicle_positions/AAAA-MM-DD/vehicle_positions_...pb
      trip_updates/AAAA-MM-DD/trip_updates_...pb
      alerts/AAAA-MM-DD/alerts_...pb
      gtfs_static/fomento_transit.zip
    bronze/
      vehicle_positions/date=AAAA-MM-DD/part-...parquet
      trip_updates/date=AAAA-MM-DD/part-...parquet
      alerts/date=AAAA-MM-DD/part-...parquet
```

Por ejemplo, desde la carpeta del proyecto:

```bash
python -m zipfile -e /ruta/al/paquete.zip .
```

Puedes consultar un Parquet directamente con DuckDB o pandas. Para el cuadro completo necesitas Parquet de las tres fuentes y la referencia GTFS. Si un período no tiene avisos, descarga otro día que sí los contenga; la interfaz indicará la antigüedad del dato, sin presentarlo como reciente. El script principal de captura puede preparar las fuentes estáticas actuales si faltan.

## Reprocesar únicamente el bruto descargado

Con el entorno de Python y las dependencias de README.md preparados:

```bash
python reprocess_downloads.py --feed trip_updates --start 2026-10-01 --end 2026-10-07
```

Repite el comando para `vehicle_positions` o `alerts` cuando corresponda. Las fechas son UTC. Lee las rutas de `config.yaml`, conserva los originales y omite los Parquet que ya existan. No descarga nuevos datos ni sobrescribe resultados existentes. Los mensajes vacíos no generan Parquet. Si un original no se puede interpretar o su nombre no coincide con la fecha de su cabecera, se informa y no se publica un resultado para él.

Este reprocesado usa el parser de la versión del repositorio. No garantiza que reproduzca futuras versiones del parser; la reproducción de una captura real se comprueba en «Del dato al indicador».

## Arranque y límites del servicio de descarga

```bash
python serve_dashboard.py --server.headless true --server.port 8501
```

La entrada añade el endpoint `/_anticipa/datos` a la aplicación HTTP de Streamlit y mantiene el mismo puerto. No modifica los paquetes instalados ni requiere cambios en el proxy. La integración está comprobada con las versiones fijadas Streamlit 1.41.1 y Tornado 6.5.5; antes de actualizarlas deben ejecutarse las pruebas HTTP y comprobar WebSockets.

Los ZIP del período se escriben directamente hacia el cliente, sin guardar otra copia del histórico. Se recorre una partición cada vez. Los índices ZIP y el manifiesto utilizan memoria acotada y ficheros temporales de metadatos, que se eliminan al terminar o cancelar. La central del ZIP se genera con ZipFile de Python 3.12 y ZIP64, usando un índice temporal en lugar de mantener en RAM todos los registros del histórico. Se permiten dos descargas simultáneas; los clientes adicionales reciben HTTP 429 con indicación de reintento. Las descargas grandes pueden tardar y requieren mantener la conexión.

El nuevo endpoint de período no aplica los antiguos topes diarios de 40.000 archivos y 2 GiB. Los enlaces diarios antiguos siguen funcionando por compatibilidad y mantienen sus límites.

El endpoint acepta exclusivamente las fuentes y formatos publicados, fechas válidas y la copia GTFS conocida. No permite elegir rutas del servidor ni exporta enlaces simbólicos, copias de recuperación, configuraciones o cachés. Si un archivo cambia durante la lectura, se interrumpe el ZIP y debe repetirse la descarga. Se cancela cuando el cliente cierra la conexión.

El menú nativo de Streamlit solo permite configurar opciones predeterminadas. El enlace se incluye en About y el acceso directo de la barra lateral abre la página de selección en otra pestaña. El cuadro mantiene su actualización manual.
