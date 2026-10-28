# El piloto en cifras

Vista de Streamlit para demostrar la captura y conservación del histórico real de Anticipa.
El botón lateral **Actualizar datos** recalcula las particiones modificadas. El resto de
interacciones usa un resumen JSON y no vuelve a consultar todo el histórico.

## Inventario

- Origen: archivos de las tres fuentes GTFS-RT bajo `data/raw` y `data/bronze`.
- Resumen: `data/inventory/pilot.json`, excluido de Git con el resto de `data/`.
- Días con capturas: fechas UTC con originales o Parquet conservados en alguna fuente.
- Snapshots: número de archivos originales `.pb`, sumado por fuente; no son peticiones HTTP.
- Filas: suma de filas declaradas en los metadatos de los Parquet legibles. No son viajes únicos.
- Tamaño: bytes lógicos de originales y Parquet, en unidades decimales. Excluye GTFS estático y copias de recuperación.
- Los Parquet cuyo metadato no puede leerse se cuentan aparte y sus filas no se inventan.
- El gráfico incluye fechas intermedias sin archivos; la jornada actual es parcial.
- La ausencia de snapshots nuevos de alertas no permite atribuir una caída al origen.

La selección del período del EDA (17/05–03/07/2026) vuelve a contar los archivos conservados
actualmente en esas fechas; no reproduce sin más las cifras congeladas de la memoria.
El CSV incluye la fecha del inventario, fuente, fecha UTC, recuentos y tamaños.

## Ejecución

Desde el proyecto y su entorno Python:

```sh
venv/bin/python -m src.pilot_inventory
```

Recuento completo, incluso de directorios sin cambios:

```sh
venv/bin/python -m src.pilot_inventory --force
```

El índice reutiliza particiones cuya carpeta no ha cambiado; una sustitución atómica de
Parquet o la creación de un archivo invalida esa partición. Si se modifica un archivo
existente sin cambiar la carpeta, debe ejecutarse el recuento completo.
Un bloqueo impide dos recuentos simultáneos y el JSON se publica por sustitución atómica.
Si un recuento falla, se conserva el resumen anterior.

Las pruebas específicas están en `tests/test_pilot_inventory.py` y `tests/test_pilot_view.py`.
