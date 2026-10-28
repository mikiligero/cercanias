# Fuentes de datos — Cercanías Renfe

Documento de referencia para arrancar un nuevo proyecto (análisis, ML, dashboard, etc.) sobre los datos ya recopilados del sistema de Cercanías/Rodalies de Renfe. Pensado para dar contexto a Claude Code en un repo nuevo.

---

## 1. Feeds GTFS-RT (tiempo real)

Renfe publica 3 feeds en formato Protobuf (GTFS Realtime), actualizados en origen cada pocos segundos. Este proyecto los descarga cada **30s**.

| Feed | URL | Contenido |
|---|---|---|
| `vehicle_positions` | `https://gtfsrt.renfe.com/vehicle_positions.pb` | Posición GPS y estado de cada tren en circulación |
| `trip_updates` | `https://gtfsrt.renfe.com/trip_updates.pb` | Retrasos por parada (fuente de verdad oficial de puntualidad) |
| `alerts` | `https://gtfsrt.renfe.com/alerts.pb` | Incidencias activas (causa, efecto, rutas/paradas afectadas) |

No requieren autenticación. Se parsean con la librería `gtfs-realtime-bindings` (`google.transit.gtfs_realtime_pb2`).

### 1.1 `vehicle_positions` — esquema tras parseo

Una fila por entidad (tren) por snapshot.

| Campo | Tipo | Notas |
|---|---|---|
| `snapshot_ts` | int (epoch) | Timestamp de la cabecera del feed (identifica el snapshot) |
| `ingested_at` | datetime UTC | Momento de ingesta local |
| `entity_id` | str | Id de la entidad dentro del feed |
| `trip_id` | str/None | **route_id, start_date y start_time vienen vacíos en Renfe** — la línea se deriva del `vehicle_label` (ej. `C1-23530-PLATF.(2)`) o por join con GTFS estático vía `trip_id` |
| `vehicle_id` / `vehicle_label` | str | Identificador y etiqueta del vehículo |
| `latitude` / `longitude` / `bearing` / `speed_ms` | float | Posición GPS |
| `stop_id` | str/None | Próxima parada |
| `current_stop_sequence` | int | |
| `current_status` | int | 0=INCOMING_AT, 1=STOPPED_AT, 2=IN_TRANSIT_TO |
| `vehicle_ts` | int/None | Timestamp propio del vehículo |

### 1.2 `trip_updates` — esquema tras parseo

Una fila por (entidad, stop_time_update). **Renfe publica una sola STU por entidad** (solo la próxima parada, no el itinerario completo).

| Campo | Tipo | Notas |
|---|---|---|
| `snapshot_ts`, `ingested_at`, `entity_id`, `trip_id`, `vehicle_id` | — | igual que arriba |
| `stop_sequence` / `stop_id` | | |
| `arrival_delay` / `departure_delay` | int (segundos) | **`arrival_delay` es la fuente de verdad oficial de retraso** |
| `arrival_time` / `departure_time` | int (epoch) | |
| `schedule_relationship` | int | 0=SCHEDULED, 1=SKIPPED, 2=NO_DATA |

### 1.3 `alerts` — esquema tras parseo

Una fila por alerta.

| Campo | Tipo | Notas |
|---|---|---|
| `cause` | int | Enum GTFS-RT: 1=UNKNOWN_CAUSE … 9=MAINTENANCE |
| `effect` | int | Enum GTFS-RT: 1=UNKNOWN_EFFECT … 9=MODIFIED_SERVICE |
| `active_start` / `active_end` | int epoch / None | `end=0` en origen se normaliza a `None` (sin fecha de fin) |
| `informed_routes` / `informed_stops` | str (CSV) | Ids concatenados por coma cuando hay varios |
| `header_text` / `description_text` | str | **`header_text` suele venir vacío en Renfe; el texto real está en `description_text`**, en español |

---

## 2. GTFS estático (planificación / topología de red)

- **URL**: `https://ssl.renfe.com/ftransit/Fichero_CER_FOMENTO/fomento_transit.zip`
- Se renueva automáticamente cada 7 días (lógica en `main.py`).
- Contenido del ZIP (formato GTFS estándar):

| Fichero | Tamaño aprox. | Uso típico |
|---|---|---|
| `agency.txt` | 0.3 KB | Datos del operador |
| `calendar.txt` | 68 KB | Días de servicio por `service_id` |
| `routes.txt` | 95 KB | Líneas (C1, C2, R1...) — `route_id` ↔ nombre/color |
| `shapes.txt` | 4.5 MB | Geometría de trazados (para mapas) |
| `stops.txt` | 175 KB | Catálogo de estaciones (id, nombre, lat/lon) |
| `stop_times.txt` | **249 MB** | Horario teórico completo: parada programada por viaje — clave para calcular retraso real vs. teórico |
| `transfers.txt` | 3.3 KB | Transbordos entre paradas |
| `trips.txt` | 18 MB | Viajes: `trip_id` ↔ `route_id`, `service_id`, dirección |

**Uso principal en este proyecto**: enriquecer los feeds en tiempo real (que solo traen ids) con nombre de línea, nombre de estación y horario teórico, vía DuckDB (`src/query.py`).

⚠️ `stop_times.txt` es el fichero más pesado del proyecto — tenerlo en cuenta si se hace una copia del repo o se sube a git (normalmente debería ir en `.gitignore`).

---

## 3. Catálogos de estaciones por núcleo (data.renfe.com — Open Data)

CSVs estáticos, uno por núcleo de Cercanías/Rodalies, descargados una sola vez como referencia (nombre, código, coordenadas de cada estación). Portal: **data.renfe.com** (open data oficial de Renfe, distinto del feed en tiempo real).

| Núcleo | URL |
|---|---|
| Madrid | `https://data.renfe.com/dataset/0b763a1a-860d-4346-8818-765d2559248f/resource/daa68d9b-77cf-4024-890f-285d31184c5a/download/listado-estaciones-cercanias-madrid.csv` |
| Barcelona (Rodalies) | `https://data.renfe.com/dataset/8bdd06a5-f7f6-4f21-9cb6-b76f39842726/resource/e3969f12-aff2-468e-9b82-a65a6dd1a0d3/download/listado-estaciones-rodalies-barcelona.csv` |
| Valencia | `https://data.renfe.com/dataset/b5a039a1-4f02-4f9c-b617-a94d55779647/resource/0b1caf38-e6a9-4965-a94d-9a24c5a4ae84/download/listado-estaciones-cercanias-valencia.csv` |
| Sevilla | `https://data.renfe.com/dataset/d13549fe-f132-4633-aa50-5ed70b2f71dd/resource/ac3b4229-1c69-412f-a6de-29df913ece92/download/listado-estaciones-cercanias-sevilla.csv` |
| Bilbao | `https://data.renfe.com/dataset/0356d125-ddd2-4db2-9f10-3083c91f3e2a/resource/3254a1ed-a2b7-4fd1-9e9e-23eefe35f162/download/listado-estaciones-cercanias-bilbao.csv` |
| Murcia/Alicante | `https://data.renfe.com/dataset/9b7b24d0-57d0-489c-bcd6-cb78e96135e1/resource/1d17b744-4c68-4578-bdc3-b51457892967/download/listado-estaciones-cercanias-murcia-alicante.csv` |
| Zaragoza | `https://data.renfe.com/dataset/479fa2dd-76e0-4a97-ac88-b848943f9133/resource/9651ce18-9437-4274-ad04-143f1cc9e6db/download/listado-estaciones-cercanias-zaragoza_2.csv` |
| Málaga | `https://data.renfe.com/dataset/9cd08424-f617-4adb-a230-68abdb2c8abf/resource/4b651d25-3fed-4f6a-be4d-18352d7e2aea/download/listado-estaciones-cercanias-malaga-v4.csv` |
| Asturias | `https://data.renfe.com/dataset/a94a67b4-c353-417c-b471-284998bc257d/resource/6b73014a-e5bd-4b0e-b17b-1f8abe47a90f/download/listado-estaciones-cercanias-asturias.csv` |
| Cádiz | `https://data.renfe.com/dataset/d2197774-6671-45a7-ad3b-eadac1b66475/resource/2c3afbbd-30f6-41b1-815a-6ef452457f92/download/cercanias-cadiz-.csv` |
| Santander | `https://data.renfe.com/dataset/aa5cf888-f8b9-4824-b60a-7ae3ba6ec32b/resource/18dd8342-b68e-4846-9e52-1a96faf85e80/download/listado-estaciones-cercanias-santander.csv` |
| San Sebastián | `https://data.renfe.com/dataset/f3dc3712-cc1e-4b23-ada0-50562e9cf38b/resource/e8e8d882-865e-4032-8d39-d60cd0cfcdff/download/listado-estaciones-cercanias-san-sebastian.csv` |

---

## 4. Datos ya acumulados (histórico local)

Este repo ya tiene un histórico ingerido desde **2026-05-17** en adelante:

- **Raw** (`data/raw/{feed}/{YYYY-MM-DD}/*.pb`): ficheros Protobuf crudos, un `.pb` por snapshot (~30s de cadencia → ~2880/día por feed). ~7.8 GB actualmente.
- **Bronze** (`data/bronze/{feed}/date=YYYY-MM-DD/part-*.parquet`): mismo contenido parseado a Parquet (Snappy), particionado Hive por fecha. ~6.5 GB actualmente.
- Reparto aproximado por feed (bronze, a fecha del último análisis): `alerts` ~947 MB, `vehicle_positions` ~759 MB, `trip_updates` ~484 MB (crecen día a día).

Se puede consultar todo con DuckDB directamente sobre los Parquet sin necesidad de cargarlo en memoria (ver `src/query.py` como referencia de vistas ya construidas, p.ej. join de `trip_updates` + GTFS estático para obtener nombre de línea/parada junto al retraso).

---

## 5. Notas para un proyecto nuevo

- **Ninguna fuente requiere API key** — todo es acceso HTTP directo sin autenticación.
- El **join crítico** para cualquier análisis de puntualidad es: `trip_updates.trip_id` → `trips.txt.trip_id` → `routes.txt.route_id` (nombre de línea) y `trip_updates.stop_id` → `stops.txt.stop_id` (nombre de estación).
- `arrival_delay` en `trip_updates` es la métrica de puntualidad ya calculada por Renfe — no hace falta derivarla comparando con `stop_times.txt`, salvo que se quiera validar/cruzar contra el horario teórico.
- Si el nuevo proyecto no necesita replicar la ingesta, probablemente le baste con: (a) una copia/subset de `data/bronze/**/*.parquet`, y (b) el GTFS estático (`stops.txt`, `routes.txt`, `trips.txt`) para los joins. `stop_times.txt` y `shapes.txt` solo son necesarios si se trabaja con horarios teóricos o geometría de trazados.
