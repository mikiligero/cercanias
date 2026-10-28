# 6.2 Calidad de los datos — EDA inicial (datos reales captura propia)

**Período analizado:** 17 mayo – 3 julio 2026 (47 días)  
**Fuente:** Pipeline GTFS-RT propio, feeds `vehicle_positions`, `trip_updates` y `alerts` de Renfe Open Data  
**Fecha del análisis:** 3 julio 2026

---

## Volumetría

| Métrica | Valor |
|---|---|
| Días con datos | 47 (17 may – 3 jul 2026) |
| Snapshots/día — media | ~2.387 (cadencia ~30 s, deduplicación por `header.timestamp`) |
| Total registros `vehicle_positions` en Bronze | ~18,8 millones |
| Archivos `.pb` raw capturados (VP + TU) | ~128.000 |
| Tamaño Bronze — `alerts` | 2,5 GB |
| Tamaño Bronze — `vehicle_positions` | 2,1 GB |
| Tamaño Bronze — `trip_updates` | 1,6 GB |
| **Total Bronze (3 feeds)** | **~6,2 GB** |
| Total datos en disco (raw + bronze) | ~14 GB |

El sistema captura del orden de **~400.000 posiciones de tren al día** en el núcleo de Madrid (vehicle_positions), almacenadas en Parquet particionado por fecha (Hive layout `date=YYYY-MM-DD`).

---

## Frescura y disponibilidad del feed

- **44 de 47 días** con captura ≥ 2.000 snapshots (≥ 87% de la ventana diaria operativa). Solo 2 días degradados (<1.000 snapshots).
- **1 corte total identificado:** 2026-06-03, los tres feeds simultáneamente a 0 snapshots (caída del servicio Open Data de Renfe).
  - Recuperación parcial el 2026-06-04: solo 23% capturado (~528 snapshots).
  - Recuperación casi completa el 2026-06-05: 74% (~1.687 snapshots).
- En días normales: **< 4–5 gaps > 2 min en todo el día** (sobre muestra de 4 días no consecutivos de junio). Gap máximo puntual aislado: ~105 min (vinculado al reinicio tras el corte del día 03). Cadencia media entre snapshots consecutivos: **~31 s**.
- Conclusión: el feed es robusto en condiciones normales; las interrupciones son cortas y esporádicas, gestionables con reintentos y control de huecos en la ingesta.

---

## Cobertura: circulaciones planificadas vs. feed RT

| Métrica | Valor |
|---|---|
| Viajes activos en GTFS estático (día laborable tipo, ej. 2026-07-01 miércoles) | 3.720 |
| `trip_id` únicos observados en feed RT ese mismo día | 3.777 |
| Coincidencia directa RT ↔ GTFS estático | **~77,7%** de los viajes planificados |
| Desajuste sin reconciliar | **~22,3%** |

**Causas del desajuste identificadas:**

1. **Espacios en blanco en `trip_id` del GTFS estático** (ej. `'1079X20207C4a '`): al aplicar `strip()` el match sube de 61,9% a 77,7%.
2. **Diferencias entre versiones del GTFS estático**: el GTFS se renueva semanalmente; los `trip_id` del feed RT pueden referenciar una versión anterior, especialmente los días siguientes a una actualización.

Implicación directa para la arquitectura: la capa Silver necesita una **capa de reconciliación de IDs** (normalización de espacios + join tolerante a variantes de versión). La hipótesis H5 (cobertura > 99%) queda sin confirmar con el GTFS actual sin reconciliación.

---

## Duplicados y ruido

- El pipeline deduplica en origen por `header.timestamp` del protobuf antes de escribir en Bronze, por lo que los duplicados exactos en Parquet son **prácticamente cero** (0 grupos duplicados en muestra de 504.105 registros de un día).
- **Posiciones estáticas (trenes estacionados):** el 21,5% de las combinaciones `(trip_id, vehicle_id, latitud, longitud)` en vehicle_positions aparece en > 3 snapshots consecutivos. Es comportamiento esperado (trenes en cabecera o estacionados); volumen manejable y filtrable con heurística de varianza de posición o campo `current_status`.
- Estimación de registros "de ruido" (posiciones estáticas repetidas): **< 25% del total**, por debajo del umbral problemático.

---

## Primeros patrones observados: retraso medio por franja horaria

**Metodología:** `arrival_delay` del feed `trip_updates` (predicción de retraso en llegada a stops futuros del viaje, en segundos), convertido a minutos. Filtro aplicado: `arrival_delay BETWEEN -300 AND 7200` (entre -5 y +120 min). Hora local Madrid (Europe/Madrid, UTC+2 en verano). Período completo 47 días.

| Franja horaria | Retraso medio (min) | Mediana (min) | P75 (min) | P90 (min) |
|---|---|---|---|---|
| 04–06 h | 1,1 | 0 | 1 | 4 |
| 06–08 h | 3,2 | 1 | 4 | 8 |
| **07–09 h** | **~4,8–5,2** | 2 | 7 | 13 |
| 08–10 h | 5,2 | 2 | 7 | 14 |
| 10–12 h | 5,5 | 2 | 7 | 14 |
| 12–14 h | 4,8 | 2 | 6 | 13 |
| 14–16 h | 5,1 | 2 | 6 | 13 |
| 16–18 h | 5,5 | 2 | 7 | 14 |
| **18–20 h** | **5,8** | 2 | 7 | 15 |
| 20–22 h | 5,8 | 2 | 7 | 15 |
| 22–00 h | 6,2 | 2 | 7 | 16 |

**Lectura del patrón:**

- La franja **04–06 h** marca el mínimo diario (~1,1 min): el servicio arranca prácticamente puntual.
- El **pico de mañana (08–10 h)** es claro, con retraso medio que salta a ~5,2 min respecto a 1,1 min de madrugada.
- El mediodía (12–14 h) muestra una ligera mejoría (~4,8 min), compatible con menor demanda y recuperación operativa.
- El **pico de tarde (18–20 h → ~5,8 min)** es el máximo absoluto, coherente con lo declarado en entrevistas (mayor impacto en el trayecto de vuelta).
- La franja nocturna (22–00 h) mantiene valores altos (~6,2 min): los retrasos acumulados durante el día no se absorben completamente hasta el cierre del servicio.

> **Nota metodológica:** `arrival_delay` es una predicción sobre stops futuros, no la llegada consumada. El crecimiento gradual a lo largo del día refleja parcialmente la propagación de retrasos que no se absorben entre servicios. Un análisis Silver más refinado deberá calcular el retraso en el stop terminal de cada viaje para obtener la puntualidad consumada.

---

## Resumen ejecutivo para la presentación

| Dimensión de calidad | Resultado provisional |
|---|---|
| **Volumetría** | ~2.400 snapshots/día · ~18,8 M registros VP totales · 6,2 GB Bronze |
| **Disponibilidad** | 44/47 días sin incidencias · 1 corte total (03-jun) · gaps < 5 min en operación normal |
| **Cobertura circulaciones** | ~77,7% de viajes con ID reconciliable directamente; ~22% requiere capa Silver de normalización |
| **Consistencia IDs** | Espacios en blanco en GTFS estático + cambios de versión semanal → reconciliación necesaria |
| **Duplicados/ruido** | <25% posiciones estáticas (trenes parados), deduplicación exacta en Bronze ya operativa |
| **Patrón de retraso** | Pico mañana 08–10 h (~5,2 min) y pico tarde 18–20 h (~5,8 min); mínimo en arranque 04–06 h (1,1 min) |
