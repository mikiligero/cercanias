"""
Dashboard por núcleo de Cercanías.
Muestra trenes en circulación, retraso máximo y medio actuales,
y la evolución histórica del retraso a lo largo del día por núcleo.

Arranca con:
    streamlit run dashboard_nucleos.py
"""
import time
import zipfile
from datetime import datetime, timezone
from pathlib import Path

import duckdb
import pandas as pd
import plotly.express as px
import streamlit as st

from src.config import load

# ---------------------------------------------------------------------------
# Mapeo código → nombre de núcleo
# ---------------------------------------------------------------------------
NUCLEUS_NAMES = {
    "10": "Madrid",
    "20": "Asturias",
    "30": "Sevilla",
    "31": "Cádiz",
    "32": "Málaga",
    "40": "Valencia",
    "41": "Murcia-Alicante",
    "45": "Valencia (Gandia)",
    "51": "Cataluña (Rodalies)",
    "60": "Bilbao",
    "62": "Cantabria",
    "70": "Zaragoza",
}

# ---------------------------------------------------------------------------
# Configuración de página
# ---------------------------------------------------------------------------
st.set_page_config(
    page_title="Cercanías por Núcleo",
    page_icon="🗺️",
    layout="wide",
)

# ---------------------------------------------------------------------------
# Recursos cacheados
# ---------------------------------------------------------------------------

@st.cache_resource
def _load_cfg():
    return load()


@st.cache_resource
def _load_gtfs(raw_dir: str):
    zip_path = Path(raw_dir) / "gtfs_static" / "fomento_transit.zip"

    def _read(name, cols):
        with zipfile.ZipFile(zip_path) as zf:
            with zf.open(name) as f:
                df = pd.read_csv(f, dtype=str)
        df.columns = df.columns.str.strip()
        return df[cols].apply(lambda c: c.str.strip() if c.dtype == "object" else c)

    trips  = _read("trips.txt",  ["trip_id", "route_id"])
    routes = _read("routes.txt", ["route_id", "route_short_name"])
    routes["nucleus_code"] = routes["route_id"].str.extract(r'^(\d+)T')
    return trips, routes


def _build_con(cfg, trips, routes):
    bronze = Path(cfg["storage"]["bronze_dir"])
    con = duckdb.connect()
    for feed in ("vehicle_positions", "trip_updates"):
        pattern = str(bronze / feed / "**" / "*.parquet")
        con.execute(
            f"CREATE VIEW {feed} AS "
            f"SELECT * FROM read_parquet('{pattern}', hive_partitioning=true)"
        )
    con.register("trips_s",  trips)
    con.register("routes_s", routes)
    return con


# ---------------------------------------------------------------------------
# Consultas por núcleo
# ---------------------------------------------------------------------------

def _current_by_nucleus(con) -> pd.DataFrame:
    """Trenes circulando y retrasos del último snapshot, por núcleo."""
    return con.execute("""
        WITH
        latest_vp AS (SELECT max(snapshot_ts) ts FROM vehicle_positions),
        latest_tu AS (SELECT max(snapshot_ts) ts FROM trip_updates),
        trenes AS (
            SELECT regexp_extract(r.nucleus_code, '^\d+') AS ncode,
                   count(*) AS circulando
            FROM vehicle_positions vp
            JOIN latest_vp ON vp.snapshot_ts = latest_vp.ts
            JOIN trips_s  t ON vp.trip_id  = t.trip_id
            JOIN routes_s r ON t.route_id  = r.route_id
            WHERE r.nucleus_code IS NOT NULL
            GROUP BY ncode
        ),
        retrasos AS (
            SELECT regexp_extract(r.nucleus_code, '^\d+') AS ncode,
                   round(max(tu.arrival_delay)  / 60.0, 1) AS max_min,
                   round(avg(tu.arrival_delay)  / 60.0, 1) AS media_min,
                   count(*) AS con_info
            FROM trip_updates tu
            JOIN latest_tu ON tu.snapshot_ts = latest_tu.ts
            JOIN trips_s  t ON tu.trip_id  = t.trip_id
            JOIN routes_s r ON t.route_id  = r.route_id
            WHERE tu.arrival_delay IS NOT NULL
              AND r.nucleus_code IS NOT NULL
            GROUP BY ncode
        )
        SELECT t.ncode,
               t.circulando,
               coalesce(r.max_min,   0) AS retraso_max_min,
               coalesce(r.media_min, 0) AS retraso_medio_min,
               coalesce(r.con_info,  0) AS trenes_con_delay
        FROM trenes t
        LEFT JOIN retrasos r ON t.ncode = r.ncode
        ORDER BY t.circulando DESC
    """).df()


def _history_by_nucleus(con) -> pd.DataFrame:
    """Retraso medio histórico por núcleo y snapshot."""
    return con.execute("""
        SELECT
            to_timestamp(tu.snapshot_ts)                     AS ts,
            regexp_extract(r.nucleus_code, '^\d+')           AS ncode,
            round(avg(tu.arrival_delay) / 60.0, 1)           AS media_min,
            round(max(tu.arrival_delay) / 60.0, 1)           AS max_min,
            count(*)                                          AS trenes
        FROM trip_updates tu
        JOIN trips_s  t ON tu.trip_id = t.trip_id
        JOIN routes_s r ON t.route_id = r.route_id
        WHERE tu.arrival_delay IS NOT NULL
          AND r.nucleus_code IS NOT NULL
        GROUP BY tu.snapshot_ts, ncode
        ORDER BY tu.snapshot_ts
    """).df()


def _top_delays(con, nucleus_code: str | None = None) -> pd.DataFrame:
    """Top trenes retrasados del último snapshot, opcional filtrado por núcleo."""
    nc_filter = f"AND regexp_extract(r.nucleus_code, '^\\d+') = '{nucleus_code}'" \
                if nucleus_code else ""
    return con.execute(f"""
        WITH latest AS (SELECT max(snapshot_ts) ts FROM trip_updates)
        SELECT
            regexp_extract(r.nucleus_code, '^\\d+')    AS ncode,
            r.route_short_name                          AS linea,
            tu.trip_id,
            round(tu.arrival_delay / 60.0, 1)          AS retraso_min
        FROM trip_updates tu
        JOIN latest   ON tu.snapshot_ts = latest.ts
        JOIN trips_s  t ON tu.trip_id  = t.trip_id
        JOIN routes_s r ON t.route_id  = r.route_id
        WHERE tu.arrival_delay > 60
          AND r.nucleus_code IS NOT NULL
          {nc_filter}
        ORDER BY tu.arrival_delay DESC
        LIMIT 20
    """).df()


# ---------------------------------------------------------------------------
# Helpers visuales
# ---------------------------------------------------------------------------

def _semaforo(minutos: float) -> str:
    if minutos <= 2:   return "🟢"
    if minutos <= 10:  return "🟡"
    return "🔴"


def _card_color(minutos: float) -> str:
    if minutos <= 2:   return "#d4edda"   # verde claro
    if minutos <= 10:  return "#fff3cd"   # amarillo claro
    return "#f8d7da"                       # rojo claro


# ---------------------------------------------------------------------------
# Construcción del dashboard
# ---------------------------------------------------------------------------
cfg           = _load_cfg()
trips, routes = _load_gtfs(cfg["storage"]["raw_dir"])
con           = _build_con(cfg, trips, routes)
now           = datetime.now(timezone.utc).strftime("%H:%M:%S UTC")

st.title("🗺️ Cercanías Renfe — Vista por Núcleo")
st.caption(f"Última actualización: {now} · Refresco automático cada 30 s")

df_cur  = _current_by_nucleus(con)
df_hist = _history_by_nucleus(con)

# Añadir nombre del núcleo
df_cur["nucleo"]  = df_cur["ncode"].map(NUCLEUS_NAMES).fillna(df_cur["ncode"])
df_hist["nucleo"] = df_hist["ncode"].map(NUCLEUS_NAMES).fillna(df_hist["ncode"])

# ===========================================================================
# SECCIÓN 1 — Tarjetas por núcleo
# ===========================================================================
st.subheader("Estado actual por núcleo")

COLS = 3
nuclei = df_cur.to_dict("records")
rows   = [nuclei[i:i+COLS] for i in range(0, len(nuclei), COLS)]

for row in rows:
    cols = st.columns(COLS)
    for col, nuc in zip(cols, row):
        bg = _card_color(nuc["retraso_medio_min"])
        sem = _semaforo(nuc["retraso_medio_min"])
        with col:
            st.markdown(
                f"""
                <div style="background:{bg};border-radius:10px;padding:16px;margin-bottom:8px">
                  <h4 style="margin:0">{sem} {nuc['nucleo']}</h4>
                  <hr style="margin:6px 0">
                  <b>🚆 Trenes circulando:</b> {int(nuc['circulando'])}<br>
                  <b>⏱️ Retraso máximo:</b> {nuc['retraso_max_min']} min<br>
                  <b>📊 Retraso medio:</b> {nuc['retraso_medio_min']} min
                </div>
                """,
                unsafe_allow_html=True,
            )

st.divider()

# ===========================================================================
# SECCIÓN 2 — Evolución histórica del retraso
# ===========================================================================
st.subheader("📈 Retraso medio histórico por núcleo")

if df_hist.empty:
    st.info("Acumulando datos... vuelve en unos minutos.")
else:
    # Selector de núcleos a mostrar
    available_nuclei = sorted(df_hist["nucleo"].unique())
    default = [n for n in available_nuclei if n in
               ["Madrid", "Cataluña (Rodalies)", "Valencia", "Sevilla",
                "Asturias", "Málaga"]]
    selected = st.multiselect(
        "Mostrar núcleos:",
        available_nuclei,
        default=default or available_nuclei[:4],
    )

    df_plot = df_hist[df_hist["nucleo"].isin(selected)]

    if not df_plot.empty:
        fig = px.line(
            df_plot,
            x="ts", y="media_min",
            color="nucleo",
            markers=True,
            labels={"ts": "Hora", "media_min": "Retraso medio (min)", "nucleo": "Núcleo"},
            title="Retraso medio por núcleo (histórico del día)",
            height=420,
        )
        fig.update_layout(
            xaxis_title="Hora (UTC)",
            yaxis_title="Retraso medio (min)",
            legend_title="Núcleo",
            hovermode="x unified",
        )
        st.plotly_chart(fig, use_container_width=True)

    # Tabla resumen del histórico
    summary = (df_hist[df_hist["nucleo"].isin(selected)]
               .groupby("nucleo")
               .agg(
                   snapshots=("ts", "count"),
                   retraso_medio_dia=("media_min", "mean"),
                   retraso_max_dia=("max_min", "max"),
               )
               .round(1)
               .reset_index()
               .sort_values("retraso_medio_dia", ascending=False))

    st.dataframe(
        summary.rename(columns={
            "nucleo": "Núcleo",
            "snapshots": "Snapshots",
            "retraso_medio_dia": "Retraso medio día (min)",
            "retraso_max_dia": "Retraso máx día (min)",
        }),
        use_container_width=True,
        hide_index=True,
    )

st.divider()

# ===========================================================================
# SECCIÓN 3 — Top retrasos detallados
# ===========================================================================
st.subheader("🔴 Top retrasos actuales")

col_filter, col_table = st.columns([1, 3])

with col_filter:
    nucleus_options = ["Todos"] + sorted(df_cur["nucleo"].tolist())
    sel_nucleus = st.selectbox("Filtrar por núcleo", nucleus_options)

with col_table:
    ncode_filter = None
    if sel_nucleus != "Todos":
        row = df_cur[df_cur["nucleo"] == sel_nucleus]
        if not row.empty:
            ncode_filter = row.iloc[0]["ncode"]

    df_top = _top_delays(con, ncode_filter)
    if not df_top.empty:
        df_top["nucleo"]   = df_top["ncode"].map(NUCLEUS_NAMES).fillna(df_top["ncode"])
        df_top["semaforo"] = df_top["retraso_min"].apply(_semaforo)
        st.dataframe(
            df_top[["semaforo", "nucleo", "linea", "trip_id", "retraso_min"]],
            use_container_width=True,
            hide_index=True,
            column_config={
                "semaforo":    st.column_config.TextColumn("", width="small"),
                "nucleo":      st.column_config.TextColumn("Núcleo"),
                "linea":       st.column_config.TextColumn("Línea"),
                "trip_id":     st.column_config.TextColumn("Tren"),
                "retraso_min": st.column_config.NumberColumn("Retraso (min)", format="%.1f"),
            },
        )
    else:
        st.success("Sin trenes con retraso > 1 min en el último snapshot.")

# ---------------------------------------------------------------------------
# Auto-refresco
# ---------------------------------------------------------------------------
time.sleep(30)
st.rerun()
