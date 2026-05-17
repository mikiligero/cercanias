"""
Dashboard Streamlit — Puntualidad Cercanías Renfe
Refresco automático cada 30 s (sincronizado con el ciclo del feed).

Arranca con:
    streamlit run dashboard.py
"""
import shutil
import time
import zipfile
from datetime import date, datetime, timedelta, timezone
from pathlib import Path

import duckdb
import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
import streamlit as st

from src.config import load
from src.query import (
    active_alerts,
    delays_by_trip,
    network_heatmap,
    punctuality_over_time,
    snapshot_summary,
)

# ---------------------------------------------------------------------------
# Constantes
# ---------------------------------------------------------------------------
NUCLEUS_NAMES = {
    "10": "Madrid",
    "20": "Asturias",
    "30": "Sevilla",
    "31": "Cádiz",
    "32": "Málaga",
    "40": "Valencia",
    "41": "Murcia-Alicante",
    "51": "Cataluña (Rodalies)",
    "60": "Bilbao",
    "62": "Cantabria",
    "70": "Zaragoza",
}

PERIOD_OPTIONS = [
    "Última hora",
    "Últimas 3h",
    "Últimas 6h",
    "Últimas 12h",
    "Últimas 24h",
    "Hoy",
    "Ayer",
    "Esta semana",
    "Semana pasada",
    "Este mes",
    "Mes pasado",
    "Personalizado",
]

# ---------------------------------------------------------------------------
# Configuración de página
# ---------------------------------------------------------------------------
st.set_page_config(
    page_title="Cercanías Renfe — Puntualidad",
    page_icon="🚆",
    layout="wide",
)

# ---------------------------------------------------------------------------
# Carga de recursos (cacheados entre refrescos)
# ---------------------------------------------------------------------------

@st.cache_resource
def _load_cfg():
    return load()


@st.cache_resource
def _load_gtfs_static(raw_dir: str):
    zip_path = Path(raw_dir) / "gtfs_static" / "fomento_transit.zip"
    if not zip_path.exists():
        return None

    def _read(name, cols):
        with zipfile.ZipFile(zip_path) as zf:
            with zf.open(name) as f:
                df = pd.read_csv(f, dtype=str)
        df.columns = df.columns.str.strip()
        return df[cols].apply(lambda c: c.str.strip() if c.dtype == "object" else c)

    trips  = _read("trips.txt",  ["trip_id", "route_id", "service_id"])
    routes = _read("routes.txt", ["route_id", "route_short_name", "route_long_name"])
    stops  = _read("stops.txt",  ["stop_id", "stop_name", "stop_lat", "stop_lon"])
    routes["nucleus_code"] = routes["route_id"].str.extract(r'^(\d+)T')
    return {"trips": trips, "routes": routes, "stops": stops}


def _build_con(cfg: dict, gtfs: dict | None) -> duckdb.DuckDBPyConnection:
    bronze = Path(cfg["storage"]["bronze_dir"])
    con = duckdb.connect()
    for feed in ("vehicle_positions", "trip_updates", "alerts"):
        pattern = str(bronze / feed / "**" / "*.parquet")
        con.execute(
            f"CREATE VIEW {feed} AS "
            f"SELECT * FROM read_parquet('{pattern}', hive_partitioning=true)"
        )
    if gtfs:
        con.register("trips",  gtfs["trips"])
        con.register("routes", gtfs["routes"])
        con.register("stops",  gtfs["stops"])
    return con


# ---------------------------------------------------------------------------
# Helpers visuales
# ---------------------------------------------------------------------------

def _semaforo(minutes: float) -> str:
    if minutes <= 2:   return "🟢"
    if minutes <= 10:  return "🟡"
    return "🔴"


def _card_bg(minutes: float) -> str:
    if minutes <= 2:   return "#d4edda"
    if minutes <= 10:  return "#fff3cd"
    return "#f8d7da"


def _ago(epoch: int | float | None) -> str:
    if epoch is None:
        return "—"
    secs = int(datetime.now(timezone.utc).timestamp() - float(epoch))
    if secs < 60:
        return f"hace {secs}s"
    return f"hace {secs // 60}m {secs % 60}s"


# ---------------------------------------------------------------------------
# Período
# ---------------------------------------------------------------------------

def _period_bounds(preset: str, custom_range=None) -> tuple[int, int]:
    """Devuelve (start_ts, end_ts) en epoch UTC según el preset."""
    now = datetime.now(timezone.utc)
    today_start = now.replace(hour=0, minute=0, second=0, microsecond=0)
    end = now

    if preset == "Última hora":
        start = now - timedelta(hours=1)
    elif preset == "Últimas 3h":
        start = now - timedelta(hours=3)
    elif preset == "Últimas 6h":
        start = now - timedelta(hours=6)
    elif preset == "Últimas 12h":
        start = now - timedelta(hours=12)
    elif preset == "Últimas 24h":
        start = now - timedelta(hours=24)
    elif preset == "Hoy":
        start = today_start
    elif preset == "Ayer":
        start = today_start - timedelta(days=1)
        end   = today_start
    elif preset == "Esta semana":
        start = today_start - timedelta(days=today_start.weekday())
    elif preset == "Semana pasada":
        this_week_start = today_start - timedelta(days=today_start.weekday())
        start = this_week_start - timedelta(days=7)
        end   = this_week_start
    elif preset == "Este mes":
        start = today_start.replace(day=1)
    elif preset == "Mes pasado":
        first_this_month = today_start.replace(day=1)
        start = (first_this_month - timedelta(days=1)).replace(day=1)
        end   = first_this_month
    elif preset == "Personalizado" and custom_range and len(custom_range) == 2:
        start = datetime(
            custom_range[0].year, custom_range[0].month, custom_range[0].day,
            tzinfo=timezone.utc,
        )
        end = datetime(
            custom_range[1].year, custom_range[1].month, custom_range[1].day,
            23, 59, 59, tzinfo=timezone.utc,
        )
    else:
        start = today_start

    return int(start.timestamp()), int(end.timestamp())


def _period_label(start_ts: int, end_ts: int) -> str:
    fmt = "%d/%m %H:%M"
    s = datetime.fromtimestamp(start_ts, tz=timezone.utc).strftime(fmt)
    e = datetime.fromtimestamp(end_ts,   tz=timezone.utc).strftime(fmt)
    return f"{s} → {e} UTC"


# ---------------------------------------------------------------------------
# Consultas por núcleo
# ---------------------------------------------------------------------------

def _nucleus_current(con) -> pd.DataFrame:
    return con.execute("""
        WITH
        latest_vp AS (SELECT max(snapshot_ts) ts FROM vehicle_positions),
        latest_tu AS (SELECT max(snapshot_ts) ts FROM trip_updates),
        trenes AS (
            SELECT r.nucleus_code AS ncode, count(*) AS circulando
            FROM vehicle_positions vp
            JOIN latest_vp ON vp.snapshot_ts = latest_vp.ts
            JOIN trips  t ON vp.trip_id = t.trip_id
            JOIN routes r ON t.route_id = r.route_id
            WHERE r.nucleus_code IS NOT NULL
            GROUP BY ncode
        ),
        retrasos AS (
            SELECT r.nucleus_code AS ncode,
                   round(max(tu.arrival_delay) / 60.0, 1) AS max_min,
                   round(avg(tu.arrival_delay) / 60.0, 1) AS media_min
            FROM trip_updates tu
            JOIN latest_tu ON tu.snapshot_ts = latest_tu.ts
            JOIN trips  t ON tu.trip_id = t.trip_id
            JOIN routes r ON t.route_id = r.route_id
            WHERE tu.arrival_delay IS NOT NULL AND r.nucleus_code IS NOT NULL
            GROUP BY ncode
        )
        SELECT t.ncode,
               t.circulando,
               coalesce(r.max_min,   0) AS retraso_max_min,
               coalesce(r.media_min, 0) AS retraso_medio_min
        FROM trenes t
        LEFT JOIN retrasos r ON t.ncode = r.ncode
        ORDER BY t.circulando DESC
    """).df()


def _nucleus_history(con, start_ts: int, end_ts: int) -> pd.DataFrame:
    return con.execute(f"""
        SELECT
            to_timestamp(tu.snapshot_ts)               AS ts,
            r.nucleus_code                             AS ncode,
            round(avg(tu.arrival_delay) / 60.0, 1)    AS media_min,
            round(max(tu.arrival_delay) / 60.0, 1)    AS max_min
        FROM trip_updates tu
        JOIN trips  t ON tu.trip_id = t.trip_id
        JOIN routes r ON t.route_id = r.route_id
        WHERE tu.arrival_delay IS NOT NULL
          AND r.nucleus_code IS NOT NULL
          AND tu.snapshot_ts BETWEEN {start_ts} AND {end_ts}
        GROUP BY tu.snapshot_ts, r.nucleus_code
        ORDER BY tu.snapshot_ts
    """).df()


def _nucleus_top_delays(con, ncode: str | None = None) -> pd.DataFrame:
    nc_filter = f"AND r.nucleus_code = '{ncode}'" if ncode else ""
    return con.execute(f"""
        WITH latest AS (SELECT max(snapshot_ts) ts FROM trip_updates)
        SELECT r.nucleus_code AS ncode, r.route_short_name AS linea,
               tu.trip_id,
               round(tu.arrival_delay / 60.0, 1) AS retraso_min
        FROM trip_updates tu
        JOIN latest   ON tu.snapshot_ts = latest.ts
        JOIN trips  t ON tu.trip_id = t.trip_id
        JOIN routes r ON t.route_id = r.route_id
        WHERE tu.arrival_delay > 60 AND r.nucleus_code IS NOT NULL
        {nc_filter}
        ORDER BY tu.arrival_delay DESC
        LIMIT 20
    """).df()


# ---------------------------------------------------------------------------
# Sidebar — selector de período
# ---------------------------------------------------------------------------

with st.sidebar:
    st.header("⏱ Período")
    period_preset = st.selectbox(
        "Rango de tiempo",
        PERIOD_OPTIONS,
        index=PERIOD_OPTIONS.index("Hoy"),
        key="period_preset",
    )

    custom_range = None
    if period_preset == "Personalizado":
        custom_range = st.date_input(
            "Selecciona fechas",
            value=(date.today() - timedelta(days=7), date.today()),
            max_value=date.today(),
            key="custom_range",
        )

    start_ts, end_ts = _period_bounds(period_preset, custom_range)
    st.caption(_period_label(start_ts, end_ts))

    st.divider()
    st.caption("El período afecta a gráficos históricos y tablas de tendencia.\nLos KPIs en vivo y el estado actual siempre muestran el último snapshot.")


# ---------------------------------------------------------------------------
# Arranque
# ---------------------------------------------------------------------------

cfg  = _load_cfg()
gtfs = _load_gtfs_static(cfg["storage"]["raw_dir"])
con  = _build_con(cfg, gtfs)
now  = datetime.now(timezone.utc).strftime("%H:%M:%S UTC")

st.title("🚆 Cercanías Renfe — Puntualidad en tiempo real")
st.caption(f"Última actualización: {now} · Refresco automático cada 30 s")

tab1, tab2, tab3, tab4 = st.tabs([
    "🔴 Red en vivo",
    "🗺️ Por núcleo",
    "📈 Tendencia del día",
    "⚙️ Sistema",
])

# ===========================================================================
# TAB 1 — Red en vivo
# ===========================================================================
with tab1:

    summary = snapshot_summary(con)
    if summary.empty or summary["trenes"].iloc[0] is None:
        st.warning("Sin datos de trip_updates todavía. Espera el primer ciclo de ingesta.")
    else:
        row = summary.iloc[0]
        c1, c2, c3, c4, c5 = st.columns(5)
        c1.metric("Trenes con info",  int(row["trenes"]))
        c2.metric("Con retraso",      int(row["con_retraso"] or 0))
        c3.metric("A tiempo",         int(row["a_tiempo"] or 0))
        c4.metric("Retraso medio",    f"{row['retraso_medio_min'] or 0} min")
        c5.metric("Retraso máximo",   f"{row['retraso_max_min'] or 0} min")
        st.caption(f"Snapshot: {row['snapshot']}")

    st.divider()

    col_left, col_right = st.columns([1, 3])
    with col_left:
        min_delay = st.slider("Retraso mínimo (min)", 0, 30, 1, key="t1_min_delay")

        # Filtro de núcleo
        sel_ncode_t1 = None
        if gtfs and "nucleus_code" in gtfs["routes"].columns:
            nucleus_map = {
                NUCLEUS_NAMES.get(n, n): n
                for n in sorted(gtfs["routes"]["nucleus_code"].dropna().unique())
            }
            nucleus_display = ["Todos los núcleos"] + sorted(nucleus_map.keys())
            sel_nuc_name = st.selectbox("Núcleo", nucleus_display, key="t1_nucleus")
            if sel_nuc_name != "Todos los núcleos":
                sel_ncode_t1 = nucleus_map[sel_nuc_name]

        # Filtro de línea (dentro del núcleo si se ha seleccionado)
        if gtfs:
            route_df = gtfs["routes"]
            if sel_ncode_t1:
                route_df = route_df[route_df["nucleus_code"] == sel_ncode_t1]
            all_lines = sorted(route_df["route_short_name"].dropna().unique().tolist())
            sel_lines = st.multiselect("Filtrar línea", all_lines, key="t1_lines")
        else:
            sel_lines = []

    with col_right:
        # Último snapshot dentro del período seleccionado
        period_snap = con.execute(
            f"SELECT max(snapshot_ts) FROM trip_updates "
            f"WHERE snapshot_ts BETWEEN {start_ts} AND {end_ts}"
        ).fetchone()[0]

        if period_snap is None:
            st.info(f"Sin datos en el período seleccionado ({period_preset}).")
        else:
            df_delays = delays_by_trip(con, snapshot_ts=period_snap,
                                       min_delay_sec=min_delay * 60)
            # Aplicar filtro de núcleo post-query
            if sel_ncode_t1 and gtfs:
                nucleus_lines = gtfs["routes"][
                    gtfs["routes"]["nucleus_code"] == sel_ncode_t1
                ]["route_short_name"].tolist()
                df_delays = df_delays[df_delays["linea"].isin(nucleus_lines)]
            if sel_lines:
                df_delays = df_delays[df_delays["linea"].isin(sel_lines)]

            if df_delays.empty:
                st.info("Sin trenes con ese retraso en el snapshot seleccionado.")
            else:
                df_delays["estado"] = df_delays["retraso_min"].apply(_semaforo)
                st.dataframe(
                    df_delays[["estado", "linea", "trip_id", "parada",
                               "retraso_min", "retraso_seg"]],
                    use_container_width=True, hide_index=True,
                    column_config={
                        "estado":      st.column_config.TextColumn("", width="small"),
                        "linea":       st.column_config.TextColumn("Línea"),
                        "trip_id":     st.column_config.TextColumn("Tren"),
                        "parada":      st.column_config.TextColumn("Parada"),
                        "retraso_min": st.column_config.NumberColumn("Retraso (min)", format="%.1f"),
                        "retraso_seg": st.column_config.NumberColumn("Retraso (seg)", format="%d"),
                    },
                )
                snap_dt = datetime.fromtimestamp(period_snap, tz=timezone.utc).strftime("%H:%M:%S UTC")
                st.caption(f"{len(df_delays)} trenes · snapshot {snap_dt}")

    st.divider()
    st.subheader("⚠️ Alertas activas")
    df_al = active_alerts(con)
    if df_al.empty:
        st.success("Sin alertas activas en este momento.")
    else:
        for _, row in df_al.iterrows():
            routes_str = row["informed_routes"] or ""
            lines = ", ".join(sorted(set(
                r.split("T")[-1] if "T" in r else r
                for r in routes_str.split(",") if r
            )))
            with st.expander(f"**{lines or row['entity_id']}** — desde {row['desde']}"):
                st.write(row["description_text"] or "Sin descripción.")


# ===========================================================================
# TAB 2 — Por núcleo
# ===========================================================================
with tab2:

    df_cur  = _nucleus_current(con)
    df_hist = _nucleus_history(con, start_ts, end_ts)

    df_cur["nucleo"]  = df_cur["ncode"].map(NUCLEUS_NAMES).fillna(df_cur["ncode"])
    df_hist["nucleo"] = df_hist["ncode"].map(NUCLEUS_NAMES).fillna(df_hist["ncode"])

    # --- Tarjetas (siempre estado actual) ---
    st.subheader("Estado actual por núcleo")
    NCOLS  = 3
    nuclei = df_cur.to_dict("records")
    for row_n in [nuclei[i:i+NCOLS] for i in range(0, len(nuclei), NCOLS)]:
        cols = st.columns(NCOLS)
        for col, nuc in zip(cols, row_n):
            bg  = _card_bg(nuc["retraso_medio_min"])
            sem = _semaforo(nuc["retraso_medio_min"])
            with col:
                st.markdown(
                    f"""
                    <div style="background:{bg};border-radius:10px;
                                padding:16px;margin-bottom:8px">
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

    # --- Histórico filtrado por período ---
    st.subheader(f"📈 Retraso histórico por núcleo · {period_preset}")
    if df_hist.empty:
        st.info(f"Sin datos en el período seleccionado ({period_preset}).")
    else:
        available = sorted(df_hist["nucleo"].unique())
        default   = [n for n in available
                     if n in ("Madrid", "Cataluña (Rodalies)", "Valencia",
                               "Sevilla", "Asturias", "Málaga")]
        selected  = st.multiselect("Mostrar núcleos:", available,
                                   default=default or available[:4], key="t2_nuclei")
        df_plot   = df_hist[df_hist["nucleo"].isin(selected)]

        if not df_plot.empty:
            fig_h = px.line(
                df_plot, x="ts", y="media_min", color="nucleo",
                markers=True,
                labels={"ts": "Hora", "media_min": "Retraso medio (min)", "nucleo": "Núcleo"},
                title=f"Retraso medio por núcleo — {period_preset}",
                height=400,
            )
            fig_h.update_layout(
                xaxis_title="Hora (UTC)", yaxis_title="Retraso medio (min)",
                hovermode="x unified",
            )
            st.plotly_chart(fig_h, use_container_width=True)

        summary_n = (df_hist[df_hist["nucleo"].isin(selected)]
                     .groupby("nucleo")
                     .agg(snapshots=("ts", "count"),
                          media_dia=("media_min", "mean"),
                          max_dia=("max_min", "max"))
                     .round(1).reset_index()
                     .sort_values("media_dia", ascending=False))
        st.dataframe(
            summary_n.rename(columns={
                "nucleo": "Núcleo", "snapshots": "Snapshots",
                "media_dia": "Retraso medio período (min)",
                "max_dia":   "Retraso máx período (min)",
            }),
            use_container_width=True, hide_index=True,
        )

    st.divider()

    # --- Top retrasos (siempre último snapshot) ---
    st.subheader("🔴 Top retrasos actuales")
    col_f, col_t = st.columns([1, 3])
    with col_f:
        nucleus_opts = ["Todos"] + sorted(df_cur["nucleo"].tolist())
        sel_nuc_t2 = st.selectbox("Filtrar por núcleo", nucleus_opts, key="t2_nucleus")
    with col_t:
        ncode_f = None
        if sel_nuc_t2 != "Todos":
            r = df_cur[df_cur["nucleo"] == sel_nuc_t2]
            if not r.empty:
                ncode_f = r.iloc[0]["ncode"]
        df_top = _nucleus_top_delays(con, ncode_f)
        if not df_top.empty:
            df_top["nucleo"]   = df_top["ncode"].map(NUCLEUS_NAMES).fillna(df_top["ncode"])
            df_top["semaforo"] = df_top["retraso_min"].apply(_semaforo)
            st.dataframe(
                df_top[["semaforo", "nucleo", "linea", "trip_id", "retraso_min"]],
                use_container_width=True, hide_index=True,
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


# ===========================================================================
# TAB 3 — Tendencia del día
# ===========================================================================
with tab3:

    st.subheader(f"Puntualidad a lo largo del tiempo · {period_preset}")
    df_time = punctuality_over_time(con, start_ts=start_ts, end_ts=end_ts)

    if df_time.empty:
        st.info(f"Sin datos en el período seleccionado ({period_preset}).")
    else:
        fig = go.Figure()
        fig.add_trace(go.Bar(
            x=df_time["ts"], y=df_time["pct_puntual"],
            name="% puntual", marker_color="steelblue", opacity=0.7, yaxis="y1",
        ))
        fig.add_trace(go.Scatter(
            x=df_time["ts"], y=df_time["retraso_medio_min"],
            name="Retraso medio (min)", line=dict(color="tomato", width=2),
            mode="lines+markers", yaxis="y2",
        ))
        fig.update_layout(
            title=f"Puntualidad — {period_preset}",
            xaxis_title="Hora (UTC)",
            yaxis=dict(title="% trenes puntuales", range=[0, 100]),
            yaxis2=dict(title="Retraso medio (min)", overlaying="y", side="right"),
            legend=dict(orientation="h", y=1.1),
            height=420,
        )
        st.plotly_chart(fig, use_container_width=True)

        st.dataframe(
            df_time.rename(columns={
                "ts": "Hora", "trenes": "Trenes",
                "pct_puntual": "% puntual", "retraso_medio_min": "Retraso medio (min)",
            }),
            use_container_width=True, hide_index=True,
        )

    st.divider()

    if gtfs:
        st.subheader(f"📍 Retraso medio acumulado por parada · {period_preset}")
        df_map = network_heatmap(con, start_ts=start_ts, end_ts=end_ts)
        if not df_map.empty:
            fig_map = px.scatter_mapbox(
                df_map, lat="lat", lon="lon",
                color="retraso_medio_min", size="n_obs",
                hover_name="stop_name",
                hover_data={"retraso_medio_min": ":.1f", "n_obs": True},
                color_continuous_scale="RdYlGn_r",
                mapbox_style="carto-positron",
                zoom=5, center={"lat": 40.4, "lon": -3.7},
                height=500,
                title=f"Retraso medio por parada — {period_preset}",
            )
            fig_map.update_layout(margin={"r": 0, "t": 40, "l": 0, "b": 0})
            st.plotly_chart(fig_map, use_container_width=True)
        else:
            st.info("Sin observaciones suficientes en el período seleccionado.")
    else:
        st.info("Mapa no disponible — GTFS estático no descargado.")


# ===========================================================================
# TAB 4 — Sistema
# ===========================================================================
with tab4:

    st.subheader("🗂️ Estado del pipeline")

    raw_dir    = Path(cfg["storage"]["raw_dir"])
    bronze_dir = Path(cfg["storage"]["bronze_dir"])

    rows_pipe = []
    for feed in ("vehicle_positions", "trip_updates", "alerts"):
        pbs    = sorted((raw_dir    / feed).rglob("*.pb"))
        pqs    = sorted((bronze_dir / feed).rglob("*.parquet"))
        raw_kb = sum(f.stat().st_size for f in pbs)  / 1024
        pq_kb  = sum(f.stat().st_size for f in pqs)  / 1024
        last_ts = None
        if pbs:
            try:
                ts_str  = pbs[-1].stem.split("_")[-1]
                last_ts = datetime.strptime(ts_str, "%Y%m%dT%H%M%SZ").replace(
                    tzinfo=timezone.utc).timestamp()
            except ValueError:
                pass
        rows_pipe.append({
            "Feed": feed, ".pb": len(pbs), "Parquet": len(pqs),
            "Raw (KB)": round(raw_kb, 1), "Parquet (KB)": round(pq_kb, 1),
            "Último snapshot": _ago(last_ts),
        })

    st.dataframe(pd.DataFrame(rows_pipe), use_container_width=True, hide_index=True)

    # --- Espacio en disco ---
    st.subheader("💾 Espacio en disco")

    def _dir_mb(path: Path, pattern: str = "*") -> float:
        return sum(f.stat().st_size for f in path.rglob(pattern) if f.is_file()) / 1024 / 1024

    raw_mb    = _dir_mb(raw_dir, "*.pb")
    bronze_mb = _dir_mb(bronze_dir, "*.parquet")
    gtfs_mb   = _dir_mb(raw_dir / "gtfs_static") if (raw_dir / "gtfs_static").exists() else 0.0
    total_mb  = raw_mb + bronze_mb + gtfs_mb

    disk      = shutil.disk_usage(raw_dir)
    disk_used_gb  = disk.used  / 1024 ** 3
    disk_total_gb = disk.total / 1024 ** 3
    disk_free_gb  = disk.free  / 1024 ** 3
    pct_used      = disk.used / disk.total

    d1, d2, d3, d4 = st.columns(4)
    d1.metric("Raw (.pb)",         f"{raw_mb:.1f} MB")
    d2.metric("Bronze (Parquet)",  f"{bronze_mb:.1f} MB")
    d3.metric("GTFS estático",     f"{gtfs_mb:.1f} MB")
    d4.metric("Total datos",       f"{total_mb:.1f} MB")

    st.progress(pct_used, text=f"Disco: {disk_used_gb:.1f} GB usados / {disk_total_gb:.1f} GB total — {disk_free_gb:.1f} GB libres")

    st.divider()

    vp_pbs = sorted((raw_dir / "vehicle_positions").rglob("*.pb"))
    if vp_pbs:
        try:
            ts_str  = vp_pbs[-1].stem.split("_")[-1]
            last_dt = datetime.strptime(ts_str, "%Y%m%dT%H%M%SZ").replace(tzinfo=timezone.utc)
            age     = (datetime.now(timezone.utc) - last_dt).total_seconds()
            if age < 60:
                st.success(f"✅ Pipeline activo — último snapshot hace {int(age)}s")
            elif age < 120:
                st.warning(f"⚠️ Pipeline lento — último snapshot hace {int(age)}s")
            else:
                st.error(f"❌ Pipeline detenido — último snapshot hace {int(age // 60)}m")
        except ValueError:
            st.warning("No se pudo leer el timestamp del último .pb")
    else:
        st.error("❌ Sin datos — ejecuta `python main.py`")

    st.divider()
    st.subheader("🔍 Calidad de datos")

    if gtfs:
        df_vp     = con.execute(
            "SELECT DISTINCT trip_id FROM vehicle_positions WHERE trip_id IS NOT NULL"
        ).df()
        total_trips = len(df_vp)
        matched     = df_vp["trip_id"].isin(gtfs["trips"]["trip_id"]).sum()
        pct_match   = round(100 * matched / total_trips, 1) if total_trips else 0
        qa1, qa2, qa3 = st.columns(3)
        qa1.metric("trip_ids únicos (RT)", total_trips)
        qa2.metric("Resueltos en GTFS estático", matched)
        qa3.metric("% resueltos", f"{pct_match}%")

    df_ent = con.execute("""
        SELECT to_timestamp(snapshot_ts) AS ts, count(*) AS entidades
        FROM vehicle_positions
        GROUP BY snapshot_ts ORDER BY snapshot_ts
    """).df()
    if not df_ent.empty:
        fig_ent = px.line(
            df_ent, x="ts", y="entidades",
            title="Trenes capturados por snapshot",
            labels={"ts": "Hora (UTC)", "entidades": "Nº trenes"}, height=300,
        )
        fig_ent.update_traces(line_color="steelblue")
        st.plotly_chart(fig_ent, use_container_width=True)

    df_gaps = con.execute("""
        WITH ts_list AS (
            SELECT DISTINCT snapshot_ts FROM vehicle_positions ORDER BY snapshot_ts
        ),
        gaps AS (
            SELECT snapshot_ts,
                   lead(snapshot_ts) OVER (ORDER BY snapshot_ts) - snapshot_ts AS gap_sec
            FROM ts_list
        )
        SELECT strftime(to_timestamp(snapshot_ts), '%H:%M:%S') AS hora, gap_sec
        FROM gaps WHERE gap_sec > 120
        ORDER BY gap_sec DESC
    """).df()
    if df_gaps.empty:
        st.success("✅ Sin huecos detectados en el histórico (todos los gaps ≤ 120s)")
    else:
        st.warning(f"⚠️ {len(df_gaps)} huecos detectados (gap > 120s entre snapshots)")
        st.dataframe(df_gaps, use_container_width=True, hide_index=True)

# ---------------------------------------------------------------------------
# Auto-refresco
# ---------------------------------------------------------------------------
time.sleep(30)
st.rerun()
