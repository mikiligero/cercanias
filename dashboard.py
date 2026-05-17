"""
Dashboard Streamlit — Puntualidad Cercanías Renfe
Refresco automático cada 30 s (sincronizado con el ciclo del feed).

Arranca con:
    streamlit run dashboard.py
"""
import time
import zipfile
from datetime import datetime, timezone
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
    """Carga el GTFS estático una sola vez (15 MB ZIP)."""
    zip_path = Path(raw_dir) / "gtfs_static" / "fomento_transit.zip"
    if not zip_path.exists():
        return None

    def _read(name, cols):
        with zipfile.ZipFile(zip_path) as zf:
            with zf.open(name) as f:
                df = pd.read_csv(f, dtype=str)
        df.columns = df.columns.str.strip()
        df = df.apply(lambda c: c.str.strip() if c.dtype == "object" else c)
        return df[cols]

    return {
        "trips":  _read("trips.txt",  ["trip_id", "route_id", "service_id"]),
        "routes": _read("routes.txt", ["route_id", "route_short_name", "route_long_name"]),
        "stops":  _read("stops.txt",  ["stop_id", "stop_name", "stop_lat", "stop_lon"]),
    }


def _build_con(cfg: dict, gtfs: dict | None) -> duckdb.DuckDBPyConnection:
    """Conexión DuckDB fresca en cada refresco (vistas apuntan a Parquet actualizados)."""
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

def _delay_color(minutes: float) -> str:
    if minutes <= 2:
        return "🟢"
    if minutes <= 10:
        return "🟡"
    return "🔴"


def _ago(epoch: int | float | None) -> str:
    if epoch is None:
        return "—"
    secs = int(datetime.now(timezone.utc).timestamp() - float(epoch))
    if secs < 60:
        return f"hace {secs}s"
    return f"hace {secs // 60}m {secs % 60}s"


# ---------------------------------------------------------------------------
# Construcción del dashboard
# ---------------------------------------------------------------------------

cfg   = _load_cfg()
gtfs  = _load_gtfs_static(cfg["storage"]["raw_dir"])
con   = _build_con(cfg, gtfs)
now   = datetime.now(timezone.utc).strftime("%H:%M:%S UTC")

st.title("🚆 Cercanías Renfe — Puntualidad en tiempo real")
st.caption(f"Última actualización: {now} · Refresco automático cada 30 s")

tab1, tab2, tab3 = st.tabs(["🔴 Red en vivo", "📈 Tendencia del día", "⚙️ Sistema"])

# ===========================================================================
# TAB 1 — Red en vivo
# ===========================================================================
with tab1:

    # KPIs
    summary = snapshot_summary(con)
    if summary.empty or summary["trenes"].iloc[0] is None:
        st.warning("Sin datos de trip_updates todavía. Espera el primer ciclo de ingesta.")
    else:
        row = summary.iloc[0]
        c1, c2, c3, c4, c5 = st.columns(5)
        c1.metric("Trenes con info",   int(row["trenes"]))
        c2.metric("Con retraso",       int(row["con_retraso"] or 0))
        c3.metric("A tiempo",          int(row["a_tiempo"] or 0))
        c4.metric("Retraso medio",     f"{row['retraso_medio_min'] or 0} min")
        c5.metric("Retraso máximo",    f"{row['retraso_max_min'] or 0} min")

        # Snapshot timestamp
        st.caption(f"Snapshot: {row['snapshot']}")

    st.divider()

    # Tabla de retrasos
    col_left, col_right = st.columns([1, 3])
    with col_left:
        min_delay = st.slider("Retraso mínimo (min)", 0, 30, 1)
        if gtfs:
            all_lines = sorted(
                gtfs["routes"]["route_short_name"].dropna().unique().tolist()
            )
            sel_lines = st.multiselect("Filtrar línea", all_lines)
        else:
            sel_lines = []

    with col_right:
        df_delays = delays_by_trip(con, min_delay_sec=min_delay * 60)
        if sel_lines:
            df_delays = df_delays[df_delays["linea"].isin(sel_lines)]

        if df_delays.empty:
            st.info("Sin trenes con ese retraso en el último snapshot.")
        else:
            df_delays["estado"] = df_delays["retraso_min"].apply(_delay_color)
            st.dataframe(
                df_delays[["estado", "linea", "trip_id", "parada",
                            "retraso_min", "retraso_seg"]],
                use_container_width=True,
                hide_index=True,
                column_config={
                    "estado":      st.column_config.TextColumn("", width="small"),
                    "linea":       st.column_config.TextColumn("Línea"),
                    "trip_id":     st.column_config.TextColumn("Tren"),
                    "parada":      st.column_config.TextColumn("Parada"),
                    "retraso_min": st.column_config.NumberColumn("Retraso (min)", format="%.1f"),
                    "retraso_seg": st.column_config.NumberColumn("Retraso (seg)", format="%d"),
                },
            )
            st.caption(f"{len(df_delays)} trenes mostrados")

    st.divider()

    # Alertas activas
    st.subheader("⚠️ Alertas activas")
    df_al = active_alerts(con)
    if df_al.empty:
        st.success("Sin alertas activas en este momento.")
    else:
        for _, row in df_al.iterrows():
            routes = row["informed_routes"] or ""
            lines  = ", ".join(sorted(set(
                r.split("T")[-1] if "T" in r else r
                for r in routes.split(",") if r
            )))
            with st.expander(f"**{lines or row['entity_id']}** — desde {row['desde']}"):
                st.write(row["description_text"] or "Sin descripción.")


# ===========================================================================
# TAB 2 — Tendencia del día
# ===========================================================================
with tab2:

    df_time = punctuality_over_time(con)

    if df_time.empty:
        st.info("Acumulando datos... vuelve en unos minutos.")
    else:
        # Gráfico doble eje: % puntual (barras) + retraso medio (línea)
        fig = go.Figure()

        fig.add_trace(go.Bar(
            x=df_time["hora"],
            y=df_time["pct_puntual"],
            name="% puntual",
            marker_color="steelblue",
            opacity=0.7,
            yaxis="y1",
        ))
        fig.add_trace(go.Scatter(
            x=df_time["hora"],
            y=df_time["retraso_medio_min"],
            name="Retraso medio (min)",
            line=dict(color="tomato", width=2),
            mode="lines+markers",
            yaxis="y2",
        ))

        fig.update_layout(
            title="Puntualidad a lo largo del día",
            xaxis_title="Hora (UTC)",
            yaxis=dict(title="% trenes puntuales", range=[0, 100]),
            yaxis2=dict(title="Retraso medio (min)", overlaying="y", side="right"),
            legend=dict(orientation="h", y=1.1),
            height=420,
        )
        st.plotly_chart(fig, use_container_width=True)

        # Tabla resumen
        st.dataframe(
            df_time.rename(columns={
                "hora": "Hora", "trenes": "Trenes",
                "pct_puntual": "% puntual", "retraso_medio_min": "Retraso medio (min)",
            }),
            use_container_width=True,
            hide_index=True,
        )

    st.divider()

    # Mapa de calor de paradas (requiere GTFS estático)
    if gtfs:
        st.subheader("📍 Retraso medio acumulado por parada")
        df_map = network_heatmap(con)
        if not df_map.empty:
            fig_map = px.scatter_mapbox(
                df_map,
                lat="lat", lon="lon",
                color="retraso_medio_min",
                size="n_obs",
                hover_name="stop_name",
                hover_data={"retraso_medio_min": ":.1f", "n_obs": True},
                color_continuous_scale="RdYlGn_r",
                mapbox_style="carto-positron",
                zoom=5,
                center={"lat": 40.4, "lon": -3.7},
                height=500,
                title="Retraso medio por parada (histórico acumulado)",
            )
            fig_map.update_layout(margin={"r": 0, "t": 40, "l": 0, "b": 0})
            st.plotly_chart(fig_map, use_container_width=True)
    else:
        st.info("Mapa no disponible — GTFS estático no descargado.")


# ===========================================================================
# TAB 3 — Sistema
# ===========================================================================
with tab3:

    st.subheader("🗂️ Estado del pipeline")

    raw_dir    = Path(cfg["storage"]["raw_dir"])
    bronze_dir = Path(cfg["storage"]["bronze_dir"])

    rows_pipeline = []
    for feed in ("vehicle_positions", "trip_updates", "alerts"):
        pbs     = sorted((raw_dir / feed).rglob("*.pb"))
        pqs     = sorted((bronze_dir / feed).rglob("*.parquet"))
        raw_kb  = sum(f.stat().st_size for f in pbs) / 1024
        pq_kb   = sum(f.stat().st_size for f in pqs) / 1024
        last_ts = None
        if pbs:
            try:
                ts_str = pbs[-1].stem.split("_")[-1]
                dt = datetime.strptime(ts_str, "%Y%m%dT%H%M%SZ").replace(
                    tzinfo=timezone.utc)
                last_ts = dt.timestamp()
            except ValueError:
                pass

        rows_pipeline.append({
            "Feed":            feed,
            ".pb capturados":  len(pbs),
            "Parquet escritos": len(pqs),
            "Raw (KB)":        round(raw_kb, 1),
            "Parquet (KB)":    round(pq_kb, 1),
            "Último snapshot": _ago(last_ts),
        })

    st.dataframe(pd.DataFrame(rows_pipeline), use_container_width=True, hide_index=True)

    # Estado general del pipeline
    vp_pbs = sorted((raw_dir / "vehicle_positions").rglob("*.pb"))
    if vp_pbs:
        try:
            last_str = vp_pbs[-1].stem.split("_")[-1]
            last_dt  = datetime.strptime(last_str, "%Y%m%dT%H%M%SZ").replace(
                tzinfo=timezone.utc)
            age_sec  = (datetime.now(timezone.utc) - last_dt).total_seconds()
            if age_sec < 60:
                st.success(f"✅ Pipeline activo — último snapshot hace {int(age_sec)}s")
            elif age_sec < 120:
                st.warning(f"⚠️ Pipeline lento — último snapshot hace {int(age_sec)}s")
            else:
                st.error(f"❌ Pipeline detenido — último snapshot hace {int(age_sec // 60)}m")
        except ValueError:
            st.warning("No se pudo leer el timestamp del último .pb")
    else:
        st.error("❌ Sin datos — ejecuta `python main.py`")

    st.divider()
    st.subheader("🔍 Calidad de datos")

    if gtfs:
        # % trip_ids resueltos
        df_vp = con.execute(
            "SELECT DISTINCT trip_id FROM vehicle_positions WHERE trip_id IS NOT NULL"
        ).df()
        total_trips = len(df_vp)
        matched     = df_vp["trip_id"].isin(gtfs["trips"]["trip_id"]).sum()
        pct_match   = round(100 * matched / total_trips, 1) if total_trips else 0

        qa1, qa2, qa3 = st.columns(3)
        qa1.metric("trip_ids únicos (RT)", total_trips)
        qa2.metric("Resueltos en GTFS estático", matched)
        qa3.metric("% resueltos", f"{pct_match}%")

    # Entidades por snapshot a lo largo del tiempo
    df_ent = con.execute("""
        SELECT
            strftime(to_timestamp(snapshot_ts), '%H:%M:%S') AS hora,
            count(*) AS entidades
        FROM vehicle_positions
        GROUP BY snapshot_ts ORDER BY snapshot_ts
    """).df()

    if not df_ent.empty:
        fig_ent = px.line(
            df_ent, x="hora", y="entidades",
            title="Trenes capturados por snapshot",
            labels={"hora": "Hora (UTC)", "entidades": "Nº trenes"},
            height=300,
        )
        fig_ent.update_traces(line_color="steelblue")
        st.plotly_chart(fig_ent, use_container_width=True)

    # Detección de huecos (gaps > 120s entre snapshots consecutivos)
    df_gaps = con.execute("""
        WITH ts_list AS (
            SELECT DISTINCT snapshot_ts FROM vehicle_positions ORDER BY snapshot_ts
        ),
        gaps AS (
            SELECT snapshot_ts,
                   lead(snapshot_ts) OVER (ORDER BY snapshot_ts) - snapshot_ts AS gap_sec
            FROM ts_list
        )
        SELECT strftime(to_timestamp(snapshot_ts), '%H:%M:%S') AS hora,
               gap_sec
        FROM gaps
        WHERE gap_sec > 120
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
