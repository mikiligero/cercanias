#!/usr/bin/env python3
"""
Informe de feeds GTFS-RT perdidos — Cercanías Renfe

Compara los snapshots realmente capturados contra los esperados
en el horario operativo (05:30 → 00:30 hora de Madrid, cadencia 30 s).

Uso:
    python analisis_feeds_perdidos.py

Genera también feeds_perdidos.csv con el detalle por feed y día.
"""
from datetime import date, timedelta, time
from pathlib import Path

import duckdb
import pandas as pd

# ── Configuración ──────────────────────────────────────────────────────────
BRONZE_DIR      = Path("data/bronze")
FEEDS           = ["vehicle_positions", "trip_updates", "alerts"]
INTERVAL_SEC    = 30
TZ              = "Europe/Madrid"
WINDOW_START    = time(5, 30)   # hora local — inicio ventana operativa
WINDOW_END      = time(0, 30)   # hora local — fin (día siguiente)
WINDOW_SEC      = 19 * 3600     # 05:30 → 00:30 = 19 h exactas
EXPECTED_PER_DAY = WINDOW_SEC // INTERVAL_SEC   # 2 280 snapshots/día


# ── Lectura ────────────────────────────────────────────────────────────────

def fetch_unique_timestamps(feed: str) -> pd.Series:
    """Devuelve los timestamps únicos (Unix seconds) del feed desde los Parquet."""
    feed_dir = BRONZE_DIR / feed
    if not feed_dir.exists() or not list(feed_dir.rglob("*.parquet")):
        return pd.Series(dtype="int64", name="snapshot_ts")

    pattern = str(feed_dir / "**" / "*.parquet")
    con = duckdb.connect()
    try:
        con.execute(
            f"CREATE VIEW v AS SELECT DISTINCT snapshot_ts "
            f"FROM read_parquet('{pattern}', hive_partitioning=true)"
        )
        df = con.execute("SELECT snapshot_ts FROM v ORDER BY snapshot_ts").df()
    finally:
        con.close()
    return df["snapshot_ts"]


# ── Lógica de ventana operativa ────────────────────────────────────────────

WIN_START_MIN = WINDOW_START.hour * 60 + WINDOW_START.minute   # 330 (05:30)
WIN_END_MIN   = WINDOW_END.hour   * 60 + WINDOW_END.minute     # 30  (00:30)


def to_operating_day(ts_series: pd.Series) -> pd.DataFrame:
    """
    Para cada timestamp UTC:
      1. Convierte a hora de Madrid.
      2. Filtra los que caen dentro de la ventana 05:30–00:30.
      3. Asigna el día operativo (los snapshots 00:00–00:29 pertenecen al día anterior).

    Devuelve DataFrame con columnas [ts, op_day].
    """
    if ts_series.empty:
        return pd.DataFrame(columns=["ts", "op_day"])

    local = pd.to_datetime(ts_series, unit="s", utc=True).dt.tz_convert(TZ)
    mins  = local.dt.hour * 60 + local.dt.minute

    # Dentro de la ventana: >= 05:30  OR  < 00:30
    mask = (mins >= WIN_START_MIN) | (mins < WIN_END_MIN)

    df = pd.DataFrame({
        "ts":         ts_series[mask].values,
        "local_date": local[mask].dt.date.values,
        "mins":       mins[mask].values,
    })

    # 00:00–00:29 → día operativo = ayer
    df["op_day"] = df["local_date"]
    early = df["mins"] < WIN_END_MIN
    df.loc[early, "op_day"] = df.loc[early, "local_date"].apply(
        lambda d: d - timedelta(days=1)
    )

    return df[["ts", "op_day"]]


def count_by_day(feed: str) -> pd.DataFrame:
    """n_actual por día operativo para un feed."""
    ts   = fetch_unique_timestamps(feed)
    snap = to_operating_day(ts)
    if snap.empty:
        return pd.DataFrame(columns=["op_day", "n_actual"])

    counts = snap.groupby("op_day").size().reset_index(name="n_actual")
    return counts.sort_values("op_day").reset_index(drop=True)


def fill_missing_days(df: pd.DataFrame, first_day: date, last_day: date) -> pd.DataFrame:
    """Rellena con 0 los días operativos sin ningún snapshot en el rango dado."""
    full_range = pd.DataFrame({
        "op_day": pd.date_range(first_day, last_day, freq="D").date
    })
    merged = full_range.merge(df, on="op_day", how="left")
    merged["n_actual"] = merged["n_actual"].fillna(0).astype(int)
    return merged


def enrich(df: pd.DataFrame) -> pd.DataFrame:
    """Añade columnas de esperados, perdidos y porcentajes."""
    d = df.copy()
    d["n_expected"]   = EXPECTED_PER_DAY
    d["n_missing"]    = (d["n_expected"] - d["n_actual"]).clip(lower=0)
    d["pct_missing"]  = (d["n_missing"]  / d["n_expected"] * 100).round(2)
    d["pct_captured"] = (100 - d["pct_missing"]).round(2)
    return d[["op_day", "n_expected", "n_actual", "n_missing", "pct_missing", "pct_captured"]]


# ── Impresión ──────────────────────────────────────────────────────────────

COL_W = 77


def rule(char="─"):
    print(f"  {char * COL_W}")


def print_feed_table(feed: str, df: pd.DataFrame, first_day: date, last_day: date):
    print(f"\n{'═' * (COL_W + 4)}")
    print(f"  FEED: {feed.upper()}")
    print(f"{'═' * (COL_W + 4)}")

    if df.empty:
        print("  Sin datos.\n")
        return

    header = (
        f"  {'Día operativo':<14}  "
        f"{'Esperado':>9}  {'Recibido':>9}  {'Perdidos':>9}  "
        f"{'% perdido':>9}  {'% captura':>9}  nota"
    )
    print(f"\n{header}")
    rule()

    for _, r in df.iterrows():
        d = r["op_day"]
        if d == first_day:
            nota = " * inicio parcial"
        elif d == last_day:
            nota = " * día en curso"
        elif r["n_actual"] == 0:
            nota = " ✗ día completo perdido"
        elif r["pct_missing"] > 20:
            nota = " ⚠ alta pérdida"
        elif r["pct_missing"] > 5:
            nota = " · pérdida moderada"
        else:
            nota = ""

        print(
            f"  {str(d):<14}  "
            f"{int(r['n_expected']):>9,}  "
            f"{int(r['n_actual']):>9,}  "
            f"{int(r['n_missing']):>9,}  "
            f"{r['pct_missing']:>8.2f}%  "
            f"{r['pct_captured']:>8.2f}%"
            f"{nota}"
        )

    rule()
    tot_exp = int(df["n_expected"].sum())
    tot_act = int(df["n_actual"].sum())
    tot_mis = int(df["n_missing"].sum())
    tot_pct_mis = tot_mis / tot_exp * 100
    tot_pct_cap = 100 - tot_pct_mis
    print(
        f"  {'GLOBAL':<14}  "
        f"{tot_exp:>9,}  "
        f"{tot_act:>9,}  "
        f"{tot_mis:>9,}  "
        f"{tot_pct_mis:>8.2f}%  "
        f"{tot_pct_cap:>8.2f}%"
    )
    print(f"\n  Días analizados: {len(df)}")


def print_summary(results: dict):
    print(f"\n{'═' * (COL_W + 4)}")
    print(f"  RESUMEN GLOBAL — TODOS LOS FEEDS")
    print(f"{'═' * (COL_W + 4)}")
    print(f"\n  {'Feed':<22}  {'Días':>5}  {'Esperado':>10}  {'Recibido':>10}  {'Perdidos':>10}  {'% pérdida':>10}")
    rule()

    grand_exp = grand_act = grand_mis = 0
    for feed in FEEDS:
        df = results.get(feed, pd.DataFrame())
        if df.empty:
            print(f"  {feed:<22}  {'—':>5}  {'—':>10}  {'—':>10}  {'—':>10}  {'—':>10}")
            continue
        fe = int(df["n_expected"].sum())
        fa = int(df["n_actual"].sum())
        fm = int(df["n_missing"].sum())
        grand_exp += fe
        grand_act += fa
        grand_mis += fm
        print(
            f"  {feed:<22}  {len(df):>5}  "
            f"{fe:>10,}  {fa:>10,}  {fm:>10,}  "
            f"{fm / fe * 100:>9.2f}%"
        )

    rule()
    gp = grand_mis / grand_exp * 100 if grand_exp else 0.0
    print(
        f"  {'TOTAL (3 feeds)':<22}  {'':>5}  "
        f"{grand_exp:>10,}  {grand_act:>10,}  {grand_mis:>10,}  "
        f"{gp:>9.2f}%"
    )
    print(f"\n  Tasa de captura global : {100 - gp:.2f}%")
    print(f"  Total feeds perdidos   : {grand_mis:,} de {grand_exp:,}")
    print(f"{'═' * (COL_W + 4)}\n")


# ── Main ───────────────────────────────────────────────────────────────────

def main():
    print(f"\n{'#' * (COL_W + 4)}")
    print(f"  INFORME DE FEEDS GTFS-RT PERDIDOS — Cercanías Renfe")
    print(f"  Horario operativo : 05:30h → 00:30h (hora de Madrid / Europe/Madrid)")
    print(f"  Cadencia esperada : {INTERVAL_SEC} s  →  {EXPECTED_PER_DAY:,} snapshots por día operativo")
    print(f"{'#' * (COL_W + 4)}")

    # Primera pasada: obtener conteos sin rellenar huecos
    raw_results: dict[str, pd.DataFrame] = {}
    for feed in FEEDS:
        print(f"\n  Procesando {feed}...", end=" ", flush=True)
        try:
            raw_results[feed] = count_by_day(feed)
            print(f"OK  ({len(raw_results[feed])} días con datos)")
        except Exception as exc:
            print(f"ERROR — {exc}")
            raw_results[feed] = pd.DataFrame()

    # Rango global (días con al menos un snapshot en cualquier feed)
    all_days = [
        d for df in raw_results.values() if not df.empty
        for d in df["op_day"].tolist()
    ]
    first_day = min(all_days) if all_days else date.today()
    last_day  = max(all_days) if all_days else date.today()

    # Segunda pasada: rellenar días sin datos y enriquecer
    results: dict[str, pd.DataFrame] = {}
    for feed in FEEDS:
        raw = raw_results[feed]
        if raw.empty:
            results[feed] = pd.DataFrame()
            continue
        filled = fill_missing_days(raw, first_day, last_day)
        results[feed] = enrich(filled)

    for feed in FEEDS:
        print_feed_table(feed, results[feed], first_day, last_day)

    print_summary(results)

    # Exportar CSV
    frames = [df.assign(feed=feed) for feed, df in results.items() if not df.empty]
    if frames:
        combined = pd.concat(frames, ignore_index=True)
        combined["op_day"] = combined["op_day"].astype(str)
        out = Path("feeds_perdidos.csv")
        combined.to_csv(out, index=False)
        print(f"  Resultados guardados en: {out}\n")


if __name__ == "__main__":
    main()
