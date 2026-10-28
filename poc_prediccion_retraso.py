#!/usr/bin/env python3
"""
PoC — Clasificador de retraso en Cercanías Renfe
¿Llegará un tren con más de 5 minutos de retraso a la siguiente parada?

Modelo    : Random Forest (validación temporal, sin data leakage)
Train     : antes del 2026-06-01
Test      : 2026-06-01 en adelante (excluyendo días con pipeline caído)
"""
import re
import zipfile
from datetime import date
from pathlib import Path

import duckdb
import numpy as np
import pandas as pd
from sklearn.ensemble import RandomForestClassifier
from sklearn.metrics import classification_report, confusion_matrix, roc_auc_score
from sklearn.preprocessing import LabelEncoder

# ── Configuración ────────────────────────────────────────────────────────────
BRONZE_DIR   = Path("data/bronze")
GTFS_ZIP     = Path("data/raw/gtfs_static/fomento_transit.zip")
TZ           = "Europe/Madrid"

DELAY_THRESH  = 5 * 60        # target: retraso > 5 min (300 s)
TEST_FROM     = date(2026, 6, 1)
SKIP_DAYS     = {date(2026, 6, 3), date(2026, 6, 4)}   # pipeline caído
TRAIN_SAMPLE  = 400_000       # sub-muestreo del train para velocidad
N_ESTIMATORS  = 150
RANDOM_STATE  = 42

LINE_RE = re.compile(r'((?:MD|LD|C|R)\d+[A-Z]*)$')


# ── Helpers ──────────────────────────────────────────────────────────────────

def _read_gtfs(zf, name: str) -> pd.DataFrame:
    with zf.open(f'{name}.txt') as f:
        df = pd.read_csv(f, dtype=str)
    df.columns = df.columns.str.strip()
    for c in df.select_dtypes('object'):
        df[c] = df[c].str.strip()
    return df


def _parse_time_min(t: str) -> int:
    """HH:MM:SS → minutos desde medianoche. Soporta horarios >24h."""
    h, m = int(t[:2]), int(t[3:5])
    return h * 60 + m


# ── Carga datos ──────────────────────────────────────────────────────────────

def load_gtfs_tables() -> dict[str, pd.DataFrame]:
    print("  Cargando GTFS estático...", end=" ", flush=True)
    with zipfile.ZipFile(GTFS_ZIP) as zf:
        tables = {n: _read_gtfs(zf, n) for n in ('stop_times', 'trips', 'routes')}
    print("OK")
    return tables


def build_stop_features(stop_times: pd.DataFrame) -> pd.DataFrame:
    """
    A partir de stop_times crea una tabla con features de posición en ruta
    indexada por (trip_id, stop_id):
      - sched_min      : minutos desde medianoche de la llegada programada
      - n_stops        : total de paradas del viaje
      - stop_rank_pct  : posición relativa en la ruta (0 = origen, 1 = destino)
      - min_to_end     : minutos programados hasta el final del trayecto
    """
    st = stop_times.copy()
    st['stop_sequence'] = st['stop_sequence'].astype(int)
    st['sched_min']     = st['arrival_time'].apply(_parse_time_min)

    trip_ext = st.groupby('trip_id').agg(
        n_stops      =('stop_sequence', 'count'),
        trip_end_min =('sched_min', 'max'),
    ).reset_index()

    st = st.merge(trip_ext, on='trip_id')
    st['stop_rank_pct'] = st['stop_sequence'] / st['n_stops']
    st['min_to_end']    = st['trip_end_min'] - st['sched_min']

    return st[['trip_id', 'stop_id', 'sched_min', 'n_stops',
               'stop_rank_pct', 'min_to_end']].drop_duplicates(['trip_id', 'stop_id'])


def load_rt_data() -> pd.DataFrame:
    """Lee trip_updates desde Parquet con DuckDB."""
    print("  Leyendo trip_updates desde Parquet...", end=" ", flush=True)
    con = duckdb.connect()
    df = con.execute(f"""
        SELECT snapshot_ts, trip_id, stop_id, stop_sequence, arrival_delay
        FROM read_parquet('{BRONZE_DIR}/trip_updates/**/**.parquet',
                          hive_partitioning=true)
        WHERE arrival_delay IS NOT NULL
          AND trip_id  IS NOT NULL
          AND stop_id  IS NOT NULL
    """).df()
    con.close()
    print(f"OK  ({len(df):,} filas)")
    return df


# ── Feature engineering ───────────────────────────────────────────────────────

FEATURES_BASE   = ['linea_enc', 'hora', 'dia_semana', 'is_weekend',
                   'is_peak', 'stop_sequence']
FEATURES_ENRICH = ['sched_min', 'n_stops', 'stop_rank_pct', 'min_to_end']
ALL_FEATURES    = FEATURES_BASE + FEATURES_ENRICH


def engineer_features(rt: pd.DataFrame, stop_feats: pd.DataFrame) -> pd.DataFrame:
    print("  Calculando features...", end=" ", flush=True)
    df = rt.copy()

    # Línea desde trip_id (regex)
    df['linea'] = df['trip_id'].str.extract(LINE_RE)[0].fillna('OTRA')

    # Features temporales (hora Madrid)
    local         = pd.to_datetime(df['snapshot_ts'], unit='s', utc=True).dt.tz_convert(TZ)
    df['hora']        = local.dt.hour.astype(np.int8)
    df['dia_semana']  = local.dt.dayofweek.astype(np.int8)   # 0=lun … 6=dom
    df['is_weekend']  = (df['dia_semana'] >= 5).astype(np.int8)
    df['is_peak']     = (
        df['hora'].between(7, 9) | df['hora'].between(17, 20)
    ).astype(np.int8)
    df['fecha'] = local.dt.date

    # Enriquecer con posición en ruta (left join → NaN para ~60% sin match)
    df = df.merge(stop_feats, on=['trip_id', 'stop_id'], how='left')

    # Codificar línea
    le = LabelEncoder()
    df['linea_enc'] = le.fit_transform(df['linea'])

    # Target
    df['retrasado'] = (df['arrival_delay'] > DELAY_THRESH).astype(np.int8)

    # Rellenar NaN de features enriquecidas con -1 (señal de "no disponible")
    df[FEATURES_ENRICH] = df[FEATURES_ENRICH].fillna(-1)

    print(f"OK  (enriquecidas con GTFS: {(df['sched_min'] >= 0).mean()*100:.1f}%)")
    return df, le


# ── Split temporal ────────────────────────────────────────────────────────────

def temporal_split(df: pd.DataFrame):
    train = df[df['fecha'] < TEST_FROM].copy()
    test  = df[(df['fecha'] >= TEST_FROM) & (~df['fecha'].isin(SKIP_DAYS))].copy()
    return train, test


# ── Modelo ────────────────────────────────────────────────────────────────────

def train(train_df: pd.DataFrame) -> RandomForestClassifier:
    print(f"  Entrenando Random Forest ({N_ESTIMATORS} árboles)...", end=" ", flush=True)

    sample = (train_df.sample(n=TRAIN_SAMPLE, random_state=RANDOM_STATE)
              if len(train_df) > TRAIN_SAMPLE else train_df)

    X = sample[ALL_FEATURES].values
    y = sample['retrasado'].values

    clf = RandomForestClassifier(
        n_estimators  = N_ESTIMATORS,
        max_depth     = 18,
        min_samples_leaf = 15,
        class_weight  = 'balanced',    # compensa desequilibrio de clases
        n_jobs        = -1,
        random_state  = RANDOM_STATE,
    )
    clf.fit(X, y)
    print("OK")
    return clf


# ── Evaluación ────────────────────────────────────────────────────────────────

def evaluate(clf: RandomForestClassifier,
             test_df: pd.DataFrame,
             le: LabelEncoder) -> None:
    X = test_df[ALL_FEATURES].values
    y = test_df['retrasado'].values

    y_pred  = clf.predict(X)
    y_proba = clf.predict_proba(X)[:, 1]
    auc     = roc_auc_score(y, y_proba)

    SEP = "═" * 62

    print(f"\n{SEP}")
    print(f"  RESULTADOS — conjunto de prueba ({TEST_FROM} en adelante)")
    print(SEP)
    print(f"  Filas test   : {len(test_df):,}")
    print(f"  % retrasados : {y.mean()*100:.1f}%  (retraso real > 5 min)")
    print()
    print(classification_report(y, y_pred,
                                target_names=['A tiempo (≤5 min)',
                                              'Retrasado (>5 min)'],
                                digits=3))
    print(f"  ROC-AUC : {auc:.4f}")

    # Confusion matrix
    cm = confusion_matrix(y, y_pred)
    print(f"\n  Matriz de confusión:")
    print(f"  {'':20}  {'Pred: a tiempo':>16}  {'Pred: retrasado':>16}")
    print(f"  {'Real: a tiempo':20}  {cm[0,0]:>16,}  {cm[0,1]:>16,}")
    print(f"  {'Real: retrasado':20}  {cm[1,0]:>16,}  {cm[1,1]:>16,}")

    # Feature importance
    fi = pd.Series(clf.feature_importances_, index=ALL_FEATURES).sort_values(ascending=False)

    print(f"\n{SEP}")
    print(f"  IMPORTANCIA DE FEATURES")
    print(SEP)
    for feat, imp in fi.items():
        label = 'linea' if feat == 'linea_enc' else feat
        bar   = '█' * max(1, int(imp * 50))
        print(f"  {label:<20}  {imp:.4f}  {bar}")

    # Análisis por línea
    print(f"\n{SEP}")
    print(f"  TASA DE RETRASO POR LÍNEA (test set, reales)")
    print(SEP)
    lineas = (test_df.groupby('linea')
              .agg(n=('retrasado', 'count'),
                   pct_retrasado=('retrasado', 'mean'))
              .query('n > 100')
              .sort_values('pct_retrasado', ascending=False)
              .head(15))
    lineas['pct_retrasado'] = (lineas['pct_retrasado'] * 100).round(1)
    print(lineas[['n', 'pct_retrasado']].rename(
        columns={'n': 'obs', 'pct_retrasado': '% retrasado'}).to_string())

    # Análisis por hora
    print(f"\n{SEP}")
    print(f"  TASA DE RETRASO POR HORA (test set, reales)")
    print(SEP)
    by_hour = (test_df.groupby('hora')['retrasado'].mean() * 100).round(1)
    for h, v in by_hour.items():
        bar = '█' * int(v / 2)
        print(f"  {h:02d}h  {v:5.1f}%  {bar}")

    print(f"\n{SEP}")
    print(f"  PoC completada.  ROC-AUC = {auc:.4f}")
    print(SEP)


# ── Main ──────────────────────────────────────────────────────────────────────

def main():
    print(f"\n{'#' * 62}")
    print(f"  PoC — Predicción de retraso >5 min en Cercanías Renfe")
    print(f"  Modelo: Random Forest  |  Split: train < {TEST_FROM} / test ≥ {TEST_FROM}")
    print(f"{'#' * 62}\n")

    # Carga
    gtfs       = load_gtfs_tables()
    stop_feats = build_stop_features(gtfs['stop_times'])
    rt         = load_rt_data()

    # Features
    df, le = engineer_features(rt, stop_feats)

    # Resumen dataset
    train_df, test_df = temporal_split(df)
    pct_enriq = (df['sched_min'] >= 0).mean() * 100
    print(f"\n  Resumen dataset:")
    print(f"    Total filas        : {len(df):,}")
    print(f"    Train              : {len(train_df):,}  (hasta {TEST_FROM})")
    print(f"    Test               : {len(test_df):,}  (desde {TEST_FROM})")
    print(f"    % retrasados global: {df['retrasado'].mean()*100:.1f}%")
    print(f"    Líneas únicas      : {df['linea'].nunique()}")
    print(f"    Con enriq. GTFS    : {pct_enriq:.1f}%")
    print(f"    Features           : {ALL_FEATURES}\n")

    # Modelo
    clf = train(train_df)
    evaluate(clf, test_df, le)


if __name__ == '__main__':
    main()
