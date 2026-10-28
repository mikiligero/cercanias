"""Fechas de consulta y formatos comunes del cuadro de mando."""
from datetime import datetime, timedelta, timezone
from zoneinfo import ZoneInfo

MADRID = ZoneInfo('Europe/Madrid')
NUCLEUS_NAMES = {
    '10': 'Madrid', '20': 'Asturias', '30': 'Sevilla', '31': 'Cádiz',
    '32': 'Málaga', '40': 'Valencia', '41': 'Murcia-Alicante',
    '45': 'Cartagena–Los Nietos', '46': 'Ferrol–Ortigueira',
    '51': 'Cataluña (Rodalies)', '60': 'Bilbao', '61': 'Gipuzkoa',
    '62': 'Cantabria', '70': 'Zaragoza',
}
PERIOD_OPTIONS = ['Última hora', 'Últimas 3h', 'Últimas 6h', 'Últimas 12h',
                  'Últimas 24h', 'Hoy', 'Ayer', 'Esta semana', 'Semana pasada',
                  'Este mes', 'Mes pasado', 'Personalizado']


def decimal(value, suffix=''):
    import pandas as pd
    return 'Sin datos' if value is None or pd.isna(value) else f'{value:.1f}'.replace('.', ',') + suffix


def local_time(epoch, fmt='%d/%m/%Y %H:%M:%S'):
    return 'Sin datos' if epoch is None else datetime.fromtimestamp(float(epoch), MADRID).strftime(fmt)


def period_bounds(preset, custom_range=None, now=None):
    """Fechas civiles de Madrid; intervalos inclusivos expresados en epoch UTC."""
    now = (now or datetime.now(timezone.utc)).astimezone(MADRID)
    today = now.replace(hour=0, minute=0, second=0, microsecond=0)
    end = now
    rolling = {'Última hora': 1, 'Últimas 3h': 3, 'Últimas 6h': 6,
               'Últimas 12h': 12, 'Últimas 24h': 24}
    if preset in rolling:
        return int(now.timestamp()) - rolling[preset] * 3600, int(now.timestamp())
    if preset == 'Ayer':
        start, end = today - timedelta(days=1), today
    elif preset == 'Esta semana':
        start = today - timedelta(days=today.weekday())
    elif preset == 'Semana pasada':
        end = today - timedelta(days=today.weekday())
        start = end - timedelta(days=7)
    elif preset == 'Este mes':
        start = today.replace(day=1)
    elif preset == 'Mes pasado':
        end = today.replace(day=1)
        start = (end - timedelta(days=1)).replace(day=1)
    elif preset == 'Personalizado' and custom_range and len(custom_range) == 2:
        start = datetime.combine(custom_range[0], datetime.min.time(), MADRID)
        end = datetime.combine(custom_range[1] + timedelta(days=1), datetime.min.time(), MADRID)
    else:
        start = today
    closed = preset in ('Ayer', 'Semana pasada', 'Mes pasado', 'Personalizado')
    return int(start.timestamp()), int(end.timestamp()) - int(closed)


def madrid_series(series):
    """Plotly recibe horas locales sin tz para evitar que el navegador las reconvierta."""
    import pandas as pd
    return pd.to_datetime(series, utc=True).dt.tz_convert(MADRID).dt.tz_localize(None)
