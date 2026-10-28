"""Reprocesa localmente los originales descargados, sin volver a descargarlos."""
import argparse
from datetime import date
from pathlib import Path
from src.config import load
from src.data_downloads import FEEDS,validate_selection
from src.parser import parse_vehicle_positions,parse_trip_updates,parse_alerts,write_parquet


def reprocess(cfg,feed,start,end):
    validate_selection(start,['raw'],[feed]);validate_selection(end,['raw'],[feed])
    if start>end:
        raise ValueError('La fecha inicial debe ser anterior o igual a la final')
    raw=Path(cfg['storage']['raw_dir'])/feed
    bronze=Path(cfg['storage']['bronze_dir'])
    parser={'vehicle_positions':parse_vehicle_positions,'trip_updates':parse_trip_updates,'alerts':parse_alerts}[feed]
    counts=dict(created=0,existing=0,empty=0,failed=0)
    if not raw.exists():
        return counts
    for folder in sorted(raw.iterdir()):
        if not folder.is_dir() or not start<=folder.name<=end:
            continue
        try:date.fromisoformat(folder.name)
        except ValueError:continue
        for original in sorted(folder.glob(feed+'_*.pb')):
            stamp=original.stem[len(feed)+1:]
            expected=bronze/feed/('date='+folder.name)/('part-'+stamp+'.parquet')
            if expected.exists():
                counts['existing']+=1;continue
            try:
                frame=parser(original)
                if frame.empty:
                    counts['empty']+=1;continue
                # El nombre debe seguir la misma fecha de la fuente que el contenido.
                from datetime import datetime,timezone
                actual=datetime.fromtimestamp(int(frame['snapshot_ts'].iloc[0]),timezone.utc).strftime('%Y%m%dT%H%M%SZ')
                if actual!=stamp:
                    raise ValueError('La fecha del nombre y la cabecera no coinciden')
                write_parquet(frame,bronze,feed);counts['created']+=1
            except Exception as exc:
                counts['failed']+=1
                print('No procesado:',original.name,str(exc)[:180])
    return counts


if __name__=='__main__':
    p=argparse.ArgumentParser(description=__doc__)
    p.add_argument('--feed',required=True,choices=FEEDS)
    p.add_argument('--start',required=True,help='Fecha inicial UTC AAAA-MM-DD')
    p.add_argument('--end',required=True,help='Fecha final UTC AAAA-MM-DD')
    args=p.parse_args()
    result=reprocess(load(),args.feed,args.start,args.end)
    print(result)
    raise SystemExit(1 if result['failed'] else 0)
