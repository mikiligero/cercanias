"""Selección acotada y ZIP por streaming de los archivos originales conservados."""
from dataclasses import dataclass
from datetime import date, datetime, timezone
import hashlib
import json
import os
from pathlib import Path
import re
import stat
import threading
import time
from urllib.parse import urlencode
import zipfile

FEEDS = ('vehicle_positions', 'trip_updates', 'alerts')
FORMATS = ('raw', 'parquet')
PUBLIC_BASE = 'https://cercanias.mikines.es'
DOWNLOAD_PAGE_URL = PUBLIC_BASE + '/?pagina=descargas'
ENDPOINT = '/_anticipa/datos'
MAX_FILES = 40000
MAX_BYTES = 2 * 1024**3
CHUNK_SIZE = 64 * 1024


@dataclass(frozen=True)
class DownloadFile:
    path: Path
    archive_path: str
    size: int
    mtime_ns: int
    inode: int


class SelectionError(ValueError):
    pass


def validate_selection(day, formats, feeds):
    if not re.fullmatch(r'\d{4}-\d{2}-\d{2}', str(day)):
        raise SelectionError('Fecha no válida; utiliza AAAA-MM-DD.')
    try:
        date.fromisoformat(day)
    except ValueError:
        raise SelectionError('Fecha no válida.') from None
    if not formats or any(value not in FORMATS for value in formats):
        raise SelectionError('Elige bruto, Parquet o ambos.')
    if not feeds or any(value not in FEEDS for value in feeds):
        raise SelectionError('Elige una de las fuentes disponibles.')
    return tuple(value for value in FORMATS if value in formats), tuple(value for value in FEEDS if value in feeds)


def _file(path, root, archive_path, cutoff):
    if path.is_symlink():
        return None
    resolved = path.resolve(strict=True)
    if not resolved.is_relative_to(root.resolve()):
        raise SelectionError('El archivo no pertenece al conjunto de datos publicado.')
    info = resolved.stat()
    if not stat.S_ISREG(info.st_mode) or info.st_mtime > cutoff:
        return None
    return DownloadFile(resolved, archive_path, info.st_size, info.st_mtime_ns, info.st_ino)


def select_files(cfg, day=None, formats=(), feeds=(), kind='day', cutoff=None):
    cutoff = time.time()-5 if cutoff is None else cutoff
    files = []
    skipped = 0
    if kind == 'gtfs':
        root = Path(cfg['storage']['raw_dir'])
        path = root/'gtfs_static/fomento_transit.zip'
        try:
            item = _file(path, root, 'data/raw/gtfs_static/fomento_transit.zip', cutoff)
        except FileNotFoundError:
            item = None
        if item:
            files.append(item)
        formats, feeds = (), ()
    elif kind == 'day':
        formats, feeds = validate_selection(day, formats, feeds)
        for fmt in formats:
            root = Path(cfg['storage']['raw_dir' if fmt == 'raw' else 'bronze_dir'])
            for feed in feeds:
                folder = root/feed/(day if fmt == 'raw' else 'date='+day)
                if not folder.exists():
                    continue
                if not folder.resolve().is_relative_to(root.resolve()):
                    raise SelectionError('La carpeta no pertenece al conjunto publicado.')
                pattern = re.compile((re.escape(feed)+'_' if fmt == 'raw' else 'part-')+r'\d{8}T\d{6}Z\.'+('pb' if fmt == 'raw' else 'parquet'))
                for entry in os.scandir(folder):
                    if not pattern.fullmatch(entry.name) or not entry.is_file(follow_symlinks=False):
                        continue
                    path = Path(entry.path)
                    archive_path = 'data/'+('raw' if fmt == 'raw' else 'bronze')+'/'+feed+'/'+folder.name+'/'+path.name
                    try:
                        item = _file(path, root, archive_path, cutoff)
                    except FileNotFoundError:
                        skipped += 1
                        continue
                    if item:
                        files.append(item)
                    else:
                        skipped += 1
                    if len(files)>MAX_FILES:
                        raise SelectionError('Este paquete contiene demasiados archivos. Elige una fuente o un formato cada vez.')
    else:
        raise SelectionError('Tipo de descarga no válido.')
    if not files:
        raise FileNotFoundError('No hay archivos estables disponibles para esta selección.')
    if sum(item.size for item in files)>MAX_BYTES:
        raise SelectionError('Este paquete supera 2 GiB sin comprimir. Elige una fuente o un formato cada vez.')
    return {'kind':kind,'day':day,'formats':list(formats),'feeds':list(feeds),
            'files':sorted(files,key=lambda item:item.archive_path),'skipped_unstable':skipped,
            'selected_at':datetime.now(timezone.utc).isoformat()}


def archive_name(day=None, formats=(), feeds=(), kind='day'):
    if kind=='gtfs':
        return 'anticipa-gtfs-disponible.zip'
    formats,feeds=validate_selection(day,formats,feeds)
    short={'vehicle_positions':'posiciones','trip_updates':'viajes','alerts':'avisos'}
    return 'anticipa-'+day+'-'+'-'.join(formats)+'-'+'-'.join(short[f] for f in feeds)+'.zip'


def download_url(day=None, formats=(), feeds=(), kind='day', base=''):
    params={'kind':kind}
    if kind=='day':
        formats,feeds=validate_selection(day,formats,feeds)
        params.update(day=day,formats=','.join(formats),feeds=','.join(feeds))
    return base.rstrip('/')+ENDPOINT+'?'+urlencode(params)


def _readme(plan):
    return '''# Datos del piloto Anticipa

## Colocar los archivos

1. Clona o descarga https://github.com/mikiligero/cercanias.
2. Descomprime este ZIP en la carpeta principal del proyecto, junto a dashboard.py y config.yaml.
3. Conserva la carpeta data/ y todos sus nombres internos. Si descargas varios días, combina sus carpetas data/.

Estructura:

    cercanias/
      dashboard.py
      config.yaml
      data/
        raw/vehicle_positions/AAAA-MM-DD/vehicle_positions_...pb
        raw/trip_updates/AAAA-MM-DD/trip_updates_...pb
        raw/alerts/AAAA-MM-DD/alerts_...pb
        raw/gtfs_static/fomento_transit.zip
        bronze/vehicle_positions/date=AAAA-MM-DD/part-...parquet
        bronze/trip_updates/date=AAAA-MM-DD/part-...parquet
        bronze/alerts/date=AAAA-MM-DD/part-...parquet

Cada paquete incluye solo los formatos y fuentes solicitados. El bruto (.pb) es el mensaje original de Renfe.
Parquet es el resultado del parser. Los datos se exportan sin aplicar el filtro analítico de retrasos.
Las fechas de los paquetes y de las carpetas son UTC. La fecha de la fuente puede diferir de la de descarga.

## Reutilizar

Parquet se puede consultar directamente con DuckDB, pandas o cualquier lector compatible.
Para abrir el cuadro de mando local, sigue README.md del repositorio. Necesitas los Parquet de las tres fuentes
y el GTFS estático. El archivo de avisos puede tener una fecha distinta: se conserva el último recibido,
y su ausencia en el período no significa que no existieran incidencias.
Si faltan referencias de estaciones, ejecuta main.py para preparar las fuentes estáticas actuales.
El bruto por sí solo necesita procesamiento; las instrucciones de reprocesado están en DESCARGAR_DATOS.md.

## Alcance

manifest.json contiene el corte de la selección, las rutas, los tamaños y SHA-256 de todos los archivos incluidos.
No se exportan contraseñas, configuraciones privadas, copias de recuperación ni datos ajenos a las fuentes elegidas.
Se excluyen archivos modificados en los últimos cinco segundos para evitar leer capturas en escritura.
El ZIP solo se completa si los archivos siguen coincidiendo con la selección durante su lectura.
La referencia GTFS, si este paquete la contiene, es la copia disponible al descargar; puede ser posterior al período.
Los mensajes originales pueden estar vacíos o contener anomalías. La descarga conserva su contenido; no lo valida.
'''


def write_archive(plan, output, cancelled=None):
    """Memoria acotada: copia en bloques y escribe el índice de una sola jornada."""
    cancelled=cancelled or threading.Event()
    manifest={k:v for k,v in plan.items() if k!='files'}
    manifest.update(version=1,timezone='UTC',files=[],filter_applied=False)
    with zipfile.ZipFile(output,'w',compression=zipfile.ZIP_DEFLATED,compresslevel=1,allowZip64=True) as archive:
        archive.writestr('LEEME-DATOS.md',_readme(plan))
        for item in plan['files']:
            if cancelled.is_set():
                raise InterruptedError('Descarga cancelada')
            fd=os.open(item.path,os.O_RDONLY | getattr(os,'O_NOFOLLOW',0))
            with os.fdopen(fd,'rb') as source:
                before=os.fstat(source.fileno())
                if (before.st_size,before.st_mtime_ns,before.st_ino)!=(item.size,item.mtime_ns,item.inode):
                    raise RuntimeError('Un archivo ha cambiado. Repite la descarga de este día.')
                digest=hashlib.sha256();size=0
                stamp=datetime.fromtimestamp(max(before.st_mtime,315532800),timezone.utc)
                info=zipfile.ZipInfo(item.archive_path,stamp.timetuple()[:6])
                info.compress_type=zipfile.ZIP_DEFLATED
                info._compresslevel=1  # Nombre disponible en Python 3.12.
                info.external_attr=0o100644<<16
                info.file_size=item.size
                with archive.open(info,'w',force_zip64=True) as target:
                    while chunk:=source.read(CHUNK_SIZE):
                        if cancelled.is_set():
                            raise InterruptedError('Descarga cancelada')
                        digest.update(chunk);size+=len(chunk);target.write(chunk)
                after=os.fstat(source.fileno())
                if (size,after.st_size,after.st_mtime_ns,after.st_ino)!=(item.size,item.size,item.mtime_ns,item.inode):
                    raise RuntimeError('Un archivo ha cambiado durante la descarga.')
                manifest['files'].append(dict(path=item.archive_path,size=size,sha256=digest.hexdigest(),
                    modified_utc=datetime.fromtimestamp(before.st_mtime,timezone.utc).isoformat()))
        manifest['completed_at']=datetime.now(timezone.utc).isoformat()
        archive.writestr('manifest.json',json.dumps(manifest,ensure_ascii=False,indent=2))
    return manifest


def downloader_script(days,formats,feeds,include_gtfs=False,base=PUBLIC_BASE):
    """Ayuda opcional para obtener varios días, sin dependencias adicionales."""
    tasks=[dict(name=archive_name(day,formats,feeds),url=download_url(day,formats,feeds,base=base)) for day in days]
    if include_gtfs:
        tasks.append(dict(name=archive_name(kind='gtfs'),url=download_url(kind='gtfs',base=base)))
    return '''#!/usr/bin/env python3
"""Descarga los paquetes elegidos de Anticipa. Ejecuta: python descargar_anticipa.py"""
import argparse
from pathlib import Path
import time
import urllib.request
import zipfile

TASKS = '''+repr(tasks)+'''

def main():
    parser=argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--dest',default='descargas_anticipa',help='Carpeta para guardar los ZIP')
    args=parser.parse_args()
    folder=Path(args.dest);folder.mkdir(parents=True,exist_ok=True)
    failures=[]
    for i,task in enumerate(TASKS,1):
        target=folder/task['name'];partial=target.with_suffix('.zip.partial')
        if target.exists() and zipfile.is_zipfile(target):
            print(f"[{i}/{len(TASKS)}] Ya existe: {target.name}");continue
        for attempt in range(3):
            try:
                print(f"[{i}/{len(TASKS)}] Descargando {target.name}",flush=True)
                with urllib.request.urlopen(task['url'],timeout=120) as response,partial.open('wb') as output:
                    while chunk:=response.read(1024*1024):output.write(chunk)
                if not zipfile.is_zipfile(partial):raise ValueError('La descarga no contiene un ZIP completo')
                partial.replace(target);break
            except Exception as exc:
                partial.unlink(missing_ok=True)
                if attempt==2:
                    failures.append(task['name']);print('No descargado:',exc)
                else:time.sleep(10*(attempt+1))
    print('ZIP guardados en',folder.resolve())
    print('Descomprime los ZIP en la carpeta principal del proyecto cercanias, conservando data/.')
    if failures:raise SystemExit('Quedan paquetes pendientes: '+', '.join(failures))

if __name__=='__main__':main()
'''
