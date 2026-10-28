from datetime import datetime,timezone
import hashlib
from io import BytesIO
import json
import os
from pathlib import Path
import zipfile
import pytest
from src.data_downloads import (select_files,write_archive,SelectionError,download_url,
                               archive_name,downloader_script,select_period,period_url)
from src.download_view import export_rows
from reprocess_downloads import reprocess


def seed(tmp_path):
    cfg={'storage':{'raw_dir':str(tmp_path/'raw'),'bronze_dir':str(tmp_path/'bronze')}}
    paths=[]
    for fmt in ('raw','parquet'):
        folder=tmp_path/('raw' if fmt=='raw' else 'bronze')/'trip_updates'/('2026-10-27' if fmt=='raw' else 'date=2026-10-27')
        folder.mkdir(parents=True)
        name='trip_updates_20261027T120000Z.pb' if fmt=='raw' else 'part-20261027T120000Z.parquet'
        path=folder/name;path.write_bytes((fmt*1000).encode())
        os.utime(path,(1793102400,1793102400));paths.append(path)
    return cfg,paths


def test_zip_preserves_paths_bytes_and_hashes(tmp_path):
    cfg,paths=seed(tmp_path)
    plan=select_files(cfg,'2026-10-27',['raw','parquet'],['trip_updates'])
    before=[p.read_bytes() for p in paths]
    output=BytesIO();manifest=write_archive(plan,output)
    with zipfile.ZipFile(BytesIO(output.getvalue())) as archive:
        assert archive.testzip() is None
        assert 'LEEME-DATOS.md' in archive.namelist()
        saved=json.loads(archive.read('manifest.json'))
        assert saved==manifest and not saved['filter_applied']
        assert len(saved['files'])==2
        for item in saved['files']:
            assert item['path'].startswith('data/') and '..' not in item['path']
            content=archive.read(item['path'])
            assert len(content)==item['size'] and hashlib.sha256(content).hexdigest()==item['sha256']
    assert [p.read_bytes() for p in paths]==before


@pytest.mark.parametrize('day,formats,feeds',[
    ('../../etc/passwd',['raw'],['trip_updates']),('2026-02-30',['raw'],['trip_updates']),
    ('2026-10-27',['env'],['trip_updates']),('2026-10-27',['raw'],['../gtfs_static']),
    ('2026-10-27',[],['trip_updates']),('2026-10-27',['raw'],[])])
def test_invalid_selection_cannot_address_other_files(tmp_path,day,formats,feeds):
    cfg,_=seed(tmp_path)
    with pytest.raises(SelectionError):select_files(cfg,day,formats,feeds)


def test_symlink_and_temporary_files_are_not_exported(tmp_path):
    cfg,paths=seed(tmp_path)
    secret=tmp_path/'secret';secret.write_bytes(b'secret')
    folder=paths[0].parent
    (folder/'trip_updates_20261027T130000Z.pb').symlink_to(secret)
    (folder/'part.tmp').write_bytes(b'incomplete')
    plan=select_files(cfg,'2026-10-27',['raw'],['trip_updates'])
    assert [item.path for item in plan['files']]==[paths[0]]


def test_changed_or_recent_file_is_not_silently_included(tmp_path):
    cfg,paths=seed(tmp_path)
    plan=select_files(cfg,'2026-10-27',['raw'],['trip_updates'])
    paths[0].write_bytes(b'changed')
    with pytest.raises(RuntimeError):write_archive(plan,BytesIO())
    with pytest.raises(FileNotFoundError):select_files(cfg,'2026-10-27',['raw'],['trip_updates'])


def test_gtfs_is_an_explicit_separate_download(tmp_path):
    cfg,_=seed(tmp_path);static=tmp_path/'raw/gtfs_static';static.mkdir()
    path=static/'fomento_transit.zip';path.write_bytes(b'reference');os.utime(path,(1793102400,1793102400))
    plan=select_files(cfg,kind='gtfs')
    assert len(plan['files'])==1 and plan['files'][0].archive_path=='data/raw/gtfs_static/fomento_transit.zip'


def test_generated_script_matches_day_selection_and_compiles():
    script=downloader_script(['2026-10-27'],['parquet'],['trip_updates'],True)
    code=compile(script,'descargar_anticipa.py','exec')
    scope={'__name__':'test_script'};exec(code,scope)
    assert len(scope['TASKS'])==2
    assert scope['TASKS'][0]['url'].endswith(download_url('2026-10-27',['parquet'],['trip_updates']))
    assert scope['TASKS'][0]['name']==archive_name('2026-10-27',['parquet'],['trip_updates'])


def test_export_summary_does_not_mix_unselected_sources_or_formats():
    rows={'a':{'feed':'trip_updates','date':'2026-10-27','raw_files':4,'parquet_files':3,'raw_bytes':40,'parquet_bytes':30},
          'b':{'feed':'alerts','date':'2026-10-27','raw_files':8,'parquet_files':7,'raw_bytes':80,'parquet_bytes':70}}
    selected=export_rows({'partitions':rows},['parquet'],['trip_updates'],'2026-10-27','2026-10-27')
    assert selected==[dict(day='2026-10-27',files=3,bytes=30,raw_files=0,parquet_files=3)]


def test_local_reprocessing_keeps_source_and_skips_existing(tmp_path,tu_pb):
    cfg={'storage':{'raw_dir':str(tmp_path/'raw'),'bronze_dir':str(tmp_path/'bronze')}}
    raw=tmp_path/'raw/trip_updates/2026-05-17';raw.mkdir(parents=True)
    source=raw/tu_pb.name;content=tu_pb.read_bytes();source.write_bytes(content)
    first=reprocess(cfg,'trip_updates','2026-05-17','2026-05-17')
    second=reprocess(cfg,'trip_updates','2026-05-17','2026-05-17')
    assert first['created']==1 and second['existing']==1 and second['created']==0
    assert source.read_bytes()==content


def seed_period(tmp_path):
    cfg,paths=seed(tmp_path)
    for day in ('2026-10-26','2026-10-28'):
        folder=tmp_path/'raw/trip_updates'/day;folder.mkdir()
        file=folder/('trip_updates_'+day.replace('-','')+'T120000Z.pb')
        file.write_bytes(day.encode());os.utime(file,(1793000000,1793000000))
    return cfg


def test_multiple_days_share_one_zip_with_bounded_indexes(tmp_path,monkeypatch):
    cfg=seed_period(tmp_path)
    static=tmp_path/'raw/gtfs_static';static.mkdir()
    gtfs=static/'fomento_transit.zip';gtfs.write_bytes(b'GTFS');os.utime(gtfs,(1793000000,1793000000))
    monkeypatch.setattr('src.data_downloads.INDEX_MEMORY_LIMIT',100)
    plan=select_period(cfg,'2026-10-26','2026-10-27',['raw'],['trip_updates'],True)
    assert not isinstance(plan['files'],list)
    output=BytesIO();result=write_archive(plan,output)
    with zipfile.ZipFile(BytesIO(output.getvalue())) as archive:
        assert archive.testzip() is None
        manifest=json.loads(archive.read('manifest.json'))
        assert manifest['start']=='2026-10-26' and manifest['end']=='2026-10-27'
        assert len(manifest['files'])==3 and result['file_count']==3
        names=[r['path'] for r in manifest['files']]
        assert sum('gtfs_static' in name for name in names)==1
        assert not any('2026-10-28' in name for name in names)
        assert any('2026-10-26' in name for name in names) and any('2026-10-27' in name for name in names)
        assert all(hashlib.sha256(archive.read(r['path'])).hexdigest()==r['sha256'] for r in manifest['files'])


def test_period_dates_and_optional_reference(tmp_path):
    cfg=seed_period(tmp_path)
    with pytest.raises(SelectionError):select_period(cfg,'2026-10-28','2026-10-26',['raw'],['trip_updates'])
    with pytest.raises(SelectionError):select_period(cfg,'../etc','2026-10-26',['raw'],['trip_updates'])
    with pytest.raises(FileNotFoundError):select_period(cfg,'2020-01-01','2020-01-02',['raw'],['trip_updates'])
    assert 'kind=period' in period_url('2026-10-26','2026-10-27',['raw'],['trip_updates'],False)


def test_disk_index_supports_zip64_file_counts():
    from src.data_downloads import _DiskIndex
    output=BytesIO();index=_DiskIndex()
    try:
        with zipfile.ZipFile(output,'w',allowZip64=True) as archive:
            archive.filelist=index
            for n in range(66000):
                archive.writestr(f'data/test/{n}.pb',b'')
                archive.NameToInfo.clear()
        with zipfile.ZipFile(BytesIO(output.getvalue())) as archive:
            assert len(archive.infolist())==66000
            assert archive.read('data/test/65999.pb')==b''
    finally:index.close()
