import asyncio
from io import BytesIO
import json
from pathlib import Path
import tempfile
import threading
from unittest.mock import patch
import zipfile
import tornado.web
from tornado.testing import AsyncHTTPTestCase,gen_test
from src.download_server import register_download_routes,_slots
from src.data_downloads import download_url
from tests.test_data_downloads import seed


class Health(tornado.web.RequestHandler):
    def get(self):self.write('ok')


class TestDownloadHTTP(AsyncHTTPTestCase):
    def get_app(self):
        self.folder=tempfile.TemporaryDirectory()
        self.cfg,_=seed(Path(self.folder.name))
        app=tornado.web.Application([(r'/health',Health)])
        register_download_routes(app,self.cfg)
        return app

    def tearDown(self):
        super().tearDown()
        self.folder.cleanup()

    def test_streamed_zip_and_headers(self):
        response=self.fetch(download_url('2026-10-27',['raw','parquet'],['trip_updates']))
        assert response.code==200 and response.headers['Content-Type']=='application/zip'
        assert 'attachment;' in response.headers['Content-Disposition']
        with zipfile.ZipFile(BytesIO(response.body)) as archive:
            assert archive.testzip() is None
            assert len(json.loads(archive.read('manifest.json'))['files'])==2

    def test_bad_selection_is_rejected_and_empty_day_is_404(self):
        assert self.fetch('/_anticipa/datos?kind=day&day=../../etc').code==400
        assert self.fetch(download_url('2026-10-26',['raw'],['alerts'])).code==404
        assert self.fetch('/_anticipa/datos?kind=secret').code==400

    def test_concurrency_limit_does_not_queue_unbounded_exports(self):
        assert _slots.acquire(False) and _slots.acquire(False)
        try:
            response=self.fetch(download_url('2026-10-27',['raw'],['trip_updates']))
            assert response.code==429 and response.headers['Retry-After']=='15'
        finally:
            _slots.release();_slots.release()

    @gen_test
    async def test_export_does_not_block_the_http_event_loop(self):
        from src.data_downloads import write_archive
        started=threading.Event();release=threading.Event()
        def delayed(plan,writer,cancelled):
            started.set()
            assert release.wait(3)
            return write_archive(plan,writer,cancelled)
        with patch('src.download_server.write_archive',side_effect=delayed):
            pending=self.http_client.fetch(self.get_url(download_url('2026-10-27',['raw'],['trip_updates'])))
            try:
                for _ in range(100):
                    if started.is_set():break
                    await asyncio.sleep(0.01)
                assert started.is_set()
                health=await self.http_client.fetch(self.get_url('/health'))
                assert health.body==b'ok' and not pending.done()
            finally:release.set()
            response=await pending
            assert response.code==200
