"""Descargas por el mismo puerto de Streamlit, sin archivos temporales de datos."""
import asyncio
from concurrent.futures import ThreadPoolExecutor
import io
import logging
import queue
import threading
import tornado.iostream
import tornado.web
from src.data_downloads import (CHUNK_SIZE,ENDPOINT,SelectionError,archive_name,select_files,
                                select_period,period_name,write_archive)

logger=logging.getLogger(__name__)
_workers=ThreadPoolExecutor(max_workers=2,thread_name_prefix='anticipa-export')
_slots=threading.BoundedSemaphore(2)
_END=object()


class QueueWriter(io.RawIOBase):
    def __init__(self,chunks,cancelled):
        self.chunks=chunks;self.cancelled=cancelled;self.position=0

    def writable(self):return True
    def seekable(self):return False
    def tell(self):return self.position

    def write(self,data):
        size=len(data)
        for offset in range(0,size,CHUNK_SIZE):
            push(self.chunks,bytes(data[offset:offset+CHUNK_SIZE]),self.cancelled)
        self.position+=size
        return size


def push(chunks,item,cancelled):
    while not cancelled.is_set():
        try:
            chunks.put(item,timeout=0.2);return
        except queue.Full:
            pass
    raise InterruptedError('El cliente ha cerrado la descarga')


def produce(plan,chunks,cancelled):
    try:
        with io.BufferedWriter(QueueWriter(chunks,cancelled),buffer_size=CHUNK_SIZE) as output:
            write_archive(plan,output,cancelled)
        push(chunks,_END,cancelled)
    except Exception as exc:
        if not cancelled.is_set():
            try:push(chunks,exc,cancelled)
            except InterruptedError:pass


class DatasetDownloadHandler(tornado.web.RequestHandler):
    def initialize(self,cfg):
        self.cfg=cfg
        self.cancelled=threading.Event()

    def on_connection_close(self):
        self.cancelled.set()

    def write_error(self,status_code,**kwargs):
        if status_code==429:
            self.set_header('Retry-After','15')
        super().write_error(status_code,**kwargs)

    async def get(self):
        kind=self.get_query_argument('kind','day')
        day=self.get_query_argument('day',None)
        formats=tuple(filter(None,self.get_query_argument('formats','').split(',')))
        feeds=tuple(filter(None,self.get_query_argument('feeds','').split(',')))
        if not _slots.acquire(blocking=False):
            self.set_header('Retry-After','15')
            raise tornado.web.HTTPError(429,reason='Hay dos descargas en curso. Inténtalo al terminar una de ellas.')
        producer=None
        try:
            loop=asyncio.get_running_loop()
            try:
                if kind=='period':
                    start=self.get_query_argument('start',None)
                    end=self.get_query_argument('end',None)
                    reference=self.get_query_argument('gtfs','1')
                    if reference not in ('0','1'):
                        raise SelectionError('La opción GTFS debe ser 0 o 1.')
                    plan=await loop.run_in_executor(_workers,lambda:select_period(self.cfg,start,end,formats,feeds,reference=='1'))
                    filename=period_name(start,end,formats,feeds)
                else:
                    plan=await loop.run_in_executor(_workers,lambda:select_files(self.cfg,day,formats,feeds,kind))
                    filename=archive_name(day,formats,feeds,kind)
            except SelectionError as exc:
                raise tornado.web.HTTPError(400,reason=str(exc)) from None
            except FileNotFoundError as exc:
                raise tornado.web.HTTPError(404,reason=str(exc)) from None
            self.set_header('Content-Type','application/zip')
            self.set_header('Content-Disposition',f'attachment; filename="{filename}"')
            self.set_header('Cache-Control','no-store')
            self.set_header('X-Content-Type-Options','nosniff')
            self.set_header('X-Accel-Buffering','no')
            chunks=queue.Queue(maxsize=8)
            producer=_workers.submit(produce,plan,chunks,self.cancelled)
            while True:
                try:
                    item=await loop.run_in_executor(None,lambda:chunks.get(timeout=0.5))
                except queue.Empty:
                    if self.cancelled.is_set():
                        return
                    continue
                if item is _END:
                    break
                if isinstance(item,Exception):
                    logger.error('No se ha completado el ZIP solicitado: %s',item)
                    self.request.connection.close()
                    return
                self.write(item)
                await self.flush()
            self.finish()
        except tornado.iostream.StreamClosedError:
            pass
        finally:
            self.cancelled.set()
            if producer is None:
                _slots.release()
            else:
                producer.add_done_callback(lambda future:_slots.release())


def register_download_routes(app,cfg,base=''):
    import re
    prefix='/'+base.strip('/') if base.strip('/') else ''
    app.add_handlers(r'.*$',[(re.escape(prefix+ENDPOINT)+r'/?',DatasetDownloadHandler,{'cfg':cfg})])
