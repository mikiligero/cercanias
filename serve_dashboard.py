"""Streamlit 1.41.1 con descargas ZIP por streaming en el mismo puerto."""
import os
from pathlib import Path
import sys


def enable_downloads():
    # La extensión está aislada aquí: no se modifica la instalación de Streamlit.
    # Al actualizar la versión fijada, comprobar la creación de la aplicación
    # y las rutas con las pruebas HTTP antes de cambiar el servicio.
    from streamlit.web.server.server import Server
    from streamlit import config
    from src.config import load
    from src.download_server import register_download_routes
    original=Server._create_app

    def create_app(self):
        app=original(self)
        register_download_routes(app,load(),config.get_option('server.baseUrlPath'))
        return app

    Server._create_app=create_app
    os.environ['ANTICIPA_DOWNLOADS_ENABLED']='1'


if __name__=='__main__':
    enable_downloads()
    from streamlit.web import cli
    sys.argv=['streamlit','run',str(Path(__file__).with_name('dashboard.py')),*sys.argv[1:]]
    cli.main()
