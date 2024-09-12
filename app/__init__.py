from flask import Flask
from app.views.dataconsume_views import ingestion_blueprint
# from app.views.kpi_views import ingestion_blueprint
import tornado.ioloop
import threading

def create_app():
    app = Flask(__name__)
    app.register_blueprint(ingestion_blueprint, url_prefix='/api/v1')
    # app.register_blueprint(ingestion_blueprint, url_prefix='/api/v2')

    return app
