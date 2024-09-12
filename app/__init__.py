from flask import Flask
from app.views.dataconsume_views import ingestion_blueprint
from app.job.anomaly_task import ready_anomaly_task
from app.views.anomaly_views import blueprint

def create_app():
    app = Flask(__name__)
    app.register_blueprint(ingestion_blueprint, url_prefix='/api/data_consume')
    app.register_blueprint(ingestion_blueprint, url_prefix='/api/kpi_views')
    app.register_blueprint(blueprint, url_prefix='/api/anomaly_views')
    app.register_blueprint(ingestion_blueprint, url_prefix='/api/alert')

    # app.register_blueprint(ingestion_blueprint, url_prefix='/api/v2')

    return app
