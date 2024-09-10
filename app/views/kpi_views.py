from flask import Blueprint, jsonify
import tornado.ioloop
import asyncio

ingestion_blueprint=Blueprint('kpi',__name__)

STATIC_JSON = {
    "account_id": 20242121,
    "name": "Sample Metric",
    "is_certified": True,
    "data_source": "DataSource1",
    "table_name": "metrics_table",
    "metric": "cpu_usage",
    "count_column": "usage_count",
    "dimensions": ["region", "host"],
    "timezone_aware": True,
    "run_anomaly": True,
    "anomaly_params": {
        "threshold": 0.8,
        "window_size": 10
    },
    "anomaly_frequency": "hourly",
    "is_static": False,
    "static_params": {},
    "active": True,
    "created_at": "2024-09-03T10:46:34Z"
}

@ingestion_blueprint.route('/kpi', methods=['POST'])
def Kpi_views():
        return jsonify(STATIC_JSON),200
def account_id():
        return STATIC_JSON["account_id"]
        
        