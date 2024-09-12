from flask import Blueprint, jsonify


ingestion_blueprint=Blueprint('kpi',__name__)

STATIC_JSON = {
    "account_id": 20242121,
    "name": "Sample Metric",
    "is_certified": True,
    "metric": "cpu_usage",
    "dimensions": ["region", "host"],
    "timezone_aware": True,
    "run_anomaly": True,
    "anomaly_params": {
        "threshold": 0.8,
        "window_size": 10
    },
    'sensitivity': 'low',   
    "anomaly_sessionality": "hourly",
    "active": True,
    "created_at": "2024-09-03T10:46:34Z"
}

@ingestion_blueprint.route('/kpi', methods=['POST'])
def Kpi_views():
        return jsonify(STATIC_JSON),200
def account_id():
        return STATIC_JSON["account_id"]
        
        