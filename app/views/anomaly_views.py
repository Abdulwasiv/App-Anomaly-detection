from flask import Blueprint, jsonify
from app.views.kpi_views import Kpi_views

blueprint=Blueprint('anomaly',__name__)



@blueprint.route("/anomaly", methods=["POST", "GET"])
def kpi_anomaly_params(kpi_id: int):
    """Get or update anomaly params for a KPI.

    (This is where anomaly is setup/configured or updated).
    """
    kpi=Kpi_views()
    print(kpi)
    is_first_time = kpi.id is None

    if is_first_time:
        print("ADD")
        #logger.info(f"Adding anomaly parameters for KPI ID: {kpi_id}")
    else:
        print("NO")
        
    if is_first_time:
        from app.job.anomaly_task import ready_anomaly_task  
        anomaly_task = ready_anomaly_task(kpi.id)
        
        if anomaly_task is None :
             print(" Not task is here ")   
        else:
            anomaly_task.apply_async()
    
    return jsonify({"msg": "Successfully updated Anomaly params", "status": "success"})
