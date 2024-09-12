from flask import Blueprint, jsonify
from app.views.kpi_views import Kpi_views

blueprint=Blueprint('anomaly-views',__name__)



@blueprint.route("/anomaly", methods=["POST", "GET"])
def kpi_anomaly_params(kpi_id: int):
    """Get or update anomaly params for a KPI.

    (This is where anomaly is setup/configured or updated).
    """
    kpi=Kpi_views()
    # kpi = cast(Optional[Kpi], Kpi.get_by_id(kpi_id))
    # logger.info(f"Info KPI {kpi}")
    # if kpi is None:
    #     return (
    #         jsonify(
    #             {
    #                 "error": f"Could not find KPI for ID: {kpi_id}",
    #                 "status": "failure",
    #             }
    #         ),
    #         400,
    #     )

    # # when method is GET we just return the anomaly params
    # if request.method == "GET":
    #     logger.info(f"anomaly_params {_get_anomaly_params_dict(kpi)}")
    #     return jsonify(
    #         {
    #             "anomaly_params": _get_anomaly_params_dict(kpi),
    #         }
    #     )
    

    # when it's POST, update anomaly params

    # check if this is first time anomaly setup or edit config
    is_first_time = kpi.anomaly_params is None

    if is_first_time:
        print("ADD")
        #logger.info(f"Adding anomaly parameters for KPI ID: {kpi_id}")
    else:
        print("NO")
      #  logger.info(f"Updating existing anomaly parameters for KPI ID: {kpi_id}")
    
    # if not request.is_json:
    #     return (
    #         jsonify(
    #             {
    #                 "error": "Request body must be a JSON "
    #                 + "(and Content-Type header must be set correctly)",
    #                 "status": "failure",
    #             }
    #         ),
    #         400,
    #     )

    # req_data: dict = cast(dict, request.get_json())

    # if "anomaly_params" not in req_data:
    #     return (
    #         jsonify(
    #             {
    #                 "error": "The request JSON needs to have anomaly_params as a field",
    #                 "status": "failure",
    #             }
    #         ),
    #         400,
    #     )

    # err, new_anomaly_params = validate_partial_anomaly_params(
    #     req_data["anomaly_params"]
    # )
    # logger.info(f" NEW PARAMS{new_anomaly_params}")
    # if err != "":
    #     return jsonify({"error": err, "status": "failure"}), 400

    # err, new_kpi = update_anomaly_params(
    #     kpi, new_anomaly_params, check_editable=not is_first_time
    # )
    
   # run_anomaly = False
   
    # if not is_first_time:
    #     if (
    #         "scheduler_params_time" not in new_anomaly_params
    #         and len(new_anomaly_params) > 0
    #     ):
    #         run_anomaly = True
    #     elif (
    #         "scheduler_params_time" in new_anomaly_params
    #         and len(new_anomaly_params) > 1
    #     ):
    #         run_anomaly = True
    #     else:
    #         run_anomaly = False

    # if run_anomaly and err == "":
    #     logger.info(
    #         "Deleting anomaly data and re-running anomaly since anomaly params was "
    #         + f"edited for KPI ID: {new_kpi.id}"
    #     )
    #     delete_anomaly_output_for_kpi(new_kpi.id)
    #     from chaos_genius.jobs.anomaly_tasks import ready_anomaly_task

    #     anomaly_task = ready_anomaly_task(new_kpi.id)
    #     logger.info(f"anomaly_params {anomaly_task}")

    #     if anomaly_task is not None:
    #         anomaly_task.apply_async()
    #         logger.info(f"Anomaly started for KPI ID: {new_kpi.id}")
    #     else:
    #         logger.info(
    #             f"Anomaly failed since KPI was not found for KPI ID: {new_kpi.id}"
    #         )

    # if err != "":
    #     return jsonify({"error": err, "status": "failure"}), 400

    # we ensure anomaly task is run as soon as analytics is configured
    # we also run RCA at the same time
    if is_first_time:
        # TODO: move this import to top and fix import issue
        from app.jobs.anomaly_tasks import ready_anomaly_task
          
        anomaly_task = ready_anomaly_task(kpi.id)
        
        if anomaly_task is None :
             print(" Not task is here ")   
            # logger.info(
            #     "Could not run anomaly task since newly configured KPI was not found: "
            #     f"{new_kpi.id}"
            # )
        else:
            anomaly_task.apply_async()
    
    return jsonify({"msg": "Successfully updated Anomaly params", "status": "success"})
