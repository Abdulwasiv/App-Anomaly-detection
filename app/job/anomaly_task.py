# from datetime import datetime, timedelta
# from typing import Optional, cast

# from celery import group
# from celery.app.base import Celery
# from celery.utils.log import get_task_logger


# # from chaos_genius.alerts import trigger_anomaly_alerts_for_kpi
# # from chaos_genius.controllers.task_monitor import (
# #     checkpoint_failure,
# #     checkpoint_initial,
# #     checkpoint_success,
# # )

# # from chaos_genius.controllers.kpi_controller import get_anomaly_kpis, get_active_kpis
# #from chaos_genius.databases.models.kpi_model import Kpi

# from app.views.kpi_views import Kpi_views 

# from chaos_genius.extensions import celery as celery_ext

# celery = cast(Celery, celery_ext.celery)
# logger = get_task_logger(__name__)



# @celery.task
# def anomaly_single_kpi(kpi_id, end_date=None):
#     """Run anomaly detection for the given KPI ID.
    
#     Must be run as a celery task.
#     """
#     # TODO: fix circular import
#     from app.controllers.kpi_controllers import run_anomaly_for_kpi
  
#     try:
#         run_anomaly_for_kpi(kpi_id, end_date)
        
#         kpi = cast(kpi, Kpi.get_by_id(kpi_id))
    

#     except Exception as e:
#         kpi = cast(Kpi, Kpi.get_by_id(kpi_id))
#        # kpi.scheduler_params = update_scheduler_params("anomaly_status", "failed")
#      #   _checkpoint_failure("Anomaly complete", e)

#     #flag_modified(kpi, "scheduler_params")
#     kpi.update(commit=True)




# @celery.task
# def anomaly_kpi():
#     kpis = get_anomaly_kpis()
#     task_group = []
#     for kpi in kpis:
#         print(f"Starting anomaly task for KPI: {kpi.id}")
#         task_group.append(anomaly_single_kpi.s(kpi.id))
#     g = group(task_group)
#     res = g.apply_async()
#     return res


# def ready_anomaly_task(kpi_id: int):
#     """Set anomaly in-progress and update last_scheduled_time for the KPI.

#     Returns a Celery task that *must* be executed (using .apply_async) soon.
#     Returns None if the KPI does not exist.
#     """
#     # get scheduler_params
#     kpi = Kpi.get_by_id(kpi_id)
#     if kpi is None:
#         return None

#     # update scheduler params
#     kpi.scheduler_params = update_scheduler_params(
#         "last_scheduled_time_anomaly", datetime.now().isoformat()
#     )
#     # write back scheduler_params
#     # flag_modified(kpi, "scheduler_params")
#     kpi = kpi.update(commit=True)

#     kpi.scheduler_params = update_scheduler_params("anomaly_status", "in-progress")
#     # write back scheduler_params
#     #flag_modified(kpi, "scheduler_params")
#     kpi = kpi.update(commit=True)

#     return anomaly_single_kpi.s(kpi_id)




