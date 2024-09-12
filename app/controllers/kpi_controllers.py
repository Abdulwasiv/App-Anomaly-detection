"""Logic and helpers for interaction with KPIs."""
import logging
from datetime import date, datetime, timedelta
from typing import Iterator, List, Optional

from sqlalchemy import delete

from chaos_genius.controllers.task_monitor import checkpoint_failure, checkpoint_success
from chaos_genius.core.anomaly.controller import AnomalyDetectionController
from chaos_genius.core.rca.constants import TIME_RANGES_BY_KEY
from chaos_genius.core.utils.data_loader import DataLoader
from chaos_genius.databases.models.anomaly_data_model import AnomalyDataOutput
from chaos_genius.databases.models.kpi_model import Kpi
from chaos_genius.extensions import db
from chaos_genius.settings import (
    DAYS_OFFSET_FOR_ANALTYICS,
    MAX_SUMMARY_DEEPDRILLS_SLACK_DAYS,
)

logger = logging.getLogger(__name__)


def _is_data_present_for_end_date(
    kpi_info: dict, end_date: Optional[date] = None
) -> bool:
    if end_date is None:
        end_date = datetime.now().date()
    else:
        end_date = datetime.now().date()
       
    df_count = DataLoader(kpi_info, end_date=end_date, days_before=0).get_count()
    logger.info("MAIN DATA  %s  ",df_count)
    return df_count != 0


def get_kpi_data_from_id(n: int) -> dict:
    """Returns the corresponding KPI data for the given KPI ID from KPI_DATA.

    :param n: ID of KPI
    :type n: int

    :raises: ValueError

    :returns: KPI data
    :rtype: dict
    """
    # TODO: Move to utils module

    kpi_info = Kpi.get_by_id(n)
    logger.info("kpi_info %s", kpi_info)    
    if kpi_info and kpi_info.as_dict:
        return kpi_info.as_dict
    raise ValueError(f"KPI ID {n} not found in KPI_DATA")

def run_anomaly_for_kpi(
    kpi_id: int, end_date: Optional[date] = None, task_id: Optional[int] = None
):
    """Runs anomaly detection for given kpi_id.

    Blocking function (it does NOT spawn a celery task).
    """
    logger.info(f"Starting Anomaly Detection for KPI ID: {kpi_id}.")
    kpi_info = get_kpi_data_from_id(kpi_id)
    logger.info(f"(KPI ID: {kpi_id}) Retrieved KPI information.")

    logger.info("(KPI ID: {kpi_id}) Selecting end date.")
    
    logger.info(kpi_info["scheduler_params"]["scheduler_frequency"])
    
    if end_date is None:
        scheduler_frequency = "M"

        if scheduler_frequency == "D":
            # by default we always calculate for n-days_offset_for_analytics
            true_end_date = datetime.now().date() - timedelta(
                days=(DAYS_OFFSET_FOR_ANALTYICS)
            )
            # Check if data is available or not then try for
            # n-days_offset_for_analytics-1
            if not _is_data_present_for_end_date(kpi_info, true_end_date):
                true_end_date = true_end_date - timedelta(days=1)
                logger.info("(KPI ID: {kpi_id}) Decreasing end date by 1.")

        elif scheduler_frequency == "H":
            true_end_date = datetime.now().replace(second=0, microsecond=0)
            logger.info(f"(KPI ID: {kpi_id}) Hourly end date is {true_end_date}.")

        elif scheduler_frequency == "M":
            true_end_date = datetime.now().replace(second=0, microsecond=0)
            logger.info(f"(KPI ID: {kpi_id}) Minute-level end date is {true_end_date}.")

        else:
            raise ValueError(
                f"KPI ID {kpi_id} has invalid scheduler frequency: "
                f"{scheduler_frequency}"
            )
    else:
        true_end_date = end_date

    logger.info(f"(KPI ID: {kpi_id}) End date is {true_end_date}.")
    
    adc = AnomalyDetectionController(kpi_info, true_end_date, task_id=task_id)
    logger.info("START %s ", adc)
    adc.detect()
    logger.info(f"Anomaly Detection has completed for KPI ID: {kpi_id}.")

