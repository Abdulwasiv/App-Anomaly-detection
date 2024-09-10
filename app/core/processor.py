import contextlib
import json
import logging
from datetime import datetime
from typing import Dict, Optional
import pandas as pd

from app.core.processor import ProcessAnomalyDetection

logger = logging.getLogger(__name__)

class AnomalyDetectionController:
    """Controller class for performing Anomaly Detection on streaming data."""

    def __init__(
        self,
        kpi_info: dict,
        save_model: bool = False,
        debug: bool = False,
        task_id: Optional[int] = None,
    ):
        """Initialize the controller.

        :param kpi_info: dictionary with information on the KPI
        :type kpi_info: dict
        :param save_model: whether to save the model or not, defaults to False
        :type save_model: bool, optional
        :param task_id: used to log checkpoints. Set to None to disable logging of checkpoints.
        :type task_id: int, optional
        """
        logger.info(f"Anomaly Controller initializing with KPI: {kpi_info['id']}")
        self.kpi_info = kpi_info
        self.save_model = save_model
        self._task_id = task_id
        self.deviation_from_mean_dict = {}
        logger.info(f"Anomaly controller initialized for KPI ID: {kpi_info['id']}")
        
    def detect(self, input_data: pd.DataFrame) -> None:
        """Perform the anomaly detection on streaming data.

        :param input_data: Dataframe with metric's data
        :type input_data: pd.DataFrame
        """
        kpi_id = self.kpi_info["id"]

        logger.info(f"Performing anomaly detection for KPI ID: {kpi_id}")

        model_name = self.kpi_info["anomaly_params"]["model_name"]
        logger.debug(f"Anomaly Model is {model_name}")

        logger.info(f"Loaded {len(input_data)} rows of input data for KPI {kpi_id}.")

        # Run anomaly detection for the overall series
        logger.info(f"Running anomaly for overall KPI {kpi_id}")
        self._run_anomaly_for_series(input_data, "overall")

        # Optional: Run anomaly detection for subdimensions or data quality if needed
        if self._to_run_subdim(self.kpi_info):
            logger.info(f"Running anomaly for subdims KPI {kpi_id}")
            self._detect_subdimensions(input_data)

        if self._to_run_data_quality(self.kpi_info):
            logger.info(f"Running anomaly for dq KPI {kpi_id}")
            self._detect_data_quality(input_data)
    
    def _run_anomaly_for_series(self, input_data: pd.DataFrame, series: str) -> None:
        """Helper method to run anomaly detection on a specific series."""
        last_date = datetime.now()  # Assume the last date is now for streaming data
        freq = self.kpi_info["anomaly_params"]["frequency"]

        # Perform anomaly detection
        anomaly_output = self._detect_anomaly(
            self.kpi_info["anomaly_params"]["model_name"],
            input_data,
            last_date,
            series,
            None,
            freq,
        )

        # Save the anomaly output
        self._save_anomaly_output(anomaly_output, series)
    
    def _detect_anomaly(
        self,
        model_name: str,
        input_data: pd.DataFrame,
        last_date: datetime,
        series: str,
        subgroup: Optional[str],
        freq: str,
    ) -> pd.DataFrame:
        """Detect anomalies in the given data."""
        input_data = input_data.reset_index().rename(
            columns={
                self.kpi_info["datetime_column"]: "dt",
                self.kpi_info["metric"]: "y",
            }
        )
        sensitivity = self.kpi_info["anomaly_params"].get("sensitivity", "medium")
        return ProcessAnomalyDetection(
            model_name,
            input_data,
            last_date,
            self.kpi_info["anomaly_params"]["anomaly_period"],
            self.kpi_info["table_name"],
            freq,
            sensitivity,
            self.slack,
            series,
            subgroup,
            self.deviation_from_mean_dict,
            self.kpi_info.get("model_kwargs", {}),
        ).predict()
        
    def _save_anomaly_output(
        self, anomaly_output: pd.DataFrame, series: str, subgroup: Optional[str] = None
    ) -> None:
        """Save anomaly output to the DB."""
        anomaly_output = anomaly_output.rename(
            columns={"dt": "data_datetime", "anomaly": "is_anomaly"}
        )
        anomaly_output["kpi_id"] = self.kpi_info["id"]
        anomaly_output["anomaly_type"] = series
        anomaly_output["series_type"] = json.dumps(subgroup) if subgroup else None
        anomaly_output["created_at"] = datetime.now()

        anomaly_output.to_sql(
            AnomalyDataOutput.__tablename__,
            db.engine,
            if_exists="append",
            chunksize=AnomalyDataOutput.__chunksize__,
        )

    @staticmethod
    def _to_run_subdim(kpi_info: dict):
        if len(kpi_info.get("dimensions", [])) == 0:
            return False
        run_optional = kpi_info.get("anomaly_params", {}).get("run_optional", None)
        return run_optional is None or run_optional["subdim"] is True

    @staticmethod
    def _to_run_data_quality(kpi_info: dict):
        run_optional = kpi_info.get("anomaly_params", {}).get("run_optional", None)
        return run_optional is None or run_optional["data_quality"] is True
