

import contextlib
import json
import logging
from datetime import date, datetime, timedelta
from typing import Dict, List, Optional, Union
import pandas as pd
import logging

from app.core.utils.data_loader import DataLoader



logger=logging.getLogger(__name__)


from app.core.processor import ProcessAnomalyDetection


class AnomalyDetectionController(object):
    """Controller class for performing Anomaly Detection."""

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

       # self.debug = self.kpi_info["anomaly_params"].get("debug", False)
        #self.debug = True if self.debug == "True" else False

        self._task_id = task_id
        self.deviation_from_mean_dict = {}

        # Determine if data is pre-aggregated
      #  conn_type = DataSource.get_by_id(kpi_info["data_source"]).as_dict["connection_type"]
     #   self._preaggregated = conn_type == "Druid"
     #   self._preaggregated_count_col = self.kpi_info["count_column"]

        # if self._preaggregated and self.kpi_info["metric"] == self._preaggregated_count_col:
        #     logger.info(
        #         "Preaggregated count column same as metric column. "
        #         "`self._preaggregated_count_column` will be updated after data loading."
        #     )

        logger.info(f"Anomaly controller initialized for KPI ID: {kpi_info['id']}")
        
    def _load_anomaly_data(self) -> pd.DataFrame:
        """Load KPI data, preprocess it and return it for anomaly detection.

        :return: Dataframe with all of KPI's data for the last
        N days/hours
        :rtype: pd.DataFrame
        """
        last_date = self._get_last_date_in_db("overall")
        period = self.kpi_info["anomaly_params"]["anomaly_period"]

        # Convert period back to days for hourly data
        if self.kpi_info["anomaly_params"]["frequency"] == "H":
            period /= 24

        if not last_date:
            dl = DataLoader(
                self.kpi_info,
                end_date=self.end_date,
                days_before=period,
            )
        else:
            start_date = last_date - timedelta(days=period)
            dl = DataLoader(
                self.kpi_info,
                end_date=self.end_date,
                stAnomalyModelart_date=start_date,
            )

        df = dl.get_data()
        logger.info(f"FIRST    SS   {df} ")
        

        # get new name of preaggregated count column if its the same as metric column
        if (
            self._preaggregated
            and self.kpi_info["metric"] == self._preaggregated_count_col
        ):
            self._preaggregated_count_col = dl.pre_aggregated_count_column

        return df
    
    def _detect_anomaly(
        self,
        model_name: str,
        input_data: pd.DataFrame,
        last_date: datetime,
        series: str,
        subgroup: str,
        freq: str,
    ) -> pd.DataFrame:
        """Detect anomalies in the given data.

        :param model_name: name of the model used for anomaly detection
        :type model_name: str
        :param input_data: Dataframe with metric's data
        :type input_data: pd.DataFrame
        :param last_date: Last date for which we have output data
        :type last_date: datetime
        :param series: series name (overall, subdim or dq)
        :type series: str
        :param subgroup: subgroup identifier
        :type subgroup: str
        :param freq: frequency of data
        :type freq: str
        :return: Dataframe with anomaly data
        :rtype: pd.DataFrame
        """
        input_data = input_data.reset_index().rename(
            columns={
                self.kpi_info["datetime_column"]: "dt",
                self.kpi_info["metric"]: "y",
            }
        )
        logger.info(f"INPUT     {input_data}")
        sensitivity = self.kpi_info["anomaly_params"].get("sensitivity", "medium")
        logger.info("last date %s",last_date)    
        logger.info(" input_data %s",input_data)    
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
        self, anomaly_output: pd.DataFrame, series: str, subgroup: dict = None
    ) -> None:
        """Save anomaly output to the DB.

        :param anomaly_output: Dataframe with anomaly data
        :type anomaly_output: pd.DataFrame
        :param series: Type of series
        :type series: str
        :param subgroup: Subgroup of the KPI
        :type subgroup: dict
        """
        if self.debug:
            print("SAVING", series, subgroup, len(anomaly_output))

        anomaly_output = anomaly_output.rename(
            columns={"dt": "data_datetime", "anomaly": "is_anomaly"}
        )
        anomaly_output["kpi_id"] = self.kpi_info["id"]
        anomaly_output["anomaly_type"] = series

        if subgroup is not None:
            subgroup = json.dumps(subgroup)
        anomaly_output["series_type"] = subgroup

        anomaly_output["created_at"] = datetime.now()

        anomaly_output.to_sql(
            AnomalyDataOutput.__tablename__,
            db.engine,
            if_exists="append",
            chunksize=AnomalyDataOutput.__chunksize__,
        )



    @staticmethod
    def _to_run_overall(kpi_info: dict):
        run_optional = kpi_info.get("anomaly_params", {}).get("run_optional", None)
        return run_optional is None or run_optional["overall"] is True

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

    def detect(self) -> None:
        """Perform the anomaly detection for given KPI."""
        kpi_id = self.kpi_info["id"]

        logger.info(f"Performing anomaly detection for KPI ID: {kpi_id}")

        model_name = self.kpi_info["anomaly_params"]["model_name"]
        logger.debug(f"Anomaly Model is {model_name}")

        logger.info(f"Loading Input Data for KPI {kpi_id}")
        try:
            input_data = self._load_anomaly_data()
        except Exception as e:  # noqa B902
            self._checkpoint_failure("Data Loader", e)
            raise e
        else:
            self._checkpoint_success("Data Loader")

        if self.kpi_info["scheduler_params"]["scheduler_frequency"] == "H":
            logger.info(f"Creating Hourly Input Dataframe for KPI {kpi_id}")
            input_data = self._create_hourly_input_data(input_data)
            logger.info(
                f"Last Data Point for Hourly Input Dataframe for KPI {self.end_date}"
            )

        logger.info(f"Loaded {len(input_data)} rows of input data.")

        if self._to_run_overall(self.kpi_info):
            logger.info(f"Running anomaly for overall KPI {kpi_id}")
            self._run_anomaly_for_series(input_data, "overall")

        if self._to_run_subdim(self.kpi_info):
            logger.info(f"Running anomaly for subdims KPI {kpi_id}")
            self._detect_subdimensions(input_data)

        if self._to_run_data_quality(self.kpi_info):
            logger.info(f"Running anomaly for dq KPI {kpi_id}")
            self._detect_data_quality(input_data)

    @staticmethod
    def total_tasks(kpi: Kpi):
        """Return the total number of sub-tasks for given KPI.

        Args:
            kpi (Kpi): Kpi object to get no. of sub-tasks for.
        """
        kpi_info = kpi.as_dict

        # start, end, alert trigger, data loader
        num = 4
        if AnomalyDetectionController._to_run_overall(kpi_info):
            num += 3
        if AnomalyDetectionController._to_run_subdim(kpi_info):
            num += 2
        if AnomalyDetectionController._to_run_data_quality(kpi_info):
            num += 2

        return num
