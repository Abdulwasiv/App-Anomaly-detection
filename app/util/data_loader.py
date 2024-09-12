import contextlib
import logging
from datetime import datetime
from typing import Dict, Optional
import logging 
import pandas as pd

logger=logging()

class DataLoader:
    """Data Loader Class for streaming data."""

    def __init__(
        self,
        kpi_info: dict,
        tail: Optional[int] = None,
        validation: bool = False,
    ):
        """Initialize Data Loader for KPI.

        :param kpi_info: KPI info to load data for
        :type kpi_info: dict
        :param tail: limit data loaded to this number of rows, defaults to None
        :type tail: int, optional
        :param validation: if validation is True, we do not perform preprocessing
        :type validation: bool, optional
        """
        self.kpi_info = kpi_info
        self.tail = tail
        self.validation = validation

        self.dt_col = self.kpi_info["datetime_column"]

       # self.connection_info = DataSource.get_by_id(kpi_info["data_source"]).as_dict
      #  self.db_connection = get_sqla_db_conn(data_source_info=self.connection_info)
      #  self.identifier = self.db_connection.sql_identifier

    # def _get_id_string(self, value):
    #     value = self.db_connection.resolve_identifier(value)
    #     return f"{self.identifier}{value}{self.identifier}"

    def _load_data(self, data: Dict):
        try:
            # Query ClickHouse for data where account_id and metric match
            query = f"""
            SELECT * FROM your_table
            WHERE account_id = '{account_id}' AND metric = '{metric}'
            """
            result = self.client.execute(query)
            return result
        except Exception as e:
            print(f"Failed to query ClickHouse: {e}")
            return None


    def _convert_date_to_string(self, date: datetime) -> str:
        """Convert date to string based on the connection's date format.

        :param date: date to convert
        :type date: datetime
        :return: formatted date string
        :rtype: str
        """
        date_str = date.strftime(self.db_connection.sql_date_format)
        if not self.kpi_info.get("timezone_aware"):
            date_str = (
                pd.Timestamp(
                    datetime.strptime(date_str, self.db_connection.sql_strptime_format)
                )
                .tz_convert(self.connection_info["database_timezone"])
                .tz_localize(None)
                .strftime(self.db_connection.sql_strftime_format)
            )
        return date_str

    # def _get_tz_from_offset_str(self, utc_offset_str="GMT+00:00"):
    #     """Get timezone from UTC offset string.

    #     :param utc_offset_str: UTC offset string
    #     :type utc_offset_str: str
    #     :return: timezone
    #     :rtype: pytz.timezone
    #     """
    #     sign = -1 if utc_offset_str[-6] == "-" else 1
    #     utc_offset_mins = int(utc_offset_str[-2:]) * sign
    #     utc_offset_hrs = int(utc_offset_str[-5:-3]) * sign

    #     utc_offset = timedelta(hours=utc_offset_hrs, minutes=utc_offset_mins)

    #     timezones = pytz.all_timezones
    #     for tz_name in timezones:
    #         with contextlib.suppress(AttributeError):
    #             tz = pytz.timezone(tz_name)
    #             tz_offset = tz._transition_info[-1][0]
    #             if utc_offset == tz_offset:
    #                 return tz
    #     raise ValueError(f"No timezone found for offset {utc_offset_str}")