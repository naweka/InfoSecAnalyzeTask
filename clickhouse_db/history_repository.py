from typing import NoReturn
import pandas
from clickhouse_db.clickhouse_client import ClickHouseClient


class HistoryRepository(ClickHouseClient):
    def insert_dataframe(self, df: pandas.DataFrame) -> NoReturn:
        super().insert_dataframe(
            'kontur_db.history',
            df,
            100000,
            ['GUID',
             'Timestamp',
             'OuterIP',
             'NgToken',
             'OpenVPNServer',
             'InnerIP']
        )
