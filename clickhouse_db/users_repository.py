from typing import NoReturn
import pandas
from clickhouse_db.clickhouse_client import ClickHouseClient


class UsersRepository(ClickHouseClient):
    def insert_dataframe(self, df: pandas.DataFrame) -> NoReturn:
        super().insert_dataframe(
            'kontur_db.users',
            df,
            100000,
            ['GUID',
             'FirstName',
             'MiddleName',
             'LastName']
        )
