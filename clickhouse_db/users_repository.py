from typing import NoReturn
import pandas
from clickhouse_db.clickhouse_client import ClickHouseClient


class UsersRepository(ClickHouseClient):
    def __init__(self):
        super().__init__()
        self.column_names = ['GUID',
                             'FirstName',
                             'MiddleName',
                             'LastName']

    def insert_dataframe(self, df: pandas.DataFrame) -> NoReturn:
        super().insert_dataframe(
            'kontur_db.users',
            df,
            100000,
            self.column_names
        )
