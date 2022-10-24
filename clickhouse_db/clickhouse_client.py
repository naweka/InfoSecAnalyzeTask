from typing import NoReturn, Any, Union

import clickhouse_connect
import pandas
from clickhouse_connect.driver.query import QueryResult

from constants import DATABASE_ADDRESS, DATABASE_USER, DATABASE_PASSWORD


class ClickHouseClient:
    """Класс для взаимодействия с бд ClickHouse."""

    # def __init__(self, host: str, username: str = 'default', password: str = 'default') -> None:
    #    self.client = clickhouse_connect.get_client(host=host, username=username, password=password)

    def __init__(self):
        self.client = clickhouse_connect.get_client(
            host= DATABASE_ADDRESS,
            username=DATABASE_USER,
            password=DATABASE_PASSWORD
        )
        if not self.client:
            raise ConnectionError('No connection to database')

    def exec_command(self, command: str) -> Union[str, int, list[str]]:
        """Метод для выполнения команд без возвращения результата."""
        return self.client.command(command)

    def exec_query(self, command: str) -> QueryResult:
        """Метод для выполнения команд с возвращением результата."""
        return self.client.query(command)

    def insert(
            self,
            table: str,
            data: list[list[Any]],
            column_names: list[str]
    ) -> None:
        """Метод для вставки значений в таблицу.

        * table: имя таблицы, в которую необходимо вставить значения.
        * data: лист листов, в которых содержатся данные для вставки.
        * column_names: имена столбцов, в которые в бд будут помещены данные.
        """
        self.client.insert(table, data, column_names=column_names)

    def insert_dataframe(
            self,
            table: str,
            dataframe: pandas.DataFrame,
            chunk_size: int,
            column_names: list[str]
    ) -> NoReturn:
        """Метод для переноса значений в бд из DataFrame.

        * table: имя таблицы, в которую необходимо вставить значения.
        * dataframe: DataFrame, откуда будут браться значения.
        * chunk_size: число элементов, вставляемых за один запрос.
        * column_names: имена столбцов, в которые в бд будут помещены данные из DataFrame.
        """
        counter = 0
        data_to_insert = []

        # Проходимся по всем строкам df, складываем в "буфер", а
        # потом отправляем данные на вставку
        for index, row in dataframe.iterrows():
            counter += 1
            data_to_insert.append([row[x] for x in column_names])
            if counter > chunk_size:
                self.client.insert(table, data_to_insert, column_names)
                data_to_insert.clear()
                counter = 0

        # Если буфер не пуст, то его содержимое тоже надо вставить
        if len(data_to_insert) != 0:
            self.insert(table, data_to_insert, column_names=column_names)