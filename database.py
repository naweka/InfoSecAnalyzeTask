import clickhouse_connect
import pandas
from clickhouse_connect.driver.query import QueryResult


class DataBase:
    """
    Класс для взаимодействия с бд ClickHouse
    """
    def __init__(self, host: str, username: str = 'default', password: str = 'default') -> None:
        self.client = clickhouse_connect.get_client(host=host, username=username, password=password)

    def check_if_client_is_connected(self) -> None:
        """
        Метод проверяет, было ли подключение инициализировано, и, если нет,
        то бросит исключение
        """
        if not self.client:
            raise Exception('No connection to database')

    def exec_command(self, command: str) -> None:
        """
        Метод для выполнения команд без возвращения результата
        """
        self.check_if_client_is_connected()
        self.client.command(command)

    def exec_query(self, command: str) -> QueryResult:
        """
        Метод для выполнения команд с возвращением результата
        """
        self.check_if_client_is_connected()
        return self.client.query(command)

    def insert(self, table: str, data: list, column_names: list) -> None:
        """
        Метод для вставки значений в таблицу
        :param table: имя таблицы, в которую необходимо вставить значения
        :param data: лист листов, в которых содержатся данные для вставки
        :param column_names: имена столбцов, в которые в бд будут помещены данные
        """
        self.check_if_client_is_connected()
        self.client.insert(table, data, column_names=column_names)

    def insert_dataframe(self, table: str, dataframe: pandas.DataFrame, chunk_size: int, column_names: list) -> None:
        """
        Метод для переноса значений в бд из DataFrame
        :param table: имя таблицы, в которую необходимо вставить значения
        :param dataframe: DataFrame, откуда будут браться значения
        :param chunk_size: число элементов, вставляемых за один запрос
        :param column_names: имена столбцов, в которые в бд будут помещены данные из DataFrame
        """
        self.check_if_client_is_connected()
        counter = 0
        data_to_insert = []

        for index, row in dataframe.iterrows():
            counter += 1
            data_to_insert.append([row[x] for x in column_names])
            if counter > chunk_size:
                self.client.insert(table, data_to_insert, column_names)
                data_to_insert.clear()
                counter = 0

        if len(data_to_insert) != 0:
            self.insert(table, data_to_insert, column_names=column_names)
            data_to_insert.clear()

