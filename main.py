import datetime
from typing import NoReturn
from clickhouse_db.clickhouse_client import ClickHouseClient
from clickhouse_db.history_repository import HistoryRepository
from clickhouse_db.users_repository import UsersRepository
import pandas as pd


def init_database_structure(db: ClickHouseClient) -> NoReturn:
    db.exec_command('CREATE DATABASE kontur_db')
    db.exec_command(
        'CREATE TABLE kontur_db.users (GUID String, FirstName String, MiddleName String, LastName String) ENGINE MergeTree PRIMARY KEY GUID')
    db.exec_command(
        'CREATE TABLE kontur_db.history (GUID String, Timestamp DateTime, OuterIP String, NgToken String, OpenVPNServer String, InnerIP String) ENGINE MergeTree PRIMARY KEY GUID')


def fill_database(db: ClickHouseClient) -> NoReturn:
    """Метод для создания базы данных и внесения начальных данных."""

    # Читаем содержимое файлов, помечаем пустые
    # значения, а также парсим дату/время
    df_history = pd.read_csv('logins_ansi.csv', sep=',', delimiter=None)
    df_history['Timestamp'] = df_history['Timestamp'].apply(
        lambda x: datetime.datetime.fromisoformat(x))
    df_history.fillna('NULL', inplace=True)

    df_users = pd.read_json('user_info_beauty.json')
    df_users.fillna('NULL', inplace=True)

    HistoryRepository().insert_dataframe(df_history)
    UsersRepository().insert_dataframe(df_users)


# Подключаемся к бд, инициализируем значения
db = ClickHouseClient()
init_database_structure(db)
fill_database(db)
