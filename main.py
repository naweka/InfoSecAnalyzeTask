import datetime
from numpy import insert, intp
from database import DataBase
import pandas as pd


def kontur_task_init_database(db: DataBase) -> None:
    """
    Метод для создания базы данных и внесения начальных данных
    """
    # создаем бд и таблицы
    db.exec_command('CREATE DATABASE kontur_db')
    db.exec_command(
        'CREATE TABLE kontur_db.users (GUID String, FirstName String, MiddleName String, LastName String) ENGINE MergeTree PRIMARY KEY GUID')
    db.exec_command(
        'CREATE TABLE kontur_db.history (GUID String, Timestamp DateTime, OuterIP String, NgToken String, OpenVPNServer String, InnerIP String) ENGINE MergeTree PRIMARY KEY GUID')

    # Читаем содержимое файлов, помечаем пустые
    # значения, а также парсим дату/время
    df = pd.read_csv('logins_ansi.csv', sep=',', delimiter=None)
    df['Timestamp'] = df['Timestamp'].apply(lambda x: datetime.datetime.fromisoformat(x))
    df.fillna('NULL', inplace=True)

    df_names = pd.read_json('user_info_beauty.json')
    df_names.fillna('NULL', inplace=True)

    # Вставляем значения
    db.insert_dataframe('kontur_db.users', df_names, 100000, ['GUID',
                                                              'FirstName',
                                                              'MiddleName',
                                                              'LastName'])

    db.insert_dataframe('kontur_db.history', df, 100000, ['GUID',
                                                          'Timestamp',
                                                          'OuterIP',
                                                          'NgToken',
                                                          'OpenVPNServer',
                                                          'InnerIP'])


# Подключаемся к бд, инициализируем значения
db = DataBase('192.168.91.128', password='a')
kontur_task_init_database(db)
