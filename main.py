import datetime
from numpy import insert, intp
from database import DataBase
import pandas as pd


def kontur_task_init_database(db: DataBase):
  db.exec_command('CREATE DATABASE kontur_db')
  db.exec_command('CREATE TABLE kontur_db.users (GUID String, FirstName String, MiddleName String, LastName String) ENGINE MergeTree PRIMARY KEY GUID')
  db.exec_command('CREATE TABLE kontur_db.history (GUID String, Timestamp DateTime, OuterIP String, NgToken String, OpenVPNServer String, InnerIP String) ENGINE MergeTree PRIMARY KEY GUID')

  df = pd.read_csv('logins_ansi.csv', sep=',', delimiter=None, warn_bad_lines=True, error_bad_lines=False)
  df['Timestamp'] = df['Timestamp'].apply(lambda x: datetime.datetime.fromisoformat(x))
  df.fillna('NULL', inplace=True)

  df_names = pd.read_json('user_info_beauty.json')
  df_names.fillna('NULL', inplace=True)

  chunk_size = 150
  counter = 0
  data_to_insert = []

  for index, row in df_names.iterrows():
    counter += 1
    data_to_insert.append([row['GUID'], row['FirstName'], row['MiddleName'], row['LastName']])
    if counter > chunk_size:
      db.insert('kontur_db.users', data_to_insert, ['GUID', 'FirstName', 'MiddleName', 'LastName'])
      data_to_insert.clear()
      counter = 0

  if len(data_to_insert) != 0:
    db.insert('kontur_db.users', data_to_insert, ['GUID', 'FirstName', 'MiddleName', 'LastName'])
    data_to_insert.clear()




  chunk_size = 20000
  counter = 0
  data_to_insert = []

  for index, row in df.iterrows():
    counter += 1
    data_to_insert.append([
                            row['GUID'], 
                            row['Timestamp'], 
                            row['OuterIP'], 
                            row['NgToken'], 
                            row['OpenVPNServer'], 
                            row['InnerIP']
                          ])
    if counter > chunk_size:
      db.insert('kontur_db.history', data_to_insert, ['GUID', 'Timestamp', 'OuterIP', 'NgToken', 'OpenVPNServer','InnerIP'])
      data_to_insert.clear()
      counter = 0

  if len(data_to_insert) != 0:
    db.insert('kontur_db.history', data_to_insert, ['GUID', 'Timestamp', 'OuterIP', 'NgToken', 'OpenVPNServer','InnerIP'])
    data_to_insert.clear()





db = DataBase('192.168.91.128', 'a')
kontur_task_init_database(db)



