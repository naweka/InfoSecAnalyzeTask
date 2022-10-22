from clickhouse_driver import connect
from database import DataBase

db = DataBase('192.168.91.128', 'a')

print(db.exec_query('select version()').result_set)