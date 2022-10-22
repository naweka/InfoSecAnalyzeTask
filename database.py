import clickhouse_connect
import pandas

class DataBase():
	def __init__(self, host: str, password: str, username: str = 'default') -> None:
		client = clickhouse_connect.get_client(host=host, username=username, password=password)
		if not client:
			raise Exception('Error while connecting')

		self.client = client


	def exec_command(self, command: str):
		if not self.client:
			raise Exception('No connection to database')
		self.client.command(command)


	def exec_query(self, command: str):
		if not self.client:
			raise Exception('No connection to database')
		return self.client.query(command)


	def insert(self, table: str, data: list, column_names: list):
		if not self.client:
			raise Exception('No connection to database')
		self.client.insert(table, data, column_names=column_names) 