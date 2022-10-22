import clickhouse_connect

class DataBase():
	def __init__(self, host: str, password: str, username: str = 'default') -> None:
		client = clickhouse_connect.get_client(host=host, username=username, password=password)
		if not client:
			raise Exception('Error while initalizing!')

		self.client = client


	def exec_command(self, command):
		if not self.client:
			raise Exception('No connection to database')
		self.client.command(command)


	def exec_query(self, command):
		if not self.client:
			raise Exception('No connection to database')
		return self.client.query(command)