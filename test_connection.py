import clickhouse_connect
client = clickhouse_connect.get_client(host='192.168.91.128', username='default', password='a')
print(client.query('SELECT version()').result_set)