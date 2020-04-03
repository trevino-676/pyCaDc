from pymysqlreplication import BinLogStreamReader

mysql_settings = {"host": "127.0.0.1", "port": 3308, "user": "root", "passwd": "cain123"}

stream = BinLogStreamReader(connection_settings=mysql_settings, server_id=1)


for binlog_event in stream:
    binlog_event.dump()

stream.close()
