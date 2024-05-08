from Connection_Class import Connection

conn = Connection(
    host='127.0.0.1',
    user='root',
    password='ccczesiulek123',
    database='SpendingsDB')


print(conn.fetch_columns())