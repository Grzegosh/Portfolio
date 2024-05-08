from Connection_Class import Connection

conn = Connection(
            host='127.0.0.1',
            user='root',
            password='ccczesiulek123',
            database='SpendingsDB')

data_wychodzące = conn.wychodzące()
data_przychodzące = conn.fetch_data('Przychody')
print(data_przychodzące)


