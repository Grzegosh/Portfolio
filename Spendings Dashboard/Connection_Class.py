import pymysql
pymysql.install_as_MySQLdb()
import pandas as pd
import MySQLdb
from tkinter import messagebox


class Connection:
    
    def __init__(self, host, user, password, database,table=None):
        self.host = host
        self.user = user
        self.password = password
        self.database = database
        self.table = table
        self.mozliwosci_tabel = ['Przychody','Wypływy','Wypływy_Rodzinne']

    def connect_to_db(self):
        db = MySQLdb.connect(
            host = self.host,
            user = self.user,
            password = self.password,
            db = self.database
        )
        return db
    
    def fetch_data(self):
        db = self.connect_to_db()
        self.query = f"SELECT * FROM {self.table}"
        cur = db.cursor()
        cur.execute(self.query)
        rows = cur.fetchall()
        df = pd.DataFrame(rows)
        df.columns = [i[0] for i in cur.description]
        cur.close()
        db.close()
        return df
    
    def fetch_columns(self):
        if self.table is None or self.table not in self.mozliwosci_tabel:
            
            messagebox.showwarning(title="Zły input.",
                                message=f"Powinineś wybrać: {self.mozliwosci_tabel[0]} lub \
                                    {self.mozliwosci_tabel[1]}")
        else:
            self.columns = self.fetch_data().columns
            print(f"Twoje kolumny to {list(self.columns)}")
            return len(self.columns)

            
    
    def wychodzące(self):
        db = self.connect_to_db()
        self.query_wypływy_rodzinne = "SELECT Data_Transakcji, Odbiorca_Płatności,\
	    CASE WHEN Ilość_Produktów = 0 THEN ROUND(Waga_Produktów * Kwota_Płatności,2)\
        ELSE ROUND(Ilość_Produktów * Kwota_Płatności,2)\
        END 'Kwota_Płatności', Grupa_Produktu AS 'Opis_Płatności'\
        FROM Wypływy\
        UNION\
        SELECT Data_Transakcji, Odbiorca_Płatności, Kwota_Płatności, Opis_płatności\
        FROM Wypływy_Rodzinne;"
        
        cur = db.cursor()
        cur.execute(self.query_wypływy_rodzinne)
        rows = cur.fetchall()
        df = pd.DataFrame(rows)
        df.columns = [i[0] for i in cur.description]
        cur.close()
        db.close()
        return df
    
    
    

        
            

    



            
        
        
    



        

