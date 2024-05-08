import tkinter as tk
from tkinter import ttk
from tkinter import messagebox
from Connection_Class import Connection
from PIL import Image, ImageTk

import pymysql
pymysql.install_as_MySQLdb()
import MySQLdb




class UI:
    
    def __init__(self,root):
        self.root = root
        self.root.title("Dodawanie itemów do bazy danych")
        
    def show_items(self):
        self.tabele = ['Przychody','Wypływy','Wypływy_Rodzinne']
        #Adding a First Row to the UI
        self.fst_column = tk.Label(self.root,text="Co chcesz dodać?")
        self.fst_column.grid(row=0, column=0)
        #Adding a Text file
        self.entry_test = tk.Entry(self.root)
        self.entry_test.grid(row=1, column=0)
        
        #Adding a Second Row to the UI
        
        self.scn_column = tk.Label(self.root,text="Do jakiej tabeli?")
        self.scn_column.grid(row=0,column=1)
        
        #Adding a Possible Tables
        
        self.tables = ttk.Combobox(self.root, textvariable="Do jakiej tabeli?", values=self.tabele)
        self.tables.grid(row=1, column=1)
        
        #Adding a Action 
        
        self.third_column = tk.Label(self.root,text="Kliknij przycisk aby sprawdzić kolumny.")
        self.third_column.grid(row=0, column=3)
        self.sprawdz_kolumny = tk.Button(self.root,text='❎',height=2,command=self.sprawdz_kolumny)
        self.sprawdz_kolumny.grid(row=1, column=3)
        
        # Dodawanie informacji o tym jakie są kolumny:
        self.forth_column = tk.Label(self.root,text="Kliknij przycisk aby dodać wiersze.")
        self.forth_column.grid(row=0, column=4)
        self.dodaj_wiersze = tk.Button(self.root, text="📪", height=2, command=self.dodaj_wiersze)
        self.dodaj_wiersze.grid(row=1,column=4)
        
        #Dodanie informacji o ostatnim paragonie.
        
        self.paragon_row = tk.Label(self.root,text="Nr ostatniego paragonu.")
        self.paragon_row.grid(row=2,column=3)
        
        self.paragon_button = tk.Button(self.root, text='🧾', height=2,command=self.sprawdz_paragon)
        self.paragon_button.grid(row=3, column=3)
        
        #Dodanie informacji o Grupach produktów
        
        self.grupy = tk.Label(self.root, text="Unikatowe Grupy.")
        self.grupy.grid(row=2, column=4)
        
        self.group_button = tk.Button(self.root, text='👨‍👧‍👦', height=2, command=self.sprawdz_grupy)
        self.group_button.grid(row=3, column=4)

    def sprawdz_kolumny(self):
        conn = Connection(
        host='127.0.0.1',
        user='root',
        password='ccczesiulek123',
        database='SpendingsDB')
        
        if self.tables.get() in conn.mozliwosci_tabel:
            conn.table = self.tables.get()
            conn.fetch_columns()
            
    def dodaj_wiersze(self):
        conn = Connection(
        host='127.0.0.1',
        user='root',
        password='ccczesiulek123',
        database='SpendingsDB')
        
        self.wpisane_wartosci = list("".join(x for x in self.entry_test.get()).split(','))
        
        if self.tables.get() != "":
            conn.table = self.tables.get()
        
        if self.tables.get() == "":
            messagebox.showerror(title="Error ❎", message="Musisz wybrać tabele.")
            
        elif len(self.wpisane_wartosci) != conn.fetch_columns():
            messagebox.showerror(title="Error ❎", message="Wpisałeś za mało wartości.")
            
        else:
            place_holder = ', '.join(['%s'] * len(self.wpisane_wartosci))
            query = f"INSERT INTO {self.tables.get()} VALUES ({place_holder})"
            conn = Connection(
            host='127.0.0.1',
            user='root',
            password='ccczesiulek123',
            database='SpendingsDB')
            
            db = conn.connect_to_db()
            cursor = db.cursor()
            try:
                cursor.execute(query,self.wpisane_wartosci)
                db.commit()
                cursor.close()
                db.close()
                messagebox.showinfo(title="Sukces.",message=f"Skutecznie dodaliśmy: {self.wpisane_wartosci}")
            except KeyboardInterrupt:
                print("Błąd.")
                
                
    def sprawdz_paragon(self):
        if self.tables.get() != 'Wypływy':
            messagebox.showerror(title="Error ❎", message="Musisz wybrać tabele związaną z wypływami..")
        else:
            conn = Connection(
            host='127.0.0.1',
            user='root',
            password='ccczesiulek123',
            database='SpendingsDB',
            table='Wypływy')
                
            print(f"Twój ostatni numer paragonu to {conn.fetch_data()['Nr_Paragonu'].max()}")
            
            
    def sprawdz_grupy(self):
        if self.tables.get() != 'Wypływy':
            messagebox.showerror(title="Error ❎", message="Musisz wybrać tabele związaną z wypływami..")
        else:
            conn = Connection(
            host='127.0.0.1',
            user='root',
            password='ccczesiulek123',
            database='SpendingsDB',
            table='Wypływy')
                
            print(f"Twoje grupy to: {', '.join(x for x in conn.fetch_data()['Grupa_Produktu'].unique())}")
        
                
                
                


        

            
        
    
        
        
        
    
            
            
            
            
        

        
            
        


        




