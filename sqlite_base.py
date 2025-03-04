import sqlite3

class SQLiteBase:
    def __init__(self, db_name):
        self.db_name = db_name
        self.conn = None
        self.cursor = None

    def connect(self):
        self.conn= sqlite3.connect(self.db_name)
        self.cursor =self.conn.cursor()
    
    def close_connection(self):
        if self.conn:
            self.conn.close()
