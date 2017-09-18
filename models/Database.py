import sqlite3
from models.FileUtil import FileUtil

class Database:

    def __init__(self, db_file):
        try:
            self.conn = sqlite3.connect(db_file)
            self.conn.row_factory = sqlite3.Row
            self.c = self.conn.cursor()
            self.fileUtil = FileUtil()
        except sqlite3.Error:
            print("Error open db.\n")

    def create_tables(self):
        try:
            self.c.execute("create table if not exists banks (id real, name text)")
            self.c.execute("create table if not exists facilities (id real, interest_rate real, amount real, bank_id real, expected_yield real)")
            self.c.execute("create table if not exists covenants (bank_id real, facility_id real, max_default_likelihood real, banned_state text)")
        except sqlite3.Error:
            print("Error creating tables.\n")

    def delete_tables(self):
        try:
            self.c.execute("delete from banks")
            self.c.execute("delete from facilities")
            self.c.execute("delete from covenants")
        except sqlite3.Error:
            print("Error creating tables.\n")

    def load_banks(self):
        input_file = self.fileUtil.get_file("banks.csv")
        banks = []
        for row in input_file:
            bank = (row['id'], row['name'])
            banks.append(bank)
        try:
            self.c.executemany('''INSERT INTO banks(id, name) VALUES(?,?)''', banks)
            self.conn.commit()
        except sqlite3.Error:
            print("Error loading banks data.\n")
        return banks

    def load_covenants(self):
        input_file = self.fileUtil.get_file("covenants.csv")
        convenants = []
        for row in input_file:
            if row['max_default_likelihood']=='':
                max_default_likelihood = 1
            else:
                max_default_likelihood = row['max_default_likelihood']
            convenant = (row['facility_id'], max_default_likelihood, row['bank_id'], row['banned_state'])
            convenants.append(convenant)
        try:
            self.c.executemany("INSERT INTO covenants(facility_id, max_default_likelihood, bank_id, banned_state) VALUES(?,?,?,?)", convenants)
            self.conn.commit()
        except sqlite3.Error:
            print("Error loading convenants data.\n")


    def load_facilities(self):
        input_file = self.fileUtil.get_file("facilities.csv")
        facilities = []
        for row in input_file:
            id = row['id']
            facility = (row['amount'], row['interest_rate'], id, row['bank_id'], 0)
            facilities.append(facility)
        try:
            self.c.executemany("INSERT INTO facilities(amount, interest_rate, id, bank_id, expected_yield) VALUES(?,?,?,?,?)", facilities)
            self.conn.commit()
        except sqlite3.Error:
            print("Error loading facilities data.\n")
