import sqlite3
from models.Database import Database
from models.FileUtil import FileUtil


class App:

    def __init__(self):
        self.db = Database('loans.db')
        self.db.create_tables()
        self.db.delete_tables()

        self.file = FileUtil()
        self.file.clean_files()

        self.expected_yield = {}

    def loadData(self):
        banks = self.db.load_banks()
        convenants = self.db.load_covenants()
        facilities = self.db.load_facilities()

    def write_yield(self):
        output_yeild_file = self.file.get_file("yield.csv", "w")
        try:
            self.db.c.execute("select id, expected_yield from facilities")
        except sqlite3.Error:
            print("Error read from facilities.\n")

        for row in self.db.c:
            output_yeild_file.writerow({'facility_id': row['id'], 'expected_yield': int(row['expected_yield'])})

    def get_yield(self, default_likelihood, interest_rate, amount, facility_interest_rate, expected_yield):
        return expected_yield + (1 - default_likelihood) * interest_rate * amount - default_likelihood * amount - facility_interest_rate * amount

    def pickFacility(self, default_likelihood, state, amount,interest_rate):
        # selection logic:
        # 1) default_likelihood should be lower than facility max_default_likelihood
        # 2) state should not be banned_state
        # 3) amount should be lower than facility amount
        # 4) order by facility amount and interest rate
        # to pick the first facility available (todo: make this more intelligent)

        self.db.c.execute(
            '''select a.id as facility_id, a.amount as amount, a.interest_rate as facility_interest_rate, a.expected_yield as expected_yield from facilities a, banks b, covenants c where a.bank_id = b.id and a.bank_id = c.bank_id and a.id = c.facility_id and c.max_default_likelihood >= ? and c.banned_state<>? and a.amount >= ? and a.interest_rate <=? order by a.amount, a.interest_rate''',
            (default_likelihood, state, amount, interest_rate))

        for row in self.db.c:
            facility_id = int(row['facility_id'])
            facility_interest_rate = float(row['facility_interest_rate'])
            new_amount_available = int(row['amount']) - int(amount)
            new_yield = self.get_yield(default_likelihood, interest_rate, amount, facility_interest_rate, row['expected_yield'])

            try:
                self.db.c.execute("update facilities set amount = ?, expected_yield = ? where id =?", (new_amount_available, new_yield, facility_id))
                self.db.conn.commit()

            except sqlite3.Error:
                print("Error updating a loan.\n")

            break
        return facility_id


    def processLoans(self):
        output_assignment_file = self.file.get_file("assignment.csv", "w")

        input_file = self.file.get_file("loans.csv")
        for row in input_file:
            interest_rate = float(row['interest_rate'])
            amount = float(row['amount'])
            id = row['id']
            default_likelihood = float(row['default_likelihood'])
            state = row['state']

            output_assignment_file.writerow({'loan_id': str(id), 'facility_id': self.pickFacility(default_likelihood, state, amount, interest_rate)})


app = App()
app.loadData()
app.processLoans()
app.write_yield()



