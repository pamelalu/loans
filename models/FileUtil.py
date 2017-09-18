import csv
import os


class FileUtil:

    FILE_DIR = 'files/'
    write_files = {
            "assignment.csv": ['loan_id', 'facility_id'],
            "yield.csv": ['facility_id', 'expected_yield'],
        }

    def get_fieldnames(self, file_name):
        if file_name in self.write_files:
            return self.write_files[file_name]
        else:
            return []

    def get_file(self, file_name, mode ='r'):
        if mode == 'r':
            file_handler = csv.DictReader(open(self.FILE_DIR + file_name))
        elif mode == 'w':
            file_handler = csv.DictWriter(open(self.FILE_DIR + file_name, 'w'), fieldnames=self.get_fieldnames(file_name))
            file_handler.writeheader()

        return file_handler

    def clean_files(self):
        if os.path.isfile("assignment.csv"):
            os.remove(self.FILE_DIR + "assignment.csv")
        if os.path.isfile("yield.csv"):
            os.remove(self.FILE_DIR + "yield.csv")

