import csv
import os
import glob
import logjson
from collections import defaultdict

newest = max(glob.iglob('/Users/lakshmi.sanjeevaraya/PycharmProjects/ccpa_automation_LOG/Logfile' + '*.log'), key=os.path.getctime)
print("&&&&&&&&&&&&&&&&&&&&&",newest)
def another_method():
    headers = ["Transction_ID", "TaskID", "RequestID", "S3 file", "Result", "CloseStatusstate", "CloseStatusCode"]
    print("$$$$$$$$$$$$$$$$$$$$$$$$$$$")
    print(newest)
    #
    # with open(newest) as f_input, open('test.csv', 'w', newline='') as f_output:
    #     csv_writer = csv.writer(f_output, delimiter=',', quoting=csv.QUOTE_NONNUMERIC)
    #     f_csv = csv.DictWriter(f_output, headers)
    #     f_csv.writeheader()
    #     if f_csv:
    #         csv_writer.writerow(col[0] for col in self.cursor.description)
    #     for row in self.cursor:
    #         csv_writer.writerow(row)


    with open(newest) as f_input, open('test.csv', 'w',newline='') as f_output:
        f_csv = csv.DictWriter(f_output, headers)
        f_csv.writeheader()
        print("%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%")
        tranaction=[]
        lines={}

        dd = defaultdict(list)

        for line in f_input:

            items = line.split()
            key, values = items[0], items[1:]
            lines[key] = values
            print("***********",lines)
            for key, value in lines.items():
                dd[key].append(value)

            print(dd)

            f_output.write(f'{line}\n')

if __name__ == '__main__':
    another_method()

