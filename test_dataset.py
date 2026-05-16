import csv

with open("dataset/cloud_logs.csv", "r", encoding="utf-8") as f:
    reader = csv.reader(f)

    for i, row in enumerate(reader):
        print(row)

        if i == 5:
            break