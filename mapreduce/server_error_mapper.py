#!/usr/bin/env python
import sys
import csv


def main():
    """
    Mapper for Output 2: Server Error Count by Service.

    Input:
        CSV log records from stdin.

    Output:
        service_name<TAB>1
        only when status_code >= 500
    """
    reader = csv.reader(sys.stdin)

    for row in reader:
        # Skip empty or malformed rows
        if not row or len(row) < 10:
            continue

        # Skip header row
        if row[0] == "timestamp":
            continue

        service_name = row[3].strip()
        status_code_text = row[6].strip()

        try:
            status_code = int(status_code_text)
        except ValueError:
            continue

        if status_code >= 500 and service_name:
            print("{}\t1".format(service_name))


if __name__ == "__main__":
    main()