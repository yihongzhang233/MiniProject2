import sys
import csv


def main():
    """
    Mapper for Output 1: Request Count by Service.

    Input:
        CSV log records from stdin.

    Output:
        service_name<TAB>1
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

        if service_name:
            print(f"{service_name}\t1")


if __name__ == "__main__":
    main()