import sys
import csv


def main():
    """
    Mapper for Output 3: Top 10 Slow Endpoints.

    Input:
        CSV log records from stdin.

    Output:
        service_name,endpoint<TAB>1
        only when response_time_ms > 800
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
        endpoint = row[4].strip()
        response_time_text = row[7].strip()

        try:
            response_time_ms = int(response_time_text)
        except ValueError:
            continue

        if response_time_ms > 800 and service_name and endpoint:
            key = f"{service_name},{endpoint}"
            print(f"{key}\t1")


if __name__ == "__main__":
    main()