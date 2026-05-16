import sys


def emit_result(service_name, count):
    """
    Print final aggregated result.
    """
    if service_name is not None:
        print(f"{service_name}\t{count}")


def main():
    """
    Reducer for Output 2: Server Error Count by Service.

    Input:
        service_name<TAB>1

    Output:
        service_name<TAB>server_error_count
    """
    current_service = None
    current_count = 0

    for line in sys.stdin:
        line = line.strip()

        if not line:
            continue

        try:
            service_name, count = line.split("\t", 1)
            count = int(count)
        except ValueError:
            continue

        if current_service == service_name:
            current_count += count
        else:
            emit_result(current_service, current_count)
            current_service = service_name
            current_count = count

    emit_result(current_service, current_count)


if __name__ == "__main__":
    main()