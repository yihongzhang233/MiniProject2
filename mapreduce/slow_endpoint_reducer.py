#!/usr/bin/env python
import sys


def main():
    """
    Reducer for Output 3: Top 10 Slow Endpoints.

    Input:
        service_name,endpoint<TAB>1

    Output:
        top 10 service_name,endpoint<TAB>slow_request_count
    """
    current_endpoint = None
    current_count = 0
    results = []

    for line in sys.stdin:
        line = line.strip()

        if not line:
            continue

        try:
            endpoint_key, count = line.split("\t", 1)
            count = int(count)
        except ValueError:
            continue

        if current_endpoint == endpoint_key:
            current_count += count
        else:
            if current_endpoint is not None:
                results.append((current_endpoint, current_count))

            current_endpoint = endpoint_key
            current_count = count

    if current_endpoint is not None:
        results.append((current_endpoint, current_count))

    # Sort by slow request count in descending order
    results.sort(key=lambda item: item[1], reverse=True)

    # Output top 10
    for endpoint_key, count in results[:10]:
        print("{}\t{}".format(endpoint_key, count))


if __name__ == "__main__":
    main()