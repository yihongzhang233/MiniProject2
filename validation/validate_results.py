from pathlib import Path

import pandas as pd


DATASET_PATH = Path("dataset/cloud_logs.csv")

REQUEST_COUNT_OUTPUT = Path("outputs/local_simulation/request_count_by_service.txt")
SERVER_ERROR_OUTPUT = Path("outputs/local_simulation/server_error_count_by_service.txt")
SLOW_ENDPOINT_OUTPUT = Path("outputs/local_simulation/top10_slow_endpoints.txt")
RAY_OUTPUT = Path("outputs/ray/degraded_services.txt")


SLOW_RESPONSE_THRESHOLD_MS = 800
SLOW_RATE_THRESHOLD = 0.20
SERVER_ERROR_RATE_THRESHOLD = 0.10
TIMEOUT_ERROR_THRESHOLD = 5


def read_text_lines_with_fallback(path):
    """
    Read text lines with encoding fallback.

    Some Windows PowerShell output files may be saved as UTF-16 LE when using '>'.
    This helper allows the validation script to read UTF-8, UTF-8 with BOM,
    UTF-16, and UTF-16 LE files.
    """
    encodings = ["utf-8", "utf-8-sig", "utf-16", "utf-16-le"]

    last_error = None

    for encoding in encodings:
        try:
            with path.open("r", encoding=encoding) as file:
                return file.readlines()
        except UnicodeDecodeError as error:
            last_error = error

    raise UnicodeDecodeError(
        last_error.encoding,
        last_error.object,
        last_error.start,
        last_error.end,
        f"Unable to decode {path} with supported encodings."
    )


def read_key_value_output(path):
    """
    Read MapReduce output in the format:
    key<TAB>value

    Returns:
        dict[str, int]
    """
    result = {}

    if not path.exists():
        raise FileNotFoundError(f"Output file not found: {path}")

    lines = read_text_lines_with_fallback(path)

    for line in lines:
        line = line.strip()

        if not line:
            continue

        try:
            key, value = line.split("\t", 1)
            result[key] = int(value)
        except ValueError:
            raise ValueError(f"Invalid key-value line in {path}: {line}")

    return result


def read_ray_output(path):
    """
    Read Ray output in the format:
    service_name,reason

    Returns:
        dict[str, str]
    """
    result = {}

    if not path.exists():
        raise FileNotFoundError(f"Ray output file not found: {path}")

    lines = read_text_lines_with_fallback(path)

    for line in lines:
        line = line.strip()

        if not line:
            continue

        try:
            service_name, reason = line.split(",", 1)
            result[service_name] = reason
        except ValueError:
            raise ValueError(f"Invalid Ray output line in {path}: {line}")

    return result


def calculate_request_count(df):
    """
    Calculate request count by service directly from the dataset.
    """
    counts = df["service_name"].value_counts().sort_index()
    return counts.astype(int).to_dict()


def calculate_server_error_count(df):
    """
    Calculate server error count by service directly from the dataset.

    Server errors are records where status_code >= 500.
    """
    server_error_df = df[df["status_code"] >= 500]
    counts = server_error_df["service_name"].value_counts().sort_index()
    return counts.astype(int).to_dict()


def calculate_top10_slow_endpoints(df):
    """
    Calculate top 10 slow endpoints directly from the dataset.

    Slow request condition:
        response_time_ms > 800

    Key format:
        service_name,endpoint
    """
    slow_df = df[df["response_time_ms"] > SLOW_RESPONSE_THRESHOLD_MS].copy()
    slow_df["endpoint_key"] = slow_df["service_name"] + "," + slow_df["endpoint"]

    counts = slow_df["endpoint_key"].value_counts()
    top10 = counts.head(10)

    return top10.astype(int).to_dict()


def calculate_degraded_services(df):
    """
    Calculate degraded services directly from the dataset.

    A service is considered degraded if at least one condition is satisfied:
    - slow request rate > 20%
    - server error rate > 10%
    - Timeout errors >= 5

    Returns:
        dict[service_name] = reason
    """
    result = {}

    for service_name, group in df.groupby("service_name"):
        total_requests = len(group)

        slow_requests = len(
            group[group["response_time_ms"] > SLOW_RESPONSE_THRESHOLD_MS]
        )

        server_errors = len(
            group[group["status_code"] >= 500]
        )

        timeout_errors = len(
            group[group["error_type"] == "Timeout"]
        )

        slow_rate = slow_requests / total_requests
        server_error_rate = server_errors / total_requests

        reasons = []

        if slow_rate > SLOW_RATE_THRESHOLD:
            reasons.append("high slow request rate")

        if server_error_rate > SERVER_ERROR_RATE_THRESHOLD:
            reasons.append("high server error rate")

        if timeout_errors >= TIMEOUT_ERROR_THRESHOLD:
            reasons.append("repeated timeout errors")

        if reasons:
            result[service_name] = "; ".join(reasons)

    return dict(sorted(result.items()))


def compare_dicts(name, expected, actual):
    """
    Compare two dictionaries and print validation result.
    """
    print("=" * 80)
    print(f"Validation: {name}")
    print("=" * 80)

    if expected == actual:
        print("PASS: output matches direct pandas calculation.")
    else:
        print("FAIL: output does not match direct pandas calculation.")

        expected_keys = set(expected.keys())
        actual_keys = set(actual.keys())

        missing_keys = expected_keys - actual_keys
        extra_keys = actual_keys - expected_keys
        common_keys = expected_keys & actual_keys

        if missing_keys:
            print(f"Missing keys in output: {sorted(missing_keys)}")

        if extra_keys:
            print(f"Extra keys in output: {sorted(extra_keys)}")

        for key in sorted(common_keys):
            if expected[key] != actual[key]:
                print(
                    f"Mismatch for {key}: "
                    f"expected={expected[key]}, actual={actual[key]}"
                )

    print()


def print_manual_validation_example(df):
    """
    Print one concrete degraded-service validation example for report writing.
    """
    service_name = "payment-service"
    group = df[df["service_name"] == service_name]

    total_requests = len(group)

    slow_requests = len(
        group[group["response_time_ms"] > SLOW_RESPONSE_THRESHOLD_MS]
    )

    server_errors = len(
        group[group["status_code"] >= 500]
    )

    timeout_errors = len(
        group[group["error_type"] == "Timeout"]
    )

    slow_rate = slow_requests / total_requests
    server_error_rate = server_errors / total_requests

    print("=" * 80)
    print("Concrete validation example for report")
    print("=" * 80)
    print(f"Checked service: {service_name}")
    print(f"total_requests = {total_requests}")
    print(f"slow_requests = {slow_requests}")
    print(f"server_errors = {server_errors}")
    print(f"timeout_errors = {timeout_errors}")
    print(f"slow_rate = {slow_rate:.4f}")
    print(f"server_error_rate = {server_error_rate:.4f}")
    print()
    print("Reasoning:")
    print(
        f"- slow_rate > 0.20: "
        f"{slow_rate:.4f} > 0.20 is {slow_rate > 0.20}"
    )
    print(
        f"- server_error_rate > 0.10: "
        f"{server_error_rate:.4f} > 0.10 is {server_error_rate > 0.10}"
    )
    print(
        f"- timeout_errors >= 5: "
        f"{timeout_errors} >= 5 is {timeout_errors >= 5}"
    )
    print()


def main():
    if not DATASET_PATH.exists():
        raise FileNotFoundError(f"Dataset not found: {DATASET_PATH}")

    df = pd.read_csv(DATASET_PATH)

    expected_request_count = calculate_request_count(df)
    actual_request_count = read_key_value_output(REQUEST_COUNT_OUTPUT)

    expected_server_error_count = calculate_server_error_count(df)
    actual_server_error_count = read_key_value_output(SERVER_ERROR_OUTPUT)

    expected_top10_slow_endpoints = calculate_top10_slow_endpoints(df)
    actual_top10_slow_endpoints = read_key_value_output(SLOW_ENDPOINT_OUTPUT)

    expected_degraded_services = calculate_degraded_services(df)
    actual_degraded_services = read_ray_output(RAY_OUTPUT)

    compare_dicts(
        "MapReduce Output 1 - Request Count by Service",
        expected_request_count,
        actual_request_count
    )

    compare_dicts(
        "MapReduce Output 2 - Server Error Count by Service",
        expected_server_error_count,
        actual_server_error_count
    )

    compare_dicts(
        "MapReduce Output 3 - Top 10 Slow Endpoints",
        expected_top10_slow_endpoints,
        actual_top10_slow_endpoints
    )

    compare_dicts(
        "Ray Output - Degraded Service Detection",
        expected_degraded_services,
        actual_degraded_services
    )

    print_manual_validation_example(df)


if __name__ == "__main__":
    main()