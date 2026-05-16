from pathlib import Path

import pandas as pd


DATASET_PATH = Path("dataset/cloud_logs.csv")

# Local simulation outputs
LOCAL_REQUEST_COUNT_OUTPUT = Path("outputs/local_simulation/request_count_by_service.txt")
LOCAL_SERVER_ERROR_OUTPUT = Path("outputs/local_simulation/server_error_count_by_service.txt")
LOCAL_SLOW_ENDPOINT_OUTPUT = Path("outputs/local_simulation/top10_slow_endpoints.txt")

# Hadoop Streaming outputs
HADOOP_REQUEST_COUNT_OUTPUT = Path("outputs/hadoop_streaming/request_count_output.txt")
HADOOP_SERVER_ERROR_OUTPUT = Path("outputs/hadoop_streaming/server_error_output.txt")
HADOOP_SLOW_ENDPOINT_OUTPUT = Path("outputs/hadoop_streaming/slow_endpoints_output.txt")

# Ray output
RAY_OUTPUT = Path("outputs/ray/degraded_services.txt")


SLOW_RESPONSE_THRESHOLD_MS = 800
SLOW_RATE_THRESHOLD = 0.20
SERVER_ERROR_RATE_THRESHOLD = 0.10
TIMEOUT_ERROR_THRESHOLD = 5


def read_text_lines_with_fallback(path):
    """
    Read text lines with encoding fallback.

    Windows PowerShell may save redirected output as UTF-16 LE.
    Hadoop output or Python output may be UTF-8.
    This function supports common encodings used in this project.
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
    Read output in the format:
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
    Read Ray degraded-service output in the format:
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
    Calculate request count by service directly from the original dataset.
    """
    counts = df["service_name"].value_counts().sort_index()
    return counts.astype(int).to_dict()


def calculate_server_error_count(df):
    """
    Calculate server error count by service directly from the original dataset.
    Server errors are records where status_code >= 500.
    """
    server_error_df = df[df["status_code"] >= 500]
    counts = server_error_df["service_name"].value_counts().sort_index()
    return counts.astype(int).to_dict()


def calculate_top10_slow_endpoints(df):
    """
    Calculate top 10 slow endpoints directly from the original dataset.

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
    Calculate degraded services directly from the original dataset.

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
    print("=" * 90)
    print(f"Validation: {name}")
    print("=" * 90)

    if expected == actual:
        print("PASS: output matches expected result.")
    else:
        print("FAIL: output does not match expected result.")

        expected_keys = set(expected.keys())
        actual_keys = set(actual.keys())

        missing_keys = expected_keys - actual_keys
        extra_keys = actual_keys - expected_keys
        common_keys = expected_keys & actual_keys

        if missing_keys:
            print(f"Missing keys in actual output: {sorted(missing_keys)}")

        if extra_keys:
            print(f"Extra keys in actual output: {sorted(extra_keys)}")

        for key in sorted(common_keys):
            if expected[key] != actual[key]:
                print(
                    f"Mismatch for {key}: "
                    f"expected={expected[key]}, actual={actual[key]}"
                )

    print()


def compare_optional_hadoop_output(name, expected, hadoop_path):
    """
    Compare Hadoop output with expected result only if the Hadoop output file exists.

    This allows the validation script to run even while Hadoop Docker work is still in progress.
    """
    print("=" * 90)
    print(f"Hadoop validation availability check: {name}")
    print("=" * 90)

    if not hadoop_path.exists():
        print(f"SKIPPED: Hadoop output not found yet: {hadoop_path}")
        print("This is acceptable if Hadoop Docker / Hadoop Streaming is still being prepared.")
        print()
        return None

    actual_hadoop = read_key_value_output(hadoop_path)

    if expected == actual_hadoop:
        print("PASS: Hadoop Streaming output matches pandas calculation.")
    else:
        print("FAIL: Hadoop Streaming output does not match pandas calculation.")

        expected_keys = set(expected.keys())
        actual_keys = set(actual_hadoop.keys())

        missing_keys = expected_keys - actual_keys
        extra_keys = actual_keys - expected_keys
        common_keys = expected_keys & actual_keys

        if missing_keys:
            print(f"Missing keys in Hadoop output: {sorted(missing_keys)}")

        if extra_keys:
            print(f"Extra keys in Hadoop output: {sorted(extra_keys)}")

        for key in sorted(common_keys):
            if expected[key] != actual_hadoop[key]:
                print(
                    f"Mismatch for {key}: "
                    f"expected={expected[key]}, hadoop={actual_hadoop[key]}"
                )

    print()
    return actual_hadoop


def compare_local_and_hadoop(name, local_result, hadoop_result):
    """
    Compare local simulation output and Hadoop Streaming output.
    """
    print("=" * 90)
    print(f"Local vs Hadoop comparison: {name}")
    print("=" * 90)

    if hadoop_result is None:
        print("SKIPPED: Hadoop output is not available yet.")
        print()
        return

    if local_result == hadoop_result:
        print("PASS: local simulation output matches Hadoop Streaming output.")
    else:
        print("FAIL: local simulation output does not match Hadoop Streaming output.")

        local_keys = set(local_result.keys())
        hadoop_keys = set(hadoop_result.keys())

        missing_keys = local_keys - hadoop_keys
        extra_keys = hadoop_keys - local_keys
        common_keys = local_keys & hadoop_keys

        if missing_keys:
            print(f"Keys missing in Hadoop output: {sorted(missing_keys)}")

        if extra_keys:
            print(f"Extra keys in Hadoop output: {sorted(extra_keys)}")

        for key in sorted(common_keys):
            if local_result[key] != hadoop_result[key]:
                print(
                    f"Mismatch for {key}: "
                    f"local={local_result[key]}, hadoop={hadoop_result[key]}"
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

    print("=" * 90)
    print("Concrete validation example for report")
    print("=" * 90)
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

    # Expected results calculated directly from the original dataset.
    expected_request_count = calculate_request_count(df)
    expected_server_error_count = calculate_server_error_count(df)
    expected_top10_slow_endpoints = calculate_top10_slow_endpoints(df)
    expected_degraded_services = calculate_degraded_services(df)

    # Local simulation outputs.
    local_request_count = read_key_value_output(LOCAL_REQUEST_COUNT_OUTPUT)
    local_server_error_count = read_key_value_output(LOCAL_SERVER_ERROR_OUTPUT)
    local_top10_slow_endpoints = read_key_value_output(LOCAL_SLOW_ENDPOINT_OUTPUT)

    # Ray output.
    actual_degraded_services = read_ray_output(RAY_OUTPUT)

    # A. Validate local simulation outputs against pandas.
    compare_dicts(
        "Local Simulation - Request Count by Service",
        expected_request_count,
        local_request_count
    )

    compare_dicts(
        "Local Simulation - Server Error Count by Service",
        expected_server_error_count,
        local_server_error_count
    )

    compare_dicts(
        "Local Simulation - Top 10 Slow Endpoints",
        expected_top10_slow_endpoints,
        local_top10_slow_endpoints
    )

    # B. Validate Hadoop Streaming outputs against pandas, if available.
    hadoop_request_count = compare_optional_hadoop_output(
        "Hadoop Streaming - Request Count by Service",
        expected_request_count,
        HADOOP_REQUEST_COUNT_OUTPUT
    )

    hadoop_server_error_count = compare_optional_hadoop_output(
        "Hadoop Streaming - Server Error Count by Service",
        expected_server_error_count,
        HADOOP_SERVER_ERROR_OUTPUT
    )

    hadoop_top10_slow_endpoints = compare_optional_hadoop_output(
        "Hadoop Streaming - Top 10 Slow Endpoints",
        expected_top10_slow_endpoints,
        HADOOP_SLOW_ENDPOINT_OUTPUT
    )

    # C. Compare local simulation outputs with Hadoop Streaming outputs.
    compare_local_and_hadoop(
        "Request Count by Service",
        local_request_count,
        hadoop_request_count
    )

    compare_local_and_hadoop(
        "Server Error Count by Service",
        local_server_error_count,
        hadoop_server_error_count
    )

    compare_local_and_hadoop(
        "Top 10 Slow Endpoints",
        local_top10_slow_endpoints,
        hadoop_top10_slow_endpoints
    )

    # D. Validate Ray output against pandas.
    compare_dicts(
        "Ray Output - Degraded Service Detection",
        expected_degraded_services,
        actual_degraded_services
    )

    # E. Print one concrete example for the report.
    print_manual_validation_example(df)


if __name__ == "__main__":
    main()