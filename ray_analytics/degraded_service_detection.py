import csv
import time
from collections import defaultdict
from pathlib import Path

import ray


DATASET_PATH = Path("dataset/cloud_logs.csv")
OUTPUT_PATH = Path("outputs/ray/degraded_services.txt")

CHUNK_SIZE = 5000

SLOW_RESPONSE_THRESHOLD_MS = 800
SLOW_RATE_THRESHOLD = 0.20
SERVER_ERROR_RATE_THRESHOLD = 0.10
TIMEOUT_ERROR_THRESHOLD = 5


def read_dataset_rows(dataset_path):
    """
    Read the CSV dataset and return all log records as a list of dictionaries.
    """
    if not dataset_path.exists():
        raise FileNotFoundError(f"Dataset not found: {dataset_path}")

    rows = []

    with dataset_path.open("r", encoding="utf-8", newline="") as csv_file:
        reader = csv.DictReader(csv_file)

        for row in reader:
            rows.append(row)

    return rows


def split_into_chunks(rows, chunk_size):
    """
    Split dataset rows into chunks for Ray parallel processing.
    """
    if chunk_size <= 0:
        raise ValueError("chunk_size must be greater than 0")

    chunks = []

    for start_index in range(0, len(rows), chunk_size):
        chunk = rows[start_index:start_index + chunk_size]
        chunks.append(chunk)

    return chunks


@ray.remote
def process_chunk(chunk):
    """
    Ray remote task.

    Each task processes one chunk of log records and produces partial
    service-level statistics.

    For each service, it counts:
    - total_requests
    - slow_requests
    - server_errors
    - timeout_errors
    """
    partial_stats = defaultdict(lambda: {
        "total_requests": 0,
        "slow_requests": 0,
        "server_errors": 0,
        "timeout_errors": 0,
    })

    for row in chunk:
        service_name = row.get("service_name", "").strip()
        status_code_text = row.get("status_code", "").strip()
        response_time_text = row.get("response_time_ms", "").strip()
        error_type = row.get("error_type", "").strip()

        if not service_name:
            continue

        try:
            status_code = int(status_code_text)
            response_time_ms = int(response_time_text)
        except ValueError:
            continue

        partial_stats[service_name]["total_requests"] += 1

        if response_time_ms > SLOW_RESPONSE_THRESHOLD_MS:
            partial_stats[service_name]["slow_requests"] += 1

        if status_code >= 500:
            partial_stats[service_name]["server_errors"] += 1

        if error_type == "Timeout":
            partial_stats[service_name]["timeout_errors"] += 1

    return dict(partial_stats)


def merge_partial_results(partial_results):
    """
    Merge all partial service-level statistics from Ray tasks.
    """
    merged_stats = defaultdict(lambda: {
        "total_requests": 0,
        "slow_requests": 0,
        "server_errors": 0,
        "timeout_errors": 0,
    })

    for partial_result in partial_results:
        for service_name, stats in partial_result.items():
            merged_stats[service_name]["total_requests"] += stats["total_requests"]
            merged_stats[service_name]["slow_requests"] += stats["slow_requests"]
            merged_stats[service_name]["server_errors"] += stats["server_errors"]
            merged_stats[service_name]["timeout_errors"] += stats["timeout_errors"]

    return dict(merged_stats)


def detect_degraded_services(merged_stats):
    """
    Detect degraded services.

    A service is considered degraded if it satisfies at least one condition:
    - slow request rate > 20%
    - server error rate > 10%
    - Timeout errors >= 5

    A service may have multiple reasons.
    """
    degraded_services = []

    for service_name, stats in sorted(merged_stats.items()):
        total_requests = stats["total_requests"]

        if total_requests == 0:
            continue

        slow_requests = stats["slow_requests"]
        server_errors = stats["server_errors"]
        timeout_errors = stats["timeout_errors"]

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
            degraded_services.append({
                "service_name": service_name,
                "reason": "; ".join(reasons),
                "total_requests": total_requests,
                "slow_requests": slow_requests,
                "server_errors": server_errors,
                "timeout_errors": timeout_errors,
                "slow_rate": slow_rate,
                "server_error_rate": server_error_rate,
            })

    return degraded_services


def write_degraded_services(output_path, degraded_services):
    """
    Write final degraded service detection output.

    Required output format:
    service_name,reason
    """
    output_path.parent.mkdir(parents=True, exist_ok=True)

    with output_path.open("w", encoding="utf-8", newline="") as output_file:
        for item in degraded_services:
            output_file.write(f"{item['service_name']},{item['reason']}\n")


def print_summary(merged_stats, degraded_services, runtime_seconds, chunk_count):
    """
    Print execution summary for checking, screenshots, and report evidence.
    """
    print("=" * 80)
    print("Ray Degraded Service Detection")
    print("=" * 80)
    print(f"Dataset path: {DATASET_PATH}")
    print(f"Output path: {OUTPUT_PATH}")
    print(f"Total services: {len(merged_stats)}")
    print(f"Number of Ray chunks: {chunk_count}")
    print(f"Runtime seconds: {runtime_seconds:.6f}")
    print()

    print("Service-level statistics:")
    for service_name, stats in sorted(merged_stats.items()):
        total_requests = stats["total_requests"]

        if total_requests == 0:
            slow_rate = 0.0
            server_error_rate = 0.0
        else:
            slow_rate = stats["slow_requests"] / total_requests
            server_error_rate = stats["server_errors"] / total_requests

        print(
            f"{service_name}: "
            f"total={stats['total_requests']}, "
            f"slow={stats['slow_requests']}, "
            f"server_errors={stats['server_errors']}, "
            f"timeouts={stats['timeout_errors']}, "
            f"slow_rate={slow_rate:.4f}, "
            f"server_error_rate={server_error_rate:.4f}"
        )

    print()
    print("Detected degraded services:")
    for item in degraded_services:
        print(f"{item['service_name']},{item['reason']}")

    print("=" * 80)


def main():
    start_time = time.time()

    rows = read_dataset_rows(DATASET_PATH)
    chunks = split_into_chunks(rows, CHUNK_SIZE)

    if not ray.is_initialized():
        ray.init(ignore_reinit_error=True)

    remote_tasks = []

    for chunk in chunks:
        remote_tasks.append(process_chunk.remote(chunk))

    partial_results = ray.get(remote_tasks)

    merged_stats = merge_partial_results(partial_results)
    degraded_services = detect_degraded_services(merged_stats)
    write_degraded_services(OUTPUT_PATH, degraded_services)

    end_time = time.time()
    runtime_seconds = end_time - start_time

    print_summary(
        merged_stats=merged_stats,
        degraded_services=degraded_services,
        runtime_seconds=runtime_seconds,
        chunk_count=len(chunks)
    )

    ray.shutdown()


if __name__ == "__main__":
    main()