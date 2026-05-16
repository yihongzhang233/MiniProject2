# Cloud Service Log Analytics

## 1. Project Overview

This mini-project analyses a synthetic cloud service log dataset using cloud object storage, MapReduce baseline analytics, Ray-based parallel analytics, and result comparison.

The required workflow is:

```text
cloud service log dataset
→ cloud object storage
→ MapReduce baseline analytics
→ Ray extension analytics
→ comparison
```

The dataset contains request-level logs from a simplified cloud-hosted application. Each record includes service name, endpoint, HTTP method, status code, response time, region, and error type.

---

## 2. Dataset

The dataset is stored locally as:

```text
dataset/cloud_logs.csv
```

The CSV fields are:

```text
timestamp, request_id, user_id, service_name, endpoint, http_method,
status_code, response_time_ms, region, error_type
```

The same dataset is used for:

- local MapReduce streaming simulation
- Hadoop Streaming execution
- Ray degraded-service detection
- validation

---

## 3. Cloud Object Storage

The dataset was uploaded manually to Alibaba Cloud OSS through the OSS console.

The object was stored as a private object. Sensitive information such as bucket name, cloud account details, usernames, and object paths are not included in this repository or report.

An anonymised evidence screenshot is stored in:

```text
screenshots/oss_upload_evidence_anonymised.png
```

An additional bucket upload screenshot may be stored as:

```text
screenshots/bucket_upload.png
```

Alibaba Cloud OSS is suitable for this log analytics workload because object storage provides scalable capacity, high durability, and low-cost storage for large log files. It also supports batch analytics workflows where data is stored first and processed later.

---

## 4. Project Structure

```text
cloud-log-analytics/
│
├── dataset/
│   └── cloud_logs.csv
│
├── mapreduce/
│   ├── request_count_mapper.py
│   ├── request_count_reducer.py
│   ├── server_error_mapper.py
│   ├── server_error_reducer.py
│   ├── slow_endpoint_mapper.py
│   └── slow_endpoint_reducer.py
│
├── ray_analytics/
│   └── degraded_service_detection.py
│
├── validation/
│   └── validate_results.py
│
├── outputs/
│   ├── local_simulation/
│   │   ├── request_count_by_service.txt
│   │   ├── server_error_count_by_service.txt
│   │   └── top10_slow_endpoints.txt
│   │
│   ├── hadoop_streaming/
│   │   ├── request_count_output.txt
│   │   ├── server_error_output.txt
│   │   └── slow_endpoints_output.txt
│   │
│   └── ray/
│       └── degraded_services.txt
│
├── screenshots/
│
├── docs/
│
├── README.md
├── requirements.txt
└── .gitignore
```

---

## 5. Environment Setup

This project uses Python 3.10.

Create and activate a virtual environment:

```powershell
py -3.10 -m venv .venv
.venv\Scripts\Activate.ps1
```

Install dependencies:

```powershell
pip install -r requirements.txt
```

The required Python packages are:

```text
pandas
ray
```

`pandas` is used for validation and result checking.  
`ray` is used for parallel degraded-service detection.

---

## 6. MapReduce Baseline Analytics

The MapReduce baseline produces three required outputs:

```text
1. Request count by service
2. Server error count by service
3. Top 10 slow endpoints
```

### 6.1 Output 1: Request Count by Service

This job counts how many log records belong to each service.

Run:

```powershell
Get-Content dataset/cloud_logs.csv |
python mapreduce/request_count_mapper.py |
Sort-Object |
python mapreduce/request_count_reducer.py > outputs/local_simulation/request_count_by_service.txt
```

Output file:

```text
outputs/local_simulation/request_count_by_service.txt
```

Selected output evidence screenshot:

```text
screenshots/mapreduce_output1_request_count.png
```

---

### 6.2 Output 2: Server Error Count by Service

This job counts records where:

```text
status_code >= 500
```

Run:

```powershell
Get-Content dataset/cloud_logs.csv |
python mapreduce/server_error_mapper.py |
Sort-Object |
python mapreduce/server_error_reducer.py > outputs/local_simulation/server_error_count_by_service.txt
```

Output file:

```text
outputs/local_simulation/server_error_count_by_service.txt
```

Selected output evidence screenshot:

```text
screenshots/mapreduce_output2_server_error_count.png
```

---

### 6.3 Output 3: Top 10 Slow Endpoints

A request is considered slow when:

```text
response_time_ms > 800
```

Run:

```powershell
Get-Content dataset/cloud_logs.csv |
python mapreduce/slow_endpoint_mapper.py |
Sort-Object |
python mapreduce/slow_endpoint_reducer.py > outputs/local_simulation/top10_slow_endpoints.txt
```

Output file:

```text
outputs/local_simulation/top10_slow_endpoints.txt
```

Selected output evidence screenshot:

```text
screenshots/mapreduce_output3_top10_slow_endpoints.png
```

---

## 7. Hadoop Streaming Execution

The same Python mapper and reducer scripts can also be executed using Hadoop Streaming in a Docker-based Hadoop environment.

Hadoop Streaming outputs are stored in:

```text
outputs/hadoop_streaming/
```

Expected Hadoop output files:

```text
outputs/hadoop_streaming/request_count_output.txt
outputs/hadoop_streaming/server_error_output.txt
outputs/hadoop_streaming/slow_endpoints_output.txt
```

If Hadoop Streaming execution is still in progress, these files may be added later.

---

## 8. Ray Extension Analytics

The Ray extension detects degraded services.

A service is considered degraded if at least one of the following conditions is satisfied:

```text
slow request rate > 20%
server error rate > 10%
Timeout errors >= 5
```

Run:

```powershell
python ray_analytics/degraded_service_detection.py
```

Output file:

```text
outputs/ray/degraded_services.txt
```

The implementation uses `@ray.remote` tasks to process chunks of `dataset/cloud_logs.csv` in parallel. Each task calculates partial service-level statistics, including total requests, slow requests, server errors, and Timeout errors. The partial results are then merged to generate the final degraded-service detection output.

Ray execution evidence screenshot:

```text
screenshots/ray_degraded_service_output.png
```

---

## 9. Validation

The results are validated using a separate pandas-based validation script.

Run:

```powershell
python validation/validate_results.py
```

The validation script checks:

```text
1. local MapReduce outputs against direct pandas calculation
2. Hadoop Streaming outputs against direct pandas calculation, if available
3. local MapReduce outputs against Hadoop Streaming outputs, if available
4. Ray degraded-service detection output against direct pandas calculation
```

Validation script:

```text
validation/validate_results.py
```

Validation evidence screenshots:

```text
screenshots/validation_results_part1.png
screenshots/validation_results_part2.png
```

---

## 10. Output Files

### Local MapReduce Simulation Outputs

```text
outputs/local_simulation/request_count_by_service.txt
outputs/local_simulation/server_error_count_by_service.txt
outputs/local_simulation/top10_slow_endpoints.txt
```

### Hadoop Streaming Outputs

```text
outputs/hadoop_streaming/request_count_output.txt
outputs/hadoop_streaming/server_error_output.txt
outputs/hadoop_streaming/slow_endpoints_output.txt
```

### Ray Output

```text
outputs/ray/degraded_services.txt
```

---

## 11. Execution Environment

Example execution environment:

```text
Python version: 3.10.x
IDE: VS Code
Operating system: Windows
Cloud object storage: Alibaba Cloud OSS
MapReduce local execution: PowerShell command-line streaming simulation
MapReduce Hadoop execution: Hadoop Streaming in Docker, if available
Ray execution: Ray local mode
```

Runtime values should be recorded from actual runs and reported in the group report.

---

## 12. Notes on Anonymisation

This repository and report does not include student names, student IDs, cloud account details, bucket names, GitHub usernames, AccessKeys, or identifiable file paths.

Screenshots used as report evidence were anonymised before submission.

