# Google Cloud DLP Tools

This repository provides a set of tools for analyzing data using Google Cloud Platform's Data Loss Prevention (DLP) API. These tools offer enhanced control for users to identify existing policy tags in BigQuery, analyze table sizes, and manage scanning sizes and costs effectively.

## **Disclaimer** 

License: Apache 2.0

This is not an officially supported Google product. This project is not
eligible for the [Google Open Source Software Vulnerability Rewards
Program](https://bughunters.google.com/open-source-security).

## Prerequisites

Before using these tools, ensure the following prerequisites are met:

1. **Google Cloud Project**: A GCP project must be set up.
2. **DLP API Enabled**: Enable the DLP API in your GCP project.
3. **Service Account**: Create a service account with the required permissions and download its JSON key file.
4. **Python Environment**: Install Python 3.7 or later.

## Setup Instructions

1. Clone the repository:
  ```bash
  git clone https://github.com/benmizrahi/gcp-dlp-tools.git
  cd gcp-dlp-tools
  ```

2. Install the required dependencies:
  ```bash
  pip install -r requirements.txt
  ```

3. Create a service account and assign the necessary permissions:
  ```bash
  gcloud iam service-accounts create dlp-scripts --description="Service account for DLP tools" --display-name="dlp-scripts-helper"
  ```

  The service account requires the following permissions:
  
  - **Organization Level**:
    - `roles/cloudasset.viewer`: Read-only access to scan resources from the Asset Inventory ([details](https://cloud.google.com/asset-inventory/docs/roles-permissions#roles)).
  
  - **Project Level** (for the project where the DLP API is enabled):
    - `roles/bigquery.admin`: To write outputs to BigQuery.
    - `roles/storage.objectUser`: To write outputs to Google Cloud Storage.

4. Set the `GOOGLE_APPLICATION_CREDENTIALS` environment variable to point to your service account key file:
  ```bash
  export GOOGLE_APPLICATION_CREDENTIALS="/path/to/your-service-account-key.json"
  ```

## Usage

Run the script with the following commands based on your requirements:

### 1. Find Policy Tags
Scan a dataset for policy tags and export the results to a specified output path. This operation does not perform DLP scanning or BigQuery jobs (unless results are written to BigQuery).

```bash
python main.py find-policy-tags --scan-path gcp://<org-id>/<folder_id>/<project_id>/<dataset_id> --output-path bq://<project_id>/<dataset_name>/<table_name>
```

**Options**:
- `--scan-path`: Specify the metadata scanning level:
  - Organization: `gcp://<org-id>/`
  - Folder: `gcp://<org-id>/<folder_id>/`
  - Project: `gcp://<org-id>/<folder_id>/<project_id>/`
  - Dataset: `gcp://<org-id>/<folder_id>/<project_id>/<dataset_id>/`
- `--output-path`: Specify where to save the results:
  - BigQuery: `bq://<project_id>/<dataset_name>/<table_name>`
  - Google Storage: `gcs://<bucket_name>/<folder>/<file_name>.csv`
  - Local File: `file://<file_name>.csv`
- `--page-size`: Number of results per page (default: 100).

---

### 2. Get Sample Scan Size
Retrieve the sample size of a scan and export the results to a BigQuery table. This operation uses BigQuery's information schema and does not perform DLP scanning.

```bash
python main.py get-sample-scan-size --scan-path gcp://<org-id>/<folder_id>/<project_id>/<dataset_id> --output-path bq://<project_id>/<dataset_name>/<table_name>
```

**Options**:
- `--scan-path`: Specify the metadata scanning level (same as above).
- `--output-path`: Specify where to save the results (same as above).
- `--page-size`: Number of results per page (default: 100).

---

### 3. Perform Sensitive Data Protection (SDP) Scan
Run a Sensitive Data Protection (SDP) scan on a specific dataset or table and save the results to a BigQuery table. This operation performs DLP scanning based on the provided filter path.

```bash
python main.py sdp-scan --project-id <dlp-project> --dataset-id <dataset_where_view_exists> --filter-path gcp://<org-id>/<project_id>/<dataset_name>/<table_name> --output-path bq://<project_id>/<dataset_name>/<output_results_table_name>
```

**Options**:
- `--project-id` (Required): The project where the DLP scan will run.
- `--dataset-id` (Required): The dataset containing the `global_tables_view` output from `get-sample-scan-size`.
- `--filter-path` (Required): Specify the metadata scanning level (same as above).
- `--output-path` (Required): Specify where to save the results in BigQuery.
- `--rows-limit-percent` (Optional): Percentage (1-100) of rows to scan.
- `--rows-limit` (Optional, Default: 100): Number of rows to scan.
- `--info-types` (Optional): Provide a text file specifying specific info types to scan. If not provided, the default info types will be used. Example:
  ```
  # info_types.txt
  EMAIL_ADDRESS
  PHONE_NUMBER
  REGEX_EXPRESSION:\d{3}-\d{2}-\d{4}
  ```

## Notes

- Ensure compliance with GCP's data handling policies when using these tools.
- Refer to the [DLP API documentation](https://cloud.google.com/dlp/docs) for additional details and best practices.
- For any issues or contributions, feel free to open an issue or submit a pull request.

