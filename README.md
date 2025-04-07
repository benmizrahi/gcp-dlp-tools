# Google Cloud DLP

This repository contains a script for analyzing data using Google Cloud Platform's Data Loss Prevention (DLP) API. The script helps identify sensitive information in your datasets.

## Prerequisites

1. **Google Cloud Project**: Ensure you have a GCP project set up.
2. **DLP API Enabled**: Enable the DLP API in your GCP project.
3. **Service Account**: Create a service account with the necessary permissions and download the JSON key file.
4. **Python Environment**: Install Python 3.7 or later.

## Setup

1. Clone the repository:
    ```bash
      git clone https://github.com/benmizrahi/gcp-dlp-tools.git
      cd gcp-dlp-tools
    ```

2. Install dependencies:
    ```bash
    pip install -r requirements.txt
    ```

3. Create service sccount:
```bash
  gcloud iam service-accounts create dlp-scripts --description="A DLP scripts service account"  --display-name="dlp-scripts-helper"
  ```

  ```bash
  gcloud auth application-default login --impersonate-service-account dlp-scripts@lithe-joy-444308-t4.iam.gserviceaccount.com
  ```
4. 

3. Set the `GOOGLE_APPLICATION_CREDENTIALS` environment variable to point to your service account key file:
    ```bash
    export GOOGLE_APPLICATION_CREDENTIALS="/path/to/your-service-account-key.json"
    ```

## Usage
2. Run the script:
    ```bash
    python main.py --help
    ```

   Below are examples of how to use the script for different operations:

   - **Find Policy Tags**: Scan a dataset for policy tags and export the results to a specified output path.
     ```bash
     python main.py find-policy-tags --scan-path gcp://<org-id>/<folder_id>/<project_id>/<dataset_id> --output-path bq://<project_id>/<dataset_name>/<table_name>
     ```

   - **Get Sample Scan Size**: Retrieve the sample size of a scan and export the results to a BigQuery table.
     ```bash
     python main.py get-sample-scan-size --scan-path gcp://<org-id>/<folder_id>/<project_id>/<dataset_id> --output-path bq://<project_id>/<dataset_name>/<table_name>
     ```

   - **SDP Scan**: Perform a Sensitive Data Protection (SDP) scan on a specific dataset or table and save the results to a BigQuery table.
     ```bash
     python main.py sdp-scan --project-id <dlp-project> --dataset-id <dataset_where_view_exists> --filter-path gcp://<org-id>/<project_id>/<dataset_name>/<table_name> --output-path bq://<project_id>/<dataset_name>/<output_results_table_name>
     ```

   Replace placeholders (e.g., `<org-id>`, `<project_id>`, `<dataset_id>`, etc.) with your specific values.

## Notes

- Ensure your dataset complies with GCP's data handling policies.
- Refer to the [DLP API documentation](https://cloud.google.com/dlp/docs) for additional details.
