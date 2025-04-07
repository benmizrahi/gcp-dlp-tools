import time
import click
import google.auth
from google.cloud import dlp_v2
from google.protobuf.json_format import MessageToDict

async def find_info_type(dlp_project_id, info_type_name):
    credentials, _ = google.auth.default(quota_project_id=dlp_project_id)
    # Initialize the DLP client
    client = dlp_v2.DlpServiceClient(credentials=credentials)
    # Get the list of stored info types
    response = client.list_stored_info_types(
        request=dlp_v2.ListStoredInfoTypesRequest(
            parent=f"projects/{dlp_project_id}/locations/global",
        )
    )
    # Iterate through the stored info types and find the one with the matching name
    for stored_info_type in response.stored_info_types:
        message = MessageToDict(stored_info_type._pb)
        if message['name'] == info_type_name:
            # Convert the StoredInfoType to a dictionary for custom_info_types
            return {
                "info_type": {"name": message['currentVersion']['config']['displayName']},
            }
    # If no matching info type is found, return None
    return None
    
async def inpect_bigquery_table(dlp_project_id,project_id, dataset_id, table_id,rows_limit, rows_limit_precent,info_types_input,sample_method="RANDOM_START"):
    
    """
    Inspect a BigQuery table for sensitive data using Google Cloud DLP API.
    Args:
        project_id (str): The GCP project ID.
        dataset_id (str): The BigQuery dataset ID.
        table_id (str): The BigQuery table ID.
        rows_limit (int): The maximum number of rows to scan.
        rows_limit_precent (int): The percentage of rows to scan.
        sample_method (str): The sampling method to use. Default is "RANDOM_START".
    Returns:
        list: A list of findings for sensitive data.
    """
    credentials, _ = google.auth.default(quota_project_id=dlp_project_id)
    # Initialize the DLP client
    client = dlp_v2.DlpServiceClient(credentials=credentials)
    
    inspect_config = {
        "min_likelihood": dlp_v2.Likelihood.LIKELY,
        "limits": {"max_findings_per_request": 5},
        "include_quote": True,
    }
    
    if info_types_input:
        inspect_config["info_types"] = [{ "name": info_type} for info_type in info_types_input if not info_type.startswith("REGEX:")]     
    
    if info_types_input:
        # If info_types_input is provided, add custom info types
        custom_info_types = []
        for info_type in info_types_input:
            if info_type.startswith("REGEX_EXPRESSION:"):
                regex = info_type.split("REGEX_EXPRESSION:")[1]
                custom_info_types.append({
                    "info_type": {"name": regex},
                    "regex": {"pattern": regex},
                })
        inspect_config["custom_info_types"] = custom_info_types
    
    
    # Define the BigQuery table reference
    table_reference = {
        "project_id": project_id,
        "dataset_id": dataset_id,
        "table_id": table_id,
    }

    storage_config = {
        "big_query_options": {
            "table_reference": table_reference,
            "rows_limit": rows_limit,
            "sample_method": sample_method,
            "rows_limit_percent": rows_limit_precent,
            "sample_method": "RANDOM_START",
        }
    }
    
    inspect_job = {
        "inspect_config": inspect_config,
        "storage_config": storage_config,
    }

    # Convert the project id into full resource ids.
    parent = f"projects/{project_id}/locations/global"

    operation = client.create_dlp_job(
        request={"parent": parent, "inspect_job": inspect_job}
    )
    print(f"Created job: {operation.name}")
    print("Waiting for job to complete...")
    
    while not operation.state == dlp_v2.DlpJob.JobState.DONE:
        click.echo(f"Job {operation.name},scanning {project_id}.{dataset_id}.{table_id} - is still running...")
        time.sleep(5)
        operation = client.get_dlp_job(name=operation.name)
    
    if operation.state == dlp_v2.DlpJob.JobState.DONE:
        click.echo(f"Job {operation.name},scanning {project_id}.{dataset_id}.{table_id} - done.")
        if len(operation.inspect_details.result.info_type_stats) > 0:
            result = {
                "project_id": project_id,
                "dataset_id": dataset_id,
                "table_id": table_id,
                "job_name": operation.name,
                "info_type": [],
            }
            for info_type in operation.inspect_details.result.info_type_stats:
                result["info_type"].append({"info_type": info_type.info_type.name,
                                            "count": info_type.count})
                
            return result
        else:
            print("No findings found.")
            return []