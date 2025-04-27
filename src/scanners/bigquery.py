import click
from google.cloud import bigquery

async def get_table_policy_tags(client,project_id, dataset_id, table_id):
    """
    Retrieves metadata for a specific BigQuery table.

    Args:
        project_id (str): The ID of the project containing the table.
        dataset_id (str): The ID of the dataset containing the table.
        table_id (str): The ID of the table.

    Returns:
        dict: A dictionary containing the table's metadata, or None if the table does not exist.
    """
    try:
        table_ref = bigquery.TableReference.from_string(f"{project_id}.{dataset_id}.{table_id}")
        table = client.get_table(table_ref)
        fields_with_policy_tags = []
        for field in table.schema:
            if field.policy_tags:
                tags = [tag for tag in field.policy_tags.names if tag]
                fields_with_policy_tags.append({
                    "name": field.name,
                    "policy_tags": tags,
                    "extra":  field.policy_tags
                })
        return  fields_with_policy_tags
    except Exception as e:
        click.echo(f"Error retrieving table metadata: {e}, skipping table {project_id}.{dataset_id}.{table_id}")
        return []

