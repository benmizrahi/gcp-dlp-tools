#  Copyright 2025 Google LLC

#  Licensed under the Apache License, Version 2.0 (the "License");
#  you may not use this file except in compliance with the License.
#  You may obtain a copy of the License at

#     https://www.apache.org/licenses/LICENSE-2.0

#  Unless required by applicable law or agreed to in writing, software
#  distributed under the License is distributed on an "AS IS" BASIS,
#  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
#  See the License for the specific language governing permissions and
#  limitations under the License.


import pandas_gbq
import functools
import concurrent
import pandas as pd

import asyncclick as click
from src.scanners import policy_tags
from src.outputs import output_factory
from google.cloud import bigquery
from src.scanners.bigquery import get_table_policy_tags

from src.utils import tools


@click.group()
def cli():
    pass

@cli.command()
@click.option('--output-path', required=True, help="Output file path can be Google Cloud Storage - have to start with gs:// or bigquery bq:// ")
@click.option('--scan-path', required=True, help='Example: gcp://organization-id/<organization-id>/folder-id/<folder-id>/project-id/<project-id>/dataset-id/<dataset-id>')
@click.option('--page-size', default=100, help='Number of results per page')
async def find_policy_tags(scan_path, output_path,page_size):
    try:
        organization_id, folder_ids, project_id, dataset_id = tools.parse_scan_path(scan_path)
        click.echo(f'Organization ID: {organization_id}')
        if folder_ids: click.echo(f'Folder ID: {folder_ids}')
        if project_id: click.echo(f'Project ID: {project_id}')
        if dataset_id: click.echo(f'Dataset ID: {dataset_id}')
    except ValueError as e:
        click.echo(str(e))
        return  
    output_func = await output_factory.get_output(output_path)
    click.echo("start scanning for policyTags...")
    client = bigquery.Client(project=project_id)
    df = pd.DataFrame()
    async for index, data in policy_tags.explore_policy_tags(organization=organization_id, folder_ids=folder_ids, project=project_id, dataset=dataset_id,page_size=page_size):
        [project,dataset_table] =  data['table'].split(":")
        [dataset,table] = dataset_table.split(".")
        error = ''
        extended_columns = []    
        try:
            columns_policy_tags = await get_table_policy_tags(client=client,project_id=project,dataset_id=dataset, table_id=table)
            for column in columns_policy_tags:
                extended_columns.append({
                    'column_name': column['name'],
                    'policy_tags': column['policy_tags'],
                })
        except Exception as e:
            click.echo(f"Error getting table metadata: {e}")
            error = str(e)
        
        df = await output_func(index, { 'project': project, 'dataset': dataset, 'table': table, 'error': error, 'results' : extended_columns }  , df)
    await output_func.close(df)
    click.echo(f"done dumping results to output...")

@cli.command()
@click.option('--scan-path', required=True, help='Example: gcp://organization-id/<organization-id>/folder-id/<folder-id>/project-id/<project-id>/dataset-id/<dataset-id>')
@click.option('--output-path', default=None, help="Output bigquery view bq://<project-id>/<dataset-id>/<view-id>")
@click.option('--page-size', default=100, help='Number of results per page')
async def get_sample_scan_size(scan_path, output_path,page_size):
    try:
        organization_id, folder_ids, project_id, dataset_id = tools.parse_scan_path(scan_path)
        click.echo(f'Organization ID: {organization_id}')
        if folder_ids: click.echo(f'Folder ID: {folder_ids}')
        if project_id: click.echo(f'Project ID: {project_id}')
        if dataset_id: click.echo(f'Dataset ID: {dataset_id}')
    except ValueError as e:
        click.echo(str(e))
        return

    bq_path = output_path.replace('bq://', '')
    project, dataset, table = bq_path.split('/')
    bigquery_client = bigquery.Client()
    async for index, data in policy_tags.get_datasets_location(organization_id, folder_ids, project_id,page_size=page_size):
        query = f"""SELECT
                "{organization_id}" AS organization_id,
                project_id,
                table_schema AS table_catalog,
                table_name,
                table_type,
                total_rows,
                total_logical_bytes,
                total_physical_bytes,
                ROUND((total_logical_bytes + total_physical_bytes) / (1024 * 1024 * 1024)) AS total_storage_gb_rounded,
                IF(total_rows > 0, ROUND((total_logical_bytes + total_physical_bytes) / total_rows / (1024 * 1024 * 1024)), 0) AS average_gb_size_rounded
            FROM
                `region-{data}`.INFORMATION_SCHEMA.TABLE_STORAGE_BY_ORGANIZATION 
            WHERE table_type = 'BASE TABLE' and DELETED = false
            \n"""
        query_job = bigquery_client.query(query)
        results = query_job.result().to_dataframe()
        if results.empty:
            click.echo(f"No data found for {data}")
            continue
        pandas_gbq.to_gbq(results,project_id=project,destination_table=f'{dataset}.{table}_{data}',if_exists='replace')
    
    bigquery_client.query(f"CREATE OR REPLACE VIEW `{project}.{dataset}.global_tables_view` AS SELECT * FROM `{project}.{dataset}.{table}_*`").result()
    click.echo(f"created view `{project}.{dataset}.global_tables_view`")
    click.echo(f"you can run 'select * from {project}.{dataset}.global_tables_view' to see the results")
    click.echo(f"done dumping results to output...")

@cli.command()
@click.option('--project-id', default=None, help='project id where the view is stored and dlp job will run')
@click.option('--dataset-id', default=None, help='the dataset id where the view is stored')
@click.option('--filter-path', default=None, help='Example: bq://<organization-id>/<project-id>/<dataset-id>/<table-id>')
@click.option('--output-path', default=None, help="Output bigquery view bq://<project-id>/<dataset-id>/<view-id>")
@click.option('--rows-limit-percent', default=None, help='how many rows to scan, default is none')
@click.option('--rows-limit', default=100, help='how many rows to scan, default is 100')
@click.option('--info-types', default=None, help='.txt path to the file with info types to scan')
async def sdp_scan(project_id, dataset_id, filter_path, rows_limit, rows_limit_percent, output_path,info_types):
    scan_path_parts = filter_path.split('/')
    filter_organization_id = scan_path_parts[2]
    filter_project_id = None
    filter_dataset_id = None
    filter_table_id = None
    
    if len(scan_path_parts) > 3:
        filter_project_id = scan_path_parts[3]
    if len(scan_path_parts) > 4:
        filter_dataset_id = scan_path_parts[4]
    if len(scan_path_parts) > 5:
        filter_table_id = scan_path_parts[5]
     
    if rows_limit and rows_limit_percent:
        raise ValueError("You can only specify one of --rows-limit or --rows-limit-percent")
    
    if info_types:
        with open(info_types, 'r') as f:
            info_types = f.read().splitlines()
            
    bigquery_client = bigquery.Client()
    query = f"""select * from `{project_id}.{dataset_id}.global_tables_view` """
    where_clause = []
    if filter_organization_id:
        where_clause.append(f"organization_id = '{filter_organization_id}'")
    if filter_project_id:
        where_clause.append(f"project_id = '{filter_project_id}'")
    if filter_dataset_id:
        where_clause.append(f"table_catalog = '{filter_dataset_id}'")
    if filter_table_id:
        where_clause.append(f"table_name = '{filter_table_id}'")
    if where_clause:
        query += " where " + " and ".join(where_clause)
    
    ## testing only
    query += " limit 1"
    
    click.echo(f"running query: {query}")
    query_job = bigquery_client.query(query)
    results = query_job.result().to_dataframe()
    if results.empty:
        click.echo(f"No data found in the view {project_id}.{dataset_id}.global_tables_view")
        return

    click.echo(f"found {len(results)} results in the view {project_id}.{dataset_id}.global_tables_view")
    click.echo(f"start scanning for sdp scan...")
    
    # Convert the results to a list of dictionaries
    data = results.to_dict(orient='records')
    
    # Using partial to fix the additional arguments
    process_func = functools.partial(
        tools.process_table, 
        rows_limit_val=rows_limit,
        rows_limit_percent_val=rows_limit_percent,
        dlp_project_id=project_id,
        info_types=info_types
    )
    
    # Collect all results
    all_results = []
    
    # Use ProcessPoolExecutor with max 10 workers
    with concurrent.futures.ProcessPoolExecutor(max_workers=10) as executor:
        # Submit all tasks
        future_to_row = {executor.submit(process_func, row): row for row in data}
        
        # Process results as they complete
        for future in concurrent.futures.as_completed(future_to_row):
            row = future_to_row[future]
            try:
                result = future.result()
                if result and result.get('success', False):
                    all_results.extend(result.get('data', []))
                    click.echo(f"Successfully processed {row['project_id']}.{row['table_catalog']}.{row['table_name']}")
                else:
                    error = result.get('error', 'No data found')
                    click.echo(f"No results for {row['project_id']}.{row['table_catalog']}.{row['table_name']}: {error}")
            except Exception as e:
                click.echo(f"Error processing {row['project_id']}.{row['table_catalog']}.{row['table_name']}: {str(e)}")
    
    # Create DataFrame from all results
    output_df = pd.DataFrame(all_results) if all_results else pd.DataFrame()
        
    if not output_df.empty:
        click.echo(f"Found a total of {len(output_df)} rows across all tables")
        output_func = await output_factory.get_output(output_path)
        await output_func.close(output_df)
    else:
        click.echo(f"No results found in any of the tables with sdp scan")
    
    click.echo(f"done dumping results to output...")
    

if __name__ == '__main__':
    cli()
