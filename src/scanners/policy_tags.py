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

import asyncio
import datetime
import click
import pandas as pd
from google.cloud import storage
from google.cloud import asset_v1
from google.protobuf.json_format import MessageToDict



async def explore_policy_tags(organization, folder_ids, project, dataset, page_size):
    # Create a client
    client = asset_v1.AssetServiceAsyncClient()
    
    # Set the parent resource based on provided parameters
    parent = f"organizations/{organization}"
    if folder_ids != None and len(folder_ids) > 0:
        parent = f"folders/{folder_ids[len(folder_ids)-1]}"
    if project !=  None and project != '':
        parent = f"projects/{project}"
    

    # Initialize request with pagination settings
    request = asset_v1.ListAssetsRequest(
        parent=parent,
        asset_types=["bigquery.googleapis.com/Table"],
        page_size=page_size,  # Number of results per page
        content_type=asset_v1.ContentType.RESOURCE,  # Get the resource representation
    )
 
    # Use the pagination built into the client
    page_iterator = await client.list_assets(request=request)
    total_pages = 0
    async for page in page_iterator.pages:
        total_pages += 1

    click.echo(f"Total pages: {total_pages}")
    if dataset is not None:
        click.echo(f"Filtering by dataset: {dataset}")
    
    page_iterator = await client.list_assets(request=request)
    index = 0
    page_id = 1
    # Process each page of results
    async for page in page_iterator.pages:
        # Process each item in the page
        click.echo(f"working on page {page_id} of {total_pages}")
        for response in page.assets:
            try:
                data = MessageToDict(response.resource._pb)
                if dataset is not None and dataset != '' and data['data']['tableReference']['datasetId'] == dataset:
                    click.echo(f"Skipping {data['data']['id']} - dataset filtred out")
                    continue
                policyFields = []
                # Only yield responses containing policy tags
                for d in data['data']['schema']['fields']:
                    if 'policyTags' in d:
                        policyFields.append({'name': d['name'], 'policyTags': d['policyTags']})
                        break
                if len(policyFields) > 0:
                    yield index, { 'table':  data['data']['id'],'policyFields': policyFields }
                index = index + 1
            except Exception as e:
                click.echo(f" WARNNING: error processing asset: {e}, skipping, asset: {data}")
                continue
        page_id = page_id + 1
        #throttle the requests to avoid hitting the API limits
        await asyncio.sleep(0.6) 

async def explore_policy_tags_export(organization, folder_ids, project, dataset, page_size,bucket_name):
    client = asset_v1.AssetServiceAsyncClient()
    
    # Set the parent resource based on provided parameters
    parent = f"organizations/{organization}"
    if folder_ids != None and len(folder_ids) > 0:
        parent = f"folders/{folder_ids[len(folder_ids)-1]}"
    if project !=  None and project != '':
        parent = f"projects/{project}"
    now =datetime.datetime.now().strftime('%Y%m%d_%H%M%S')
    
    file_name = f"export_{now}.json"
    export_file = f"gs://{bucket_name}/{file_name}"
    # Initialize request with pagination settings
    request = asset_v1.ExportAssetsRequest(
        parent=parent,
        asset_types=["bigquery.googleapis.com/Table"],
        content_type=asset_v1.ContentType.RESOURCE,    
        output_config=asset_v1.OutputConfig(
            gcs_destination=asset_v1.GcsDestination(
                uri=export_file,
            )
        )
    )

    result = await client.export_assets(request)
    click.echo(f"Exporting assets to {export_file} - this may take a few minutes")
    while not await result.done():
        # Wait for the operation to complete
        click.echo(f"still exporting assets to {export_file} - this may take a few minutes")
        await asyncio.sleep(5)
    
    if dataset is not None:
        click.echo(f"Filtering by dataset: {dataset}")

    click.echo(f"Exporting assets to {export_file}")
    # Process the result
    client = storage.Client()
    bucket = client.get_bucket(bucket_name)
    blob = bucket.blob(file_name)
    # Download the file to a local path
    blob.download_to_filename(file_name)
    click.echo(f"Downloaded {file_name} to local path")
    with pd.read_json(file_name, chunksize=1000,lines=True) as reader:
        for chunk in reader:
            # Process each chunk of data
            try:
                for index, data in chunk.iterrows():
                    # Process each row of data
                    data = data.to_dict()['resource']
                    if dataset is not None and dataset != '' and data['data']['tableReference']['datasetId'] == dataset:
                        click.echo(f"Skipping {data['data']['id']} - dataset filtred out")
                        continue
                    policyFields = []
                    # Only yield responses containing policy tags
                    for d in data['data']['schema']['fields']:
                        if 'policyTags' in d:
                            policyFields.append({'name': d['name'], 'policyTags': d['policyTags']})
                            break
                    if len(policyFields) > 0:
                        yield index, { 'table':  data['data']['id'],'policyFields': policyFields }
                    index = index + 1
            except Exception as e:
                click.echo(f" WARNNING: error processing asset: {e}, skipping, asset: {data}")
                continue

    
    
    

async def get_datasets_location(organization, folder_ids, project, dataset=None,page_size=100):
    client = asset_v1.AssetServiceAsyncClient()
    
    # Set the parent resource based on provided parameters
    parent = f"organizations/{organization}"
    if folder_ids != None and len(folder_ids) > 0:
        parent = f"folders/{folder_ids[len(folder_ids)-1]}"
    if project is not None and project != '':
        parent = f"projects/{project}"
    
    request = asset_v1.QueryAssetsRequest(
        parent=parent,
        statement="""
            SELECT
                resource.location AS location,
                COUNT(1) AS count
            FROM
                bigquery_googleapis_com_Dataset
            GROUP BY
                location
            HAVING count(1) > 0
            ORDER BY
                count DESC;
        """,
        page_size=page_size
    )
    
    query_results = await client.query_assets(request)
    if not query_results.done:
        raise Exception(f"Error {query_results.error}")
    
    index = 0
    click.echo(f"Processing results")
    for row in query_results.query_result.rows:
        for k, v in row.items():
           yield index, v[0]['v']
           index = index + 1