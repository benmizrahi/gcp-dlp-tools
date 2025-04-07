import click
from google.cloud import asset_v1
from google.protobuf.json_format import MessageToDict


async def explore_policy_tags(organization, folder, project, dataset, page_size):
    # Create a client
    client = asset_v1.AssetServiceAsyncClient()
    
    # Set the parent resource based on provided parameters
    parent = f"organizations/{organization}"
    if folder !=  None and folder !=   '':
        parent = f"folders/{folder}"
    if project !=  None and project != '':
        parent = f"projects/{project}"
    
    # Build the dataset filter if provided
    dataset_filter = ""
    if dataset !=  None and project !=  None:
        dataset_filter = f"parent:projects/{project}/datasets/{dataset}"
    
    # Initialize request with pagination settings
    request = asset_v1.ListAssetsRequest(
        parent=parent,
        asset_types=["bigquery.googleapis.com/Table"],
        page_size=page_size,  # Number of results per page
        content_type=asset_v1.ContentType.RESOURCE,  # Get the resource representation
    )
    
    # Apply dataset filter if specified
    if dataset_filter:
        request.filter = dataset_filter
    
    # Use the pagination built into the client
    page_iterator = await client.list_assets(request=request)
    
    index = 0
    page_id = 1
    # Process each page of results
    async for page in page_iterator.pages:
        # Process each item in the page
        click.echo(f"working on page {page_id}")
        for response in page.assets:
            data = MessageToDict(response.resource._pb)
            policyFields = []
            # Only yield responses containing policy tags
            for d in data['data']['schema']['fields']:
                if 'policyTags' in d:
                    policyFields.append({'name': d['name'], 'policyTags': d['policyTags']})
                    break
            if len(policyFields) > 0:
                yield index, { 'table':  data['data']['id'],'policyFields': policyFields }
            index = index + 1
        page_id = page_id + 1

async def get_datasets_location(organization, folder, project, dataset=None,page_size=100):
    client = asset_v1.AssetServiceAsyncClient()
    
    # Set the parent resource based on provided parameters
    parent = f"organizations/{organization}"
    if folder is not None and folder != '':
        parent = f"folders/{folder}"
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