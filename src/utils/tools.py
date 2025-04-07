
import asyncio

from src.scanners import inspection


def parse_scan_path(scan_path):
    # Validate and parse scan path
    scan_path_parts = scan_path.split('/')
    
    # Validate basic format
    if not scan_path.startswith("gcp://"):
        raise ValueError('Invalid scan path format. It should start with gcp://')
    
    # Extract available components
    if len(scan_path_parts) < 3:
        raise ValueError('Organization ID is required in the scan path')
    
    organization_id = scan_path_parts[2]
    folder_id = scan_path_parts[3] if len(scan_path_parts) > 3 else None
    project_id = scan_path_parts[4] if len(scan_path_parts) > 4 else None
    dataset_id = scan_path_parts[5] if len(scan_path_parts) > 5 else None
    
    return organization_id, folder_id, project_id, dataset_id

def process_table(row_dict, rows_limit_val, rows_limit_percent_val,dlp_project_id,info_types):
    # Create a new event loop for this process
    loop = asyncio.new_event_loop()
    asyncio.set_event_loop(loop)
    
    async def run_inspection():
        p_id = row_dict['project_id']
        table_schema = row_dict['table_catalog']
        table_name = row_dict['table_name']

        try:
            results = await inspection.inpect_bigquery_table(
               dlp_project_id, p_id, table_schema, table_name, rows_limit_val, rows_limit_percent_val,info_types_input=info_types
            )
            
            if results:
                print(f"found {len(results)} results in the table {p_id}.{table_schema}.{table_name} with sdp scan")
                return {'success': True, 'data': results}
            else:
                print(f"No results found in the table {p_id}.{table_schema}.{table_name} with sdp scan")
                return {'success': False}
        except Exception as e:
            print(f"Error inspecting table {p_id}.{table_schema}.{table_name}: {str(e)}")
            return {'success': False, 'error': str(e)}
    
    try:
        # Run the async function and get results
        result = loop.run_until_complete(run_inspection())
        loop.close()
        return result
    except Exception as e:
        return {'success': False, 'error': str(e)}

