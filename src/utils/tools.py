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

from src.scanners import inspection



def parse_scan_path(scan_path):
    if not scan_path.startswith("gcp://"):
        raise ValueError("Invalid scan path format. It should start with gcp://")
    
    scan_path_parts = scan_path.split('/')
    if len(scan_path_parts) < 3 or scan_path_parts[2] != "organization-id":
        raise ValueError("Invalid scan path format. Missing organization-id.")
    
    #remove empty strings
    scan_path_parts =  [x for x in scan_path_parts if x != '']
    
    organization_id = scan_path_parts[2]
    folder_ids = []
    project_id = None
    dataset_id = None
    
    i = 3
    while i < len(scan_path_parts):
        if scan_path_parts[i] == "folder-id":
            if i + 1 < len(scan_path_parts):
                folder_ids.append(scan_path_parts[i + 1])
                i += 2
            else:
                raise ValueError("Invalid scan path format. Missing folder-id value.")
        elif scan_path_parts[i] == "project-id":
            if i + 1 < len(scan_path_parts):
                project_id = scan_path_parts[i + 1]
                i += 2
            else:
                raise ValueError("Invalid scan path format. Missing project-id value.")
        elif scan_path_parts[i] == "dataset-id":
            if i + 1 < len(scan_path_parts):
                dataset_id = scan_path_parts[i + 1]
                i += 2
            else:
                raise ValueError("Invalid scan path format. Missing dataset-id value.")
        else:
            raise ValueError(f"Unexpected path component: {scan_path_parts[i]}")
    
    return [organization_id,folder_ids,project_id, dataset_id]
    
  
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

