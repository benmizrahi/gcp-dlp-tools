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

import pandas as pd
import pandas_gbq
from typing import Dict, Any

async def pandas_output(path: str):
    """
    Creates a file based on the given path and returns a write function that accepts data.
    
    Args:
        path (str): Output path starting with file://
    
    Returns:
        function: An async function that writes data to the specified output
    """
    # Determine file format
    is_local = path.lower().startswith("file://") 
    is_csv = path.lower().endswith('.csv')
    is_jsonl = path.lower().endswith('.jsonl') or path.lower().endswith('.json')
    
    is_bq = path.lower().startswith("bq://")
    is_gcs = path.lower().startswith("gcs://") 
    
    if (is_local or is_gcs) and not (is_csv or is_jsonl):
        raise ValueError(f"Unsupported file format. Path must end with .csv, .json, or .jsonl")
    
    async def write_data(index:int, data: Dict[str, Any],df: pd.DataFrame ):
        if index == 0:
            return pd.DataFrame.from_dict([data])
        else:
            return  pd.concat([df, pd.DataFrame([data])], ignore_index=True)
    
    async def close(df: pd.DataFrame):
        if is_local and is_csv:
            df.to_csv(path.replace('file://',''), index=False) 
        elif is_local and is_jsonl:
            df.to_json(path_or_buf=path.replace('file://',''),orient='records', lines=True)
        elif is_bq:
            bq_path = path.replace('bq://','')
            project, dataset, table = bq_path.split('/')
            pandas_gbq.to_gbq(df,project_id=project,destination_table=f'{dataset}.{table}',if_exists='replace')
        elif is_gcs:
            gcs_path = path.replace('gcs://','').split('/')
            bucket_name = gcs_path[0]
            blob_name = '/'.join(gcs_path[1,])
            if is_csv:
                df.to_csv(f'gs://{bucket_name}/{blob_name}')
            elif is_jsonl:
                df.to_json(f'gs://{bucket_name}/{blob_name}',orient='records', lines=True)
    
    # Add close method to the write function
    write_data.close = close
    return write_data