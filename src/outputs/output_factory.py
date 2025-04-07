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

import click
from .pandas_output import pandas_output 


async def get_output(output_path):
    """
    Process different types of output paths: file://, gs://, or bq://
    
    Args:
        output_path (str): Path string that starts with file://, gs://, or bq://
        
    Returns:
        Appropriate handler for the given path type
    """
    if output_path.startswith("file://"):
        click.echo(f"writing output - using local file {output_path}")
        # Handle local file output
        return await pandas_output(output_path)
        
    elif output_path.startswith("gs://"):
        click.echo(f"writing output - using google storage file {output_path}")
        # Handle Google Cloud Storage output
        return await pandas_output(output_path)
        
    elif output_path.startswith("bq://"):
        click.echo(f"writing output - using google bigquery file {output_path}")
        # Handle BigQuery output
        return await pandas_output(output_path)
        