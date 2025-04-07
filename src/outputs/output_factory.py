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
        