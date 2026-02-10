from typing import Any
from mcp.server.fastmcp import FastMCP
from profyl.data_sources.excel import ExcelDataSource
from profyl.data_sources.mongo import MongoDataSource
from profyl.caches.redis_cache import RedisCache
import os

mcp = FastMCP("profyl", json_response=True)

data_source_type = os.getenv("DATA_SOURCE_TYPE")
if data_source_type == "excel":
    data_source = ExcelDataSource()
    # Add an ENV VAR for passing in the file path
    
elif data_source_type == "mongo":
    collection_name = os.getenv("MONGO_COLLECTION_NAME")
    if collection_name is not None:
        data_source = MongoDataSource(collection_name=collection_name)
    else:
        data_source = MongoDataSource()
    
    document_id = os.getenv("MONGO_DOCUMENT_ID")
    if document_id is not None:
        data_source.load(document_id)
    else:
        raise TypeError("No document ID provided.")
else:
    raise ValueError(f"{data_source_type} is not a supported data source type.")

cache_type = os.getenv("CACHE_TYPE")
if cache_type == "redis":
    cache = RedisCache()
    cache.populate_from(data_source, 0)
else:
    raise ValueError(f"{cache_type} is not a supported cache type.")

# THIS SCHEMA MAPPING IS WRONG, WE HAVE TO COMPARE BETWEEN SHEETS INSIDE DATASETS, NOT BETWEEN SHEETS INSIDE THE SAME DATASET
@mcp.prompt()
def generate_schema_mapping(num_samples: int) -> Any:
    payload = data_source.get_schema_map_payload(num_samples)
    
    return f"""I need you to generate a schema mapping based on the data below. 
    Please analyze the field names and sample values to propose relationships.
    All you need to do is normalize the columns between sheets into a single
    canonical mapped column schema. Nothing else should be said or done.
    
    DATA PAYLOAD:
    {payload}
    """

@mcp.tool()
def get_unique_vals(dataset: int, sheet: int, col: int) -> set[Any]:
    """Get the unique values from a column in a sheet."""
    return cache.get_unique_vals(dataset, sheet, col)

@mcp.tool()
def get_sample_rows(dataset: int, sheet: int, num_rows: int) -> list[list[Any]]:
    """Get unedited sample rows in a sheet based on the designated number."""
    return cache.get_sample_rows(dataset, sheet, num_rows)

@mcp.tool()
def get_row(dataset: int, sheet: int, row: int) -> list[Any]:
    """Get a row in a sheet based on index."""
    return cache.get_row(dataset, sheet, row)
    
@mcp.tool()
def get_col(dataset: int, sheet: int, col: int) -> list[Any]:
    """Get a column in a sheet based on index."""
    return cache.get_col(dataset, sheet, col)

@mcp.tool()
def get_cell(dataset: int, sheet: int, row: int, col: int) -> Any:
    """Get a cell in a sheet based on row index and column index."""
    return cache.get_cell(dataset, sheet, row, col)
    
@mcp.tool()
def value_cross_ref(dataset: int, sheet: int, col: int, val: int) -> bool:
    """Check if a value exists in another sheet's column (foreign key validation)."""
    return cache.value_cross_ref(dataset, sheet, col, val)

if __name__ == "__main__":
    mcp.run()