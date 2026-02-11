import threading
from typing import Any
from profyl.core.abstractions.cache import Cache
from profyl.core.abstractions.registry import DataSourceType, Registry
from mcp.server.fastmcp import FastMCP

class Manager:
    def __init__(self, registry: Registry, cache: Cache) -> None:
        self.reg = registry
        self.cache = cache
        self.ds_count = 0
    
    def register_dataset(self, source: DataSourceType, reference: str, key: str):
        self.reg.add(source, reference, key)
    
    def load_dataset(self, key: str):
        entry = self.reg.get(key)
        data_source = entry.source
        
        for sheet in range(data_source.get_sheet_count()):
            self.cache.set_headers(self.ds_count, sheet, data_source.read_headers(sheet))
            self.cache.add_unedited_row_indices(self.ds_count, sheet, set(range(data_source.get_row_count(sheet))))
            for row in range(data_source.get_row_count(sheet)):
                row_data = data_source.read_row(sheet, row)
                self.cache.set_row(self.ds_count, sheet, row, row_data)
        
        self.ds_count += 1
    
    def start_mcp(self):
        mcp = FastMCP("profyl", json_response=True)
        
        @mcp.prompt()
        def generate_schema_mapping(num_samples: int) -> Any:
            payload = self.data_source.get_schema_map_payload(num_samples)
            
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
            return self.cache.get_unique_vals(dataset, sheet, col)
            
        @mcp.tool()
        def get_sample_rows(dataset: int, sheet: int, num_rows: int) -> list[list[Any]]:
            """Get unedited sample rows in a sheet based on the designated number."""
            return self.cache.get_sample_rows(dataset, sheet, num_rows)
            
        @mcp.tool()
        def get_row(dataset: int, sheet: int, row: int) -> list[Any]:
            """Get a row in a sheet based on index."""
            return self.cache.get_row(dataset, sheet, row)
            
        @mcp.tool()
        def get_col(dataset: int, sheet: int, col: int) -> list[Any]:
            """Get a column in a sheet based on index."""
            return self.cache.get_col(dataset, sheet, col)
            
        @mcp.tool()
        def get_cell(dataset: int, sheet: int, row: int, col: int) -> Any:
            """Get a cell in a sheet based on row index and column index."""
            return self.cache.get_cell(dataset, sheet, row, col)
            
        @mcp.tool()
        def value_cross_ref(dataset: int, sheet: int, col: int, val: int) -> bool:
            """Check if a value exists in another sheet's column (foreign key validation)."""
            return self.cache.value_cross_ref(dataset, sheet, col, val)
            
        threading.Thread(target=mcp.run, daemon=True).start()