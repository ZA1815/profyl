import json
import threading
from typing import Any
from profyl.core.abstractions.cache import Cache
from profyl.core.abstractions.registry import DataSourceType, Registry
from mcp.server.fastmcp import FastMCP

class Manager:
    def __init__(self, registry: Registry, cache: Cache) -> None:
        self.reg = registry
        self.cache = cache
    
    def register_dataset(self, key: str, source: DataSourceType, reference: str) -> None:
        self.reg.add(source, reference, key)
    
    def load_dataset(self, key: str):
        entry = self.reg.get(key)
        data_source = entry.source
        source_num = entry.source_num
        
        for sheet in range(data_source.get_sheet_count()):
            self.cache.set_headers(source_num, sheet, data_source.read_headers(sheet))
            self.cache.add_unedited_row_indices(source_num, sheet, set(range(data_source.get_row_count(sheet))))
            for row in range(data_source.get_row_count(sheet)):
                row_data = data_source.read_row(sheet, row)
                self.cache.set_row(source_num, sheet, row, row_data)
    
    def remove_dataset(self, key: str):
        entry = self.reg.get(key)
        self.cache.remove_dataset(entry.source_num)
        self.reg.remove(key)
    
    def list_datasets(self) -> list[str]:
        entries = self.reg.get_all()
        datasets = []
        for key, entry in entries:
            datasets.append(f"Dataset '{key}':\n".encode("utf-8"))
            datasets.append(f"Source: {str(entry.source)}\n".encode("utf-8"))
            datasets.append(f"Reference: {entry.reference}\n".encode("utf-8"))
            datasets.append(f"Timestamp: {entry.timestamp}\n".encode("utf-8"))
            datasets.append(f"Status: {entry.status}\n".encode("utf-8"))
            
        return datasets
    
    def build_schema_map_payload(self, num_samples: int) -> str:
        schema_map_list = []
        for (key, entry) in self.reg.get_all():
            entry_schema = entry.source.get_schema_map_payload(num_samples)
            entry_schema["Dataset Name"] = key
            schema_map_list.append(entry_schema)
        
        return json.dumps(schema_map_list)
    
    def start_mcp(self):
        mcp = FastMCP("profyl", json_response=True)
        
        @mcp.prompt()
        def generate_schema_mapping(num_samples: int) -> Any:
            payload = self.build_schema_map_payload(num_samples)
            
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
        def value_cross_ref(dataset: int, sheet: int, col: int, val: str) -> bool:
            """Check if a value exists in another sheet's column (foreign key validation)."""
            return self.cache.value_cross_ref(dataset, sheet, col, val)
            
        threading.Thread(target=mcp.run, daemon=True).start()