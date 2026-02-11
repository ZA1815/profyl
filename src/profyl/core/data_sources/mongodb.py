from typing import Any
from profyl.core.abstractions import DataSource
from profyl.core.abstractions.data_source import SheetData
from pymongo import MongoClient, ReplaceOne
import os

class MongoDBDataSource(DataSource):
    def __init__(self, collection_name: str = "datasets") -> None:
        mongo_uri = os.getenv("MONGO_URI", "mongodb://localhost:27017/test")
        try:
            self.client = MongoClient(mongo_uri)
            self.client.admin.command("ping")
        except Exception as e:
            print(f"Connection failed: {e}")
            return
        
        self.db = self.client.get_database()
        self.collection = self.db[collection_name]
        self.data = {}
        self.source = ""
    
    def load(self, source: str):
        self.data = self.collection.find_one({"_id": source})
        self.source = source
        try:
            sheets = self.data["sheets"]
        except KeyError:
            print("KeyError: 'sheets' doesn't exist at the top level.")
            return
        
        try:
            for sheet in sheets:
                sheet["name"]
                sheet["headers"]
                sheet["rows"]
        except KeyError:
            print("KeyError: 'name', 'headers' or 'rows' keys don't exist in every item in 'sheets'.")
            return
    
    def get_schema_map_payload(self, num_samples: int) -> dict[str, Any]:
        payload = {
            "Dataset Name": "",
            "Original Headers": {},
            "Sample Data": {}
        }
        for sheet_idx, _ in enumerate(self.data["sheets"]):
            payload["Original Headers"][f"Sheet {sheet_idx + 1}"] = self.read_headers(sheet_idx)
            row_count = self.get_row_count(sheet_idx)
            if row_count < num_samples:
                for idx in row_count:
                    payload["Sample Data"].setdefault(f"Sheet {sheet_idx + 1}", {})[f"Row {idx + 1}"] = self.read_row(sheet_idx, idx)
            else:
                sample_indices = [i * row_count // num_samples for i in range(num_samples)]
                for idx in sample_indices:
                    payload["Sample Data"].setdefault(f"Sheet {sheet_idx + 1}", {})[f"Row {idx + 1}"] = self.read_row(sheet_idx, idx)
        
        return payload
    
    def read_row(self, sheet: int, row: int) -> list[str]:
        return self.data["sheets"][sheet]["rows"][row]
        
    def read_headers(self, sheet: int) -> list[str]:
        return self.data["sheets"][sheet]["headers"]
        
    def read_col(self, sheet: int, col: int) -> tuple[str, list[str]]:
        header = self.data["sheets"][sheet]["headers"][col]
        col_data = []
        for row in self.data["sheets"][sheet]["rows"]:
            col_data.append(row[col])
            
        return (header, col_data)
    
    def save(self, data: list[SheetData]):
        self.data.clear()
        self.data["sheets"] = []
        
        for sheet in data:
            self.data["sheets"].append({"name": sheet.name, "headers": sheet.headers, "rows": sheet.rows})
        
        ReplaceOne({"_id": self.source}, self.data, upsert=True)
            
    def get_sheet_count(self) -> int:
        return len(self.data["sheets"])
        
    def get_row_count(self, sheet: int) -> int:
        return len(self.data["sheets"][sheet]["rows"])
        
    def get_col_count(self, sheet: int) -> int:
        return len(self.data["sheets"][sheet]["headers"])