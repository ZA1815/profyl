from profyl.abstractions.data_source import DataSource
from pymongo import MongoClient
import os
from bson.objectid import ObjectId

class MongoDataSource(DataSource):
    def __init__(self, db_name: str = "test", collection_name: str = "datasets") -> None:
        mongo_uri = os.getenv("MONGO_URI", "mongodb://localhost:27017/")
        try:
            self.client = MongoClient(mongo_uri)
            self.client.admin.command("ping")
        except Exception as e:
            print(f"Connection failed: {e}")
            return
        
        self.db = self.client[db_name]
        self.collection = self.db[collection_name]
        self.data = {}
    
    def load(self, source: str):
        self.data = self.collection.find_one({"_id": ObjectId(source)})
        try:
            sheets = self.data["sheets"]
        except KeyError:
            print("KeyError: 'sheets' doesn't exist at the top level.")
            return
        
        try:
            for sheet in sheets:
                sheet["headers"]
                sheet["rows"]
        except KeyError:
            print("KeyError: 'headers' or 'rows' keys don't exist in every item in 'sheets'.")
            return
    
    def read_row(self, sheet: int, row: int) -> list[str]:
        return self.data["sheets"][sheet]["rows"][row]
        
    def read_headers(self, sheet) -> list[str]:
        return self.data["sheets"][sheet]["headers"]
        
    def read_col(self, sheet: int, col: int) -> tuple[str, list[str]]:
        header = self.data["sheets"][sheet]["headers"][col]
        col_data = []
        for row in self.data["sheets"][sheet]["rows"]:
            col_data.append(row[col])
            
        return (header, col_data)
        
    def get_sheet_count(self) -> int:
        return len(self.data["sheets"])
        
    def get_row_count(self, sheet: int) -> int:
        return len(self.data["sheets"][sheet]["rows"])
        
    def get_col_count(self, sheet: int) -> int:
        return len(self.data["sheets"][sheet]["headers"])