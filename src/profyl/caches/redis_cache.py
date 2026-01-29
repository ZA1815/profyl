from abstractions.cache import Cache
import redis
import json

class RedisCache(Cache):
    def __init__(self, host="localhost", port=6379) -> None:
        self.r = redis.Redis(host=host, port=port, db=0, decode_responses=True)
        if not self.r.ping():
            raise ConnectionError(f"Failed to connect to Redis on host='{host}', port='{port}'")
        
    def get_unique_vals(self, dataset: int, sheet: int, col: int) -> set[str]:
        return super().get_unique_vals(dataset, sheet, col)
        
    def add_unedited_row_indices(self, dataset: int, sheet: int, indices: set[int]):
        self.r.sadd(f"unedited:{dataset}#{sheet}", *indices)
        
    def remove_unedited_row_indices(self, dataset: int, sheet: int, indices: list[int]):
        self.r.srem(f"unedited:{dataset}#{sheet}", *indices)
        
    def get_sample_rows(self, dataset: int, sheet: int, num_rows: int) -> list[list[str]]:
        return super().get_sample_rows(dataset, sheet, num_rows)
        
    def set_row(self, dataset: int, sheet: int, row: int, data: list[str]):
        self.r.set(f"row:{dataset}#{sheet}#{row}", json.dumps(data))
    
    def get_row(self, dataset: int, sheet: int, row: int) -> list[str]:
        raw_data = self.r.get(f"row:{dataset}#{sheet}#{row}")
        if isinstance(raw_data, str):
            return json.loads(raw_data)
        else:
            print(f"Key not found: row:{dataset}#{sheet}#{row}")
            return []
    
    def set_col(self, dataset: int, sheet: int, col: int, data: tuple[str, list[str]]):
        return super().set_col(dataset, sheet, col, data)
    
    def get_col(self, dataset: int, sheet: int, col: int) -> tuple[list[str]]:
        return super().get_col(dataset, sheet, col)
    
    def set_cell(self, dataset: int, sheet: int, row: int, col: int, data: str):
        self.r.set(f"cell:{dataset}#{sheet}#{row}#{col}", data)
    
    def get_cell(self, dataset: int, sheet: int, row: int, col: int) -> str:
        raw_data = self.r.get(f"row:{dataset}#{sheet}#{row}#{col}")
        if isinstance(raw_data, str):
            return raw_data
        else:
            print(f"Key not found: row:{dataset}#{sheet}#{row}#{col}")
            return ""
    
    def value_cross_ref(self, dataset: int, sheet: int, col: int, val: str) -> bool:
        unique_vals = self.get_unique_vals(dataset, sheet, col)
        if val in unique_vals:
            return True
        else:
            return False