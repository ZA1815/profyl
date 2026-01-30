from profyl.abstractions.cache import Cache
import redis
import json
import os

class RedisCache(Cache):
    def __init__(self) -> None:
        redis_url = os.getenv("REDIS_URL", "redis://localhost:6379/0")
        self.r = redis.from_url(redis_url, decode_responses=True)
        try:
            self.r.ping()
        except redis.ConnectionError:
            print("Redis connection failed.")
        
    def get_unique_vals(self, dataset: int, sheet: int, col: int) -> set[str]:
        col_vals = self.get_col(dataset, sheet, col)
        return set(col_vals)
        
    def add_unedited_row_indices(self, dataset: int, sheet: int, indices: set[int]):
        self.r.sadd(f"unedited:{dataset}#{sheet}", *indices)
        
    def remove_unedited_row_indices(self, dataset: int, sheet: int, indices: list[int]):
        self.r.srem(f"unedited:{dataset}#{sheet}", *indices)
    
    def get_sample_rows(self, dataset: int, sheet: int, num_rows: int) -> list[list[str]]:
        raw_data = self.r.srandmember(f"unedited:{dataset}#{sheet}", num_rows)
        indices = [json.loads(s) for s in raw_data]
        raw_rows = self.r.mget(f"row:{dataset}#{sheet}#{i}" for i in indices)
        return [json.loads(s) for s in raw_rows]
        
    def set_headers(self, dataset: int, sheet: int, headers: list[str]):
        self.r.set(f"headers:{dataset}#{sheet}", json.dumps(headers))
        
    def set_row(self, dataset: int, sheet: int, row: int, data: list[str]):
        self.r.set(f"row:{dataset}#{sheet}#{row}", json.dumps(data))
        raw_data = self.r.get(f"meta:{dataset}#{sheet}#row_count")
        if isinstance(raw_data, str):
            row_count = json.loads(raw_data)
            if row >=row_count:
                self.r.incr(f"meta:{dataset}#{sheet}#row_count") 
        else:
            self.r.incr(f"meta:{dataset}#{sheet}#row_count")
    
    def get_row(self, dataset: int, sheet: int, row: int) -> list[str]:
        raw_data = self.r.get(f"row:{dataset}#{sheet}#{row}")
        if isinstance(raw_data, str):
            return json.loads(raw_data)
        else:
            print(f"Key not found: row:{dataset}#{sheet}#{row}")
            return []
    
    def set_col(self, dataset: int, sheet: int, col: int, data: list[str]):
        raw_data = self.r.get(f"meta:{dataset}#{sheet}#row_count")
        if isinstance(raw_data, str):
            row_count = json.loads(raw_data)
            raw_strings = self.r.mget([f"row:{dataset}#{sheet}#{i}" for i in range(row_count)])
            rows = [json.loads(s) for s in raw_strings if s is not None]
            rows[0][col] = data[0]
            for i in range(1, len(rows)):
                rows[i][col] = data[i]
                
            self.r.mset(dict(zip([f"row:{dataset}#{sheet}#{i}" for i in range(row_count)], [json.dumps(s) for s in rows])))
        else:
            print(f"Key not found: meta:{dataset}#{sheet}#row_count")
    
    def get_col(self, dataset: int, sheet: int, col: int) -> list[str]:
        raw_data = self.r.get(f"meta:{dataset}#{sheet}#row_count")
        if isinstance(raw_data, str):
            row_count = json.loads(raw_data)
            raw_rows = self.r.mget([f"row:{dataset}#{sheet}#{i}" for i in range(row_count)])
            rows = [json.loads(s) for s in raw_rows if s is not None]
            col_list = []
            for i in range(len(rows)):
                col_list.append(rows[i][col])
            
            return col_list
        else:
            print(f"Key not found: meta:{dataset}#{sheet}#row_count")
            return []
    
    def set_cell(self, dataset: int, sheet: int, row: int, col: int, data: str):
        row_data = self.get_row(dataset, sheet, row)
        row_data[col] = data
        self.set_row(dataset, sheet, row, row_data)
    
    def get_cell(self, dataset: int, sheet: int, row: int, col: int) -> str:
        row_data = self.get_row(dataset, sheet, row)
        return row_data[col]
    
    def value_cross_ref(self, dataset: int, sheet: int, col: int, val: str) -> bool:
        unique_vals = self.get_unique_vals(dataset, sheet, col)
        if val in unique_vals:
            return True
        else:
            return False