from abstractions.cache import Cache
import redis

class RedisCache(Cache):
    def __init__(self, host="localhost", port=6379) -> None:
        self.r = redis.Redis(host=host, port=port, db=0)
        
    def get_unique_vals(self, dataset: int, sheet: int, col: int) -> set[str]:
        
    
    def set_unedited_row_indices(self, dataset: int, sheet: int):
        
    
    def get_sample_rows(self, dataset: int, sheet: int) -> list[list[str]]:
        
        
    def set_row(self, dataset: int, sheet: int, row: int, data: list[str]):
        
    
    def get_row(self, dataset: int, sheet: int, row: int) -> list[str]:
        
    
    def set_cell(self, dataset: int, sheet: int, row: int, col: int, data: str):
        
    
    def get_cell(self, dataset: int, sheet: int, row: int, col: int) -> str:
        
    
    def value_cross_ref(self, dataset: int, sheet: int, col: int, val: str) -> bool:
        