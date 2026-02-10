from abc import ABC, abstractmethod

class DataSource(ABC):
    @abstractmethod
    def load(self, source: str):
        pass
    
    def get_schema_map_payload(self, num_samples: int) -> str:
        pass
    
    @abstractmethod
    def read_row(self, sheet: int, row: int) -> list[str]: # Add list[row_obj] later
        pass
    
    @abstractmethod
    def read_headers(self, sheet) -> list[str]:
        pass
        
    @abstractmethod
    def read_col(self, sheet: int, col: int) -> tuple[str, list[str]]: # Add list[col_obj] later
        pass
    
    @abstractmethod
    def get_sheet_count(self) -> int:
        pass
        
    @abstractmethod
    def get_row_count(self, sheet: int) -> int:
        pass
        
    @abstractmethod
    def get_col_count(self, sheet: int) -> int:
        pass