from abc import ABC, abstractmethod

class Cache(ABC):
    @abstractmethod
    def get_unique_vals(self, dataset: int, sheet: int, col: int) -> set[str]:
        pass
    
    @abstractmethod
    def add_unedited_row_indices(self, dataset: int, sheet: int, indices: set[int]):
        pass
    
    @abstractmethod
    def remove_unedited_row_indices(self, dataset: int, sheet: int, indices: list[int]):
        pass
    
    @abstractmethod
    def get_sample_rows(self, dataset: int, sheet: int, num_rows: int) -> list[list[str]]: 
        pass
    
    @abstractmethod
    def set_headers(self, dataset: int, sheet: int, headers: list[str]):
        pass
    
    @abstractmethod
    def set_row(self, dataset: int, sheet: int, row: int, data: list[str]): # Add list[row_obj] later
        pass
        
    @abstractmethod
    def get_row(self, dataset: int, sheet: int, row: int) -> list[str]: # Add list[row_obj] later
        pass
    
    @abstractmethod
    def set_col(self, dataset: int, sheet: int, col: int, data: list[str]):
        pass
    
    @abstractmethod
    def get_col(self, dataset: int, sheet: int, col: int) -> list[str]:
        pass
        
    @abstractmethod
    def set_cell(self, dataset: int, sheet: int, row: int, col: int, data: str):
        pass
        
    @abstractmethod
    def get_cell(self, dataset: int, sheet: int, row: int, col: int) -> str:
        pass
        
    @abstractmethod
    def value_cross_ref(self, dataset: int, sheet: int, col: int, val: str) -> bool:
        pass