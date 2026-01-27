from abc import ABC, abstractmethod

class Cache(ABC):
    @abstractmethod
    def store_unique_vals(self):
        pass
    
    @abstractmethod
    def get_unique_vals(self) -> set:
        pass
    
    @abstractmethod
    def store_sample_rows(self):
        pass
    
    @abstractmethod
    def get_sample_rows(self) -> list: # Will likely be more explicit and do list[row_obj]
        pass
    
    @abstractmethod
    def set_row_data(self):
        pass
        
    @abstractmethod
    def get_row_data(self, key: str) -> list: # Will likely be more explicit and do list[row_obj]
        pass
        
    @abstractmethod
    def value_in_unique_vals(self, key: str) -> bool:
        pass
        
    @abstractmethod
    def value_cross_ref(self, key: str) -> bool:
        pass