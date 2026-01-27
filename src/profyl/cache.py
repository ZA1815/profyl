from abc import ABC, abstractmethod

from src.profyl.data_source import DataSource

class Cache(ABC):
    def populate_from(self, data_source: DataSource, dataset: int, row_sample_num: int):
        for sheet in range(data_source.get_sheet_count()):
            sample_rows: list[list[str]] = []
            row_num = data_source.get_row_count(sheet)
            if row_num <= 25:
                for row in range(row_num):
                    sample_rows.append(data_source.read_row(sheet, row))
            else:
                sample_indices = [i * row_num // row_sample_num for i in range(row_sample_num)]
                for row in sample_indices:
                    sample_rows.append(data_source.read_row(sheet, row))
                        
            self.store_sample_rows(dataset, sheet, sample_rows)
                    
            for col in range(data_source.get_col_count(sheet)):
                unique_vals: set[str] = set(data_source.read_col(sheet, col)[1])
                self.store_unique_vals(dataset, sheet, col, unique_vals)
        
    @abstractmethod
    def store_unique_vals(self, dataset: int, sheet: int, col: int, vals: set[str]):
        pass
    
    @abstractmethod
    def get_unique_vals(self, dataset: int, sheet: int, col: int) -> set[str]:
        pass
    
    @abstractmethod
    def store_sample_rows(self, dataset: int, sheet: int, rows: list[list[str]]): # Add list[row_obj] later
        pass
    
    @abstractmethod
    def get_sample_rows(self, dataset: int, sheet: int) -> list[list[str]]: # Add list[row_obj] later 
        pass
    
    @abstractmethod
    def set_row_data(self, dataset: int, sheet: int, row: int, data: list[str]): # Add list[row_obj] later
        pass
        
    @abstractmethod
    def get_row_data(self, dataset: int, sheet: int, row: int) -> list[str]: # Add list[row_obj] later
        pass
    
    @abstractmethod
    def set_cell_data(self, dataset: int, sheet: int, row: int, col: int, data: str):
        pass
        
    @abstractmethod
    def get_cell_data(self, dataset: int, sheet: int, row: int, col: int) -> str:
        pass
        
    @abstractmethod
    def value_cross_ref(self, dataset: int, sheet: int, col: int, val: str) -> bool:
        pass