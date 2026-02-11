from typing import Any
from profyl.core.abstractions import DataSource
import pandas as pd

from profyl.core.abstractions.data_source import SheetData

class ExcelDataSource(DataSource):
    def __init__(self) -> None:
        self.data: dict[str, pd.DataFrame] = {}
        self.sheet_names = []
        self.source = ""
    
    def load(self, source: str):
        self.data = pd.read_excel(source, sheet_name=None)
        self.sheet_names = list(self.data.keys())
        self.source = source
    
    def get_schema_map_payload(self, num_samples: int) -> dict[str, Any]:
        payload = {
            "Dataset Name": "",
            "Original Headers": {},
            "Sample Data": {}
        }
        for sheet_idx, _ in enumerate(self.sheet_names):
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
        sheet_df = self._get_sheet_df(sheet)
        row_vals: list = sheet_df.iloc[row].tolist()
        return list(map(str, row_vals))
    
    def read_headers(self, sheet: int) -> list[str]:
        sheet_df = self._get_sheet_df(sheet)
        headers: list = sheet_df.columns.tolist()
        return list(map(str, headers))
        
    def read_col(self, sheet: int, col: int) -> tuple[str, list[str]]:
        sheet_df = self._get_sheet_df(sheet)
        col_name: str = str(sheet_df.columns[col])
        col_vals: list = sheet_df.iloc[:, col].tolist()
        
        return (col_name, list(map(str, col_vals)))
    
    def save(self, data: list[SheetData]):
        self.data.clear()
        self.sheet_names.clear()
        
        with pd.ExcelWriter(self.source) as ew:
            for sheet in data:
                self.sheet_names.append(sheet.name)
                self.data[sheet.name] = pd.DataFrame(sheet.rows, columns=sheet.headers)
                self.data[sheet.name].to_excel(ew, sheet_name=sheet.name, index=False)
                # Could add formatting here later using openpyxl
    
    def get_sheet_count(self) -> int:
        return len(self.data)
    
    def get_row_count(self, sheet: int) -> int:
        sheet_df = self._get_sheet_df(sheet)
        
        return len(sheet_df)
        
    def get_col_count(self, sheet: int) -> int:
        sheet_df = self._get_sheet_df(sheet)
        
        return len(sheet_df.columns)
    
    def _get_sheet_df(self, sheet: int) -> pd.DataFrame:
        sheet_df = self.data.get(self.sheet_names[sheet])
        if sheet_df is not None:
            return sheet_df
        else:
            raise ValueError(f"Sheet '{self.sheet_names[sheet]}' not found in data.")