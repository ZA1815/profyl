from profyl.abstractions.data_source import DataSource
import pandas as pd

class PandasDataSource(DataSource):
    def __init__(self) -> None:
        self.data: dict[str, pd.DataFrame] = {}
        self.sheet_names = []
    
    def load(self, source: str):
        self.data = pd.read_excel(source, sheet_name=None)
        self.sheet_names = list(self.data.keys())
        
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