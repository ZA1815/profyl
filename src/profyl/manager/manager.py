from profyl.core.abstractions.cache import Cache
from profyl.core.abstractions.registry import DataSourceType, Registry
from profyl.core.data_sources.excel import ExcelDataSource
from profyl.core.data_sources.mongo import MongoDataSource

class Manager:
    def __init__(self, registry: Registry, cache: Cache) -> None:
        self.reg = registry
        self.cache = cache
        self.ds_count = 0
    
    def register_dataset(self, source: DataSourceType, reference: str, key: str):
        self.reg.add(source, reference, key)
    
    def load_dataset(self, key: str):
        entry = self.reg.get(key)
        
        match entry.source:
            case DataSourceType.Excel:
                data_source = ExcelDataSource()
            case DataSourceType.MongoDB:
                data_source = MongoDataSource()
                
        data_source.load(entry.reference)
        
        for sheet in range(data_source.get_sheet_count()):
            self.cache.set_headers(self.ds_count, sheet, data_source.read_headers(sheet))
            self.cache.add_unedited_row_indices(self.ds_count, sheet, set(range(data_source.get_row_count(sheet))))
            for row in range(data_source.get_row_count(sheet)):
                row_data = data_source.read_row(sheet, row)
                self.cache.set_row(self.ds_count, sheet, row, row_data)
        
        self.ds_count += 1