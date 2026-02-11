from datetime import datetime, timezone
from typing import ItemsView
from profyl.core.abstractions import Registry, Status, DataSourceType, Entry
from profyl.core.data_sources.excel import ExcelDataSource
from profyl.core.data_sources.mongo import MongoDataSource

class DictRegistry(Registry):
    def __init__(self) -> None:
        self.reg = {}
        self.source_num = 0
    
    def add(self, source: DataSourceType, reference: str, key: str) -> None:
        match source:
            case DataSourceType.Excel:
                data_source = ExcelDataSource()
                data_source.load(reference)
            case DataSourceType.MongoDB:
                data_source = MongoDataSource()
                data_source.load(reference)
                
        self.reg[key] = Entry(data_source, self.source_num, reference, datetime.now(timezone.utc))
        self.source_num += 1
        
    def get(self, key: str) -> Entry:
        return self.reg[key]
        
    def remove(self, key: str) -> None:
        del self.reg[key]
        
    def get_all(self) -> ItemsView[str, Entry]:
        return self.reg.items()
    
    def update_status(self, key: str, status: Status) -> None:
        self.reg[key].status = status