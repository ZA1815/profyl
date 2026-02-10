from datetime import datetime
from time import timezone
from typing import ValuesView
from profyl.core.abstractions import Registry, Status, DataSourceType, Entry

class DictRegistry(Registry):
    def __init__(self) -> None:
        self.reg = {}
    
    def add(self, source: DataSourceType, reference: str, key: str) -> None:
        self.reg[key] = Entry(source, reference, datetime.now(timezone.utc))
        
    def get(self, key: str) -> Entry:
        return self.reg[key]
        
    def remove(self, key: str) -> None:
        del self.reg[key]
        
    def get_all(self) -> ValuesView[Entry]:
        return self.reg.values()
    
    def update_status(self, key: str, status: Status) -> None:
        self.reg[key].status = status