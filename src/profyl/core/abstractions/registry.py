from abc import ABC, abstractmethod
from dataclasses import dataclass
from datetime import datetime
from enum import Enum, auto
from typing import ItemsView
from profyl.core.abstractions.data_source import DataSource

class Status(Enum):
    Registered = auto()
    Loaded = auto()
    Ready = auto()
    
class DataSourceType(Enum):
    Excel = auto()
    MongoDB = auto()

class RegistryType(str, Enum):
    PythonDict = auto()

@dataclass
class Entry:
    source: DataSource
    source_num: int
    reference: str
    timestamp: datetime
    status: Status = Status.Registered
    
class Registry(ABC):
    @abstractmethod
    def add(self, source: DataSourceType, reference: str, key: str) -> None:
        pass
    
    @abstractmethod
    def get(self, key: str) -> Entry:
        pass
    
    @abstractmethod
    def remove(self, key: str) -> None:
        pass
    
    @abstractmethod
    def get_all(self) -> ItemsView[str, Entry]:
        pass
        
    @abstractmethod
    def update_status(self, key: str, status: Status) -> None:
        pass