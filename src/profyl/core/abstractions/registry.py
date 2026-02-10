from abc import ABC, abstractmethod
from dataclasses import dataclass
from datetime import datetime
from enum import Enum, auto
from typing import ValuesView

class Status(Enum):
    Registered = auto()
    Loaded = auto()
    Ready = auto()
    
class DataSourceType(Enum):
    Excel = auto()
    MongoDB = auto()

@dataclass
class Entry:
    source: DataSourceType
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
    def get_all(self) -> ValuesView[Entry]:
        pass
        
    @abstractmethod
    def update_status(self, key: str, status: Status) -> None:
        pass