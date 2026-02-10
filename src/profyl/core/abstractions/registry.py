from abc import ABC, abstractmethod
from profyl.core.abstractions.data_source import DataSource
from enum import Enum, auto

class Status(Enum):
    Registered = auto()
    Loaded = auto()
    Ready = auto()
    
class Registry(ABC):
    @abstractmethod
    def add(source: DataSource, reference: str, key: str) -> None:
        pass
    
    @abstractmethod
    def get(key: str) -> tuple[DataSource, str]:
        pass
    
    @abstractmethod
    def remove(key: str) -> None:
        pass
    
    @abstractmethod
    def list_all() -> list[tuple[str, tuple[DataSource, str]]]:
        pass
        
    @abstractmethod
    def update_status(status: Status) -> None:
        pass