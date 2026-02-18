from dataclasses import dataclass
from profyl.core.abstractions.cache import CacheType
from profyl.core.abstractions.registry import DataSourceType, RegistryType

@dataclass
class AuthenticationMixin:
    token: str | None
    
@dataclass
class ProjectMixin:
    project: str

@dataclass
class InitCommand(AuthenticationMixin, ProjectMixin):
    registry: RegistryType
    cache: CacheType
    authz: bool

@dataclass
class RegisterDatasetCommand(AuthenticationMixin, ProjectMixin):
    key: str
    source: DataSourceType
    reference: str
    
@dataclass
class LoadDatasetCommand(AuthenticationMixin, ProjectMixin):
    key: str
    
@dataclass
class RemoveDatasetCommand(AuthenticationMixin, ProjectMixin):
    key: str

@dataclass
class ListDatasetsCommand(AuthenticationMixin, ProjectMixin):
    pass

@dataclass
class StartMCPCommand(AuthenticationMixin, ProjectMixin):
    pass
    
@dataclass
class SchemaMapCommand(AuthenticationMixin, ProjectMixin):
    num_samples: int