from dataclasses import dataclass
from profyl.core.abstractions.cache import CacheType
from profyl.core.abstractions.registry import DataSourceType, RegistryType

@dataclass
class AuthenticationMixin:
    # Fill this in later
    pass
    
@dataclass
class AuthorizationMixin:
    # Fill this in later
    pass
    
@dataclass
class ProjectMixin:
    project: str

@dataclass
class InitCommand():
    registry: RegistryType
    cache: CacheType
    auth: bool
    namespacing: bool
    project: str | None
    authz: bool

@dataclass
class RegisterDatasetCommand(AuthenticationMixin, AuthorizationMixin, ProjectMixin):
    key: str
    source: DataSourceType
    reference: str
    
@dataclass
class LoadDatasetCommand(AuthenticationMixin, AuthorizationMixin, ProjectMixin):
    key: str
    
@dataclass
class RemoveDatasetCommand(AuthenticationMixin, AuthorizationMixin, ProjectMixin):
    key: str

@dataclass
class ListDatasetsCommand(AuthenticationMixin, AuthorizationMixin, ProjectMixin):
    pass

@dataclass
class StartMCPCommand(AuthenticationMixin, AuthorizationMixin, ProjectMixin):
    pass
    
@dataclass
class SchemaMapCommand(AuthenticationMixin, AuthorizationMixin, ProjectMixin):
    num_samples: int