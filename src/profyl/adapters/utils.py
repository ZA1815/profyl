import os
import pickle
import socket
import struct
import subprocess
import sys
import tomllib

from profyl.core.abstractions.cache import CacheType
from profyl.core.abstractions.registry import DataSourceType, RegistryType
from profyl.core.commands.commands import InitCommand, ListDatasetsCommand, LoadDatasetCommand, RegisterDatasetCommand, RemoveDatasetCommand, SchemaMapCommand, StartMCPCommand
from profyl.error import AuthError, ConfigError, PayloadError, ProjectError

def init_util(
    registry: RegistryType,
    cache: CacheType,
    project: str = "Namespacing not enabled",
    authz: bool = False,
    allowed_users: list[int] = [],
    token: str | None = None
):
    with open(".profyl/daemon/config.toml", "rb") as f:
        data = tomllib.load(f)
    
    try:
        host = data["profyl-scoped"]["host"]
        port = data["profyl-scoped"]["port"]
        namespacing = data["profyl-scoped"]["namespacing"]
        auth = data["profyl-scoped"]["auth"]
    except KeyError as e:
        raise ConfigError(f"[profyl] ERROR: Missing table or key from .profyl/daemon/config.toml: {e}")
        
    if namespacing and project == "Namespacing not enabled":
        raise PayloadError("[profyl] ERROR: Project name must be provided if namespacing is enabled")
    
    if authz and not auth:
        raise PayloadError("[profyl] ERROR: Authentication must be enabled if authorization is enabled")
    
    if not namespacing:
        if project != "Namespacing not enabled" and authz:
            raise PayloadError("[profyl] ERROR: Neither the project name nor authorization can be used without namespacing enabled")
        if project != "Namespacing not enabled":
            raise PayloadError("[profyl] ERROR: Project name cannot be given without namespacing enabled")
        if authz:
            raise PayloadError("[profyl] ERROR: Authorization cannot be used without namespacing enabled")
    
    if namespacing:
        for proj in data["project"]:
            found = proj.get("name") == project
            if found:
                raise ProjectError(f"[profyl] ERROR: Project already exists: {project}")
    else:
        if data.get("project") is not None:
            if len(data["project"]) > 1:
                raise ConfigError("[profyl] ERROR: Number of projects cannot be greater than 1 if namespacing is not enabled")
            elif data["project"][0]["name"] != "Namespacing not enabled":
                raise ProjectError("[profyl] ERROR: Name of project when namespacing is not enabled has to be 'Namespacing not enabled'")    
        
    if auth and token is None:
        raise AuthError("[profyl] ERROR: Auth enabled, but JWT token not found")
    
    # Allow users to change Registry and Cache from config as well, but unnecessary complexity right now
    with open(".profyl/daemon/config.toml", 'a') as f:
        print(
f'''
[[project]]
name = "{project}"
authz = {str(authz).lower()}''', file=f)
        
    command = InitCommand(project=project, token=token, registry=registry, cache=cache, authz=authz, allowed_users=allowed_users)
    data = pickle.dumps(command)
    connect(host, port, data)

def register_util(
    key: str,
    source: DataSourceType,
    reference: str,
    project: str = "Namespacing not enabled",
    token: str | None = None
):
    (host, port) = cmd_check(project, token)
    
    command = RegisterDatasetCommand(project=project, token=token, key=key, source=source, reference=reference)
    data = pickle.dumps(command)
    connect(host, port, data)

def load_util(
    key: str,
    project: str = "Namespacing not enabled",
    token: str | None = None
):
    (host, port) = cmd_check(project, token)
        
    command = LoadDatasetCommand(project=project, token=token, key=key)
    data = pickle.dumps(command)
    connect(host, port, data)

def remove_util(
    key: str,
    project: str = "Namespacing not enabled",
    token: str | None = None
):
    (host, port) = cmd_check(project, token)
                
    command = RemoveDatasetCommand(project=project, token=token, key=key)
    data = pickle.dumps(command)
    connect(host, port, data)

def list_util(
    project: str = "All projects",
    token: str | None = None
) -> str:
    with open(".profyl/daemon/config.toml", 'rb') as f:
        data = tomllib.load(f)
     
    try:
        host = data["profyl-scoped"]["host"]
        port = data["profyl-scoped"]["port"]
        auth = data["profyl-scoped"]["auth"]
        data["project"]
    except KeyError as e:
        raise ConfigError(f"Missing table or key from .profyl/daemon/config.toml: {e}")
     
    if auth and token is None:
        raise AuthError("Auth enabled, but JWT token not found")
        
    command = ListDatasetsCommand(project=project, token=token)
    data = pickle.dumps(command)
    text = connect(host, port, data)
    return text

def start_mcp_util(
    project: str = "Namespacing not enabled",
    token: str | None = None
):
    (host, port) = cmd_check(project, token)
    
    command = StartMCPCommand(project=project, token=token)
    data = pickle.dumps(command)
    connect(host, port, data)

def schema_map_util(
    num_samples: int = 25,
    project: str = "Namespacing not enabled",
    token: str | None = None
):
    (host, port) = cmd_check(project, token)
    
    command = SchemaMapCommand(project=project, token=token, num_samples=num_samples)
    data = pickle.dumps(command)
    connect(host, port, data)

def add_sys_kwargs(dict: dict):
    if sys.platform == "win32":
        dict.update({ "creationflags": subprocess.DETACHED_PROCESS | subprocess.CREATE_NO_WINDOW })
        
    else:
        dict.update({ "start_new_session": True })

def cmd_check(project: str, token: str | None) -> tuple[str, int]:
    with open(".profyl/daemon/config.toml", 'rb') as f:
        data = tomllib.load(f)
    
    try:
        host = data["profyl-scoped"]["host"]
        port = data["profyl-scoped"]["port"]
        namespacing = data["profyl-scoped"]["namespacing"]
        auth = data["profyl-scoped"]["auth"]
        data["project"]
    except KeyError as e:
        raise ConfigError(f"Missing table or key from .profyl/daemon/config.toml: {e}")
    
    if not namespacing and project != "Namespacing not enabled":
        raise PayloadError("Project name given when namespacing is not enabled")
    
    if namespacing:
        if not any(proj.get("name") == project for proj in data["project"]):
            raise ProjectError(f"Project doesn't exist: {project}")
    else:
        if data.get("project") is None:
            raise ProjectError("Number of projects has to be at least 1")
        elif len(data["project"]) > 1:
            raise ConfigError("Number of projects cannot be greater than 1 if namespacing is not enabled")
        elif data["project"][0]["name"] != "Namespacing not enabled":
            raise ProjectError("Name of project when namespacing is not enabled has to be 'Namespacing not enabled'")
    
    if auth and token is None:
        raise AuthError("[profyl] ERROR: Auth enabled, but JWT token not found")
        
    return (host, port)

def connect(host: str, port: int, data: bytes) -> str:
    with socket.create_connection((host, port)) as s:
        s.sendall(struct.pack("!I", len(data)))
        s.sendall(data)
        length_bytes = b""
        while len(length_bytes) < 4:
            chunk = s.recv(4 - len(length_bytes))
            length_bytes += chunk
            
        length = struct.unpack("!I", length_bytes)[0]
        data = b""
        while len(data) < length:
            chunk = s.recv(length - len(data))
            data += chunk
            
        text = data.decode("utf-8")
        print(text)
        return text