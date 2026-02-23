# USE FILE WATCHER TO CHECK FOR CHANGES IN TOML CONFIG
# I DON'T HAVE DUPLICATE CHECKS YET FOR LOADING, MAKE SURE TO ADD THAT
# CLEAN UP THE BODIES OF THE FUNCTIONS INTO A HELPER

import pickle
import socket
import struct
import subprocess
import sys
import os
import tomllib
from pathlib import Path
from profyl.core.abstractions.cache import CacheType
from profyl.core.abstractions.registry import DataSourceType, RegistryType
from profyl.core.commands import InitCommand, ListDatasetsCommand, LoadDatasetCommand, RegisterDatasetCommand, RemoveDatasetCommand, SchemaMapCommand, StartMCPCommand
from profyl.error import AuthError, ConfigError, ProjectError
import typer
from typer.params import Annotated

cli = typer.Typer()

@cli.command()
def start(
    ctx: typer.Context,
    host: Annotated[str, typer.Option(help="Host machine IP")] = "localhost",
    port: Annotated[int, typer.Option(help="Host machine port")] = 8000,
    namespacing: Annotated[bool, typer.Option(help="Enable namespacing")] = False,
    auth: Annotated[bool, typer.Option(help="Enable authentication")] = False
):
    info_path = Path(".profyl/config.toml")
    out_path = Path(".profyl/daemon_out.log")
    err_path = Path(".profyl/daemon_err.log")
    out_path.parent.mkdir(parents=True, exist_ok=True)
    err_path.parent.mkdir(parents=True, exist_ok=True)
    popen_kwargs = {
        "args": [sys.executable, "-u", "-m", "profyl.daemon", f"{host}", f"{port}", f"{namespacing}", "None"],
        "stdin": subprocess.DEVNULL,
        "stdout": open(out_path, 'w'),
        "stderr": open(err_path, 'w'),
        "close_fds": True
    }
    if auth:
        secret_key = os.getenv("SECRET_KEY")
        if secret_key is not None:
            popen_kwargs["args"][-1] = secret_key
        else:
            raise ctx.fail("--auth turned on, but JWT secret key not found in environment variable 'SECRET_KEY'")
    
    if sys.platform == "win32":
        popen_kwargs.update({ "creationflags": subprocess.DETACHED_PROCESS | subprocess.CREATE_NO_WINDOW })
        
    else:
        popen_kwargs.update({ "start_new_session": True })
    
    with open(info_path, 'w') as f:
        print(
f'''[profyl-scoped]
host = "{host}"
port = {port}
namespacing = {str(namespacing).lower()}
auth = {str(auth).lower()}''', file=f)
    
    subprocess.Popen(**popen_kwargs)

@cli.command()
def init(
    ctx: typer.Context,
    registry: Annotated[RegistryType, typer.Argument(help="Registry type", case_sensitive=False)],
    cache: Annotated[CacheType, typer.Argument(help="Cache type", case_sensitive=False)],
    project: Annotated[str, typer.Option("--project", help="Project name (required if namespacing is enabled)")] = "Namespacing not enabled",
    authz: bool = typer.Option(False, "--authz", help="Enable authorization (requires namespacing enabled)"),
    allowed_users: list[int] = typer.Option([], "--allowed-users", help="User IDs of allowed users for project (requires authz enabled)")
):
    with open(".profyl/config.toml", "rb") as f:
        data = tomllib.load(f)
    
    try:
        host = data["profyl-scoped"]["host"]
        port = data["profyl-scoped"]["port"]
        namespacing = data["profyl-scoped"]["namespacing"]
        auth = data["profyl-scoped"]["auth"]
    except KeyError as e:
        raise ConfigError(f"Missing table or key from .profyl/config.toml: {e}")
        
    if namespacing and project == "Namespacing not enabled":
        raise ctx.fail("The --project flag must be used if namespacing is enabled")
    
    if authz and not auth:
        raise ctx.fail("The auth must be enabled if --authz is enabled")
    
    if not namespacing:
        if project != "Namespacing not enabled" and authz:
            raise ctx.fail("Neither the --project nor the --authz flags can be used without namespacing enabled")
        if project != "Namespacing not enabled":
            raise ctx.fail("The --project flag cannot be used without namespacing enabled")
        if authz:
            raise ctx.fail("The --authz flag cannot be used without namespacing enabled")
    
    if namespacing:
        for proj in data["project"]:
            found = proj.get("name") == project
            if found:
                raise ProjectError(f"Project already exists: {project}")
    else:
        if data.get("project") is not None:
            if len(data["project"]) > 1:
                raise ConfigError("Number of projects cannot be greater than 1 if namespacing is not enabled")
            elif data["project"][0]["name"] != "Namespacing not enabled":
                raise ProjectError("Name of project when namespacing is not enabled has to be 'Namespacing not enabled'")    
        
    
    if auth:
        token = os.getenv("USER_TOKEN")
        if token is None:
            raise AuthError("Auth enabled, but USER_TOKEN not found")
    else:
        token = None
    
    # Allow users to change Registry and Cache from config as well, but unnecessary complexity right now
    with open(".profyl/config.toml", 'a') as f:
        print(
f'''
[[project]]
name = "{project}"
authz = {str(authz).lower()}''', file=f)
    
    command = InitCommand(project=project, token=token, registry=registry, cache=cache, authz=authz, allowed_users=allowed_users)
    data = pickle.dumps(command)
    connect(host, port, data)

@cli.command()
def register(
    ctx: typer.Context,
    key: Annotated[str, typer.Argument(help="Dataset identifier")],
    source: Annotated[DataSourceType, typer.Argument(help="DataSource type", case_sensitive=False)],
    reference: Annotated[str, typer.Argument(help="String to access DataSource")],
    project: Annotated[str, typer.Option(help="Project name (only valid if namespacing is enabled")] = "Namespacing not enabled"
):
    (host, port, token) = cmd_check(ctx, project)
    
    command = RegisterDatasetCommand(project=project, token=token, key=key, source=source, reference=reference)
    data = pickle.dumps(command)
    connect(host, port, data)
    
@cli.command()
def load(
    ctx: typer.Context,
    key: Annotated[str, typer.Argument(help="Identifier for dataset")],
    project: Annotated[str, typer.Option(help="Project name (only valid if namespacing is enabled)")] = "Namespacing not enabled"
):
    (host, port, token) = cmd_check(ctx, project)
        
    command = LoadDatasetCommand(project=project, token=token, key=key)
    data = pickle.dumps(command)
    connect(host, port, data)

@cli.command()
def remove(
    ctx: typer.Context,
    key: Annotated[str, typer.Argument(help="Identifier for dataset")],
    project: Annotated[str, typer.Option(help="Project name (only valid if namespacing is enabled)")] = "Namespacing not enabled"
):
    (host, port, token) = cmd_check(ctx, project)
                
    command = RemoveDatasetCommand(project=project, token=token, key=key)
    data = pickle.dumps(command)
    connect(host, port, data)

@cli.command()
def list(
    ctx: typer.Context,
    project: Annotated[str, typer.Option("--project", help="Project to list datasets for")] = "All projects"
):
    with open(".profyl/config.toml", 'rb') as f:
        data = tomllib.load(f)
     
    try:
        host = data["profyl-scoped"]["host"]
        port = data["profyl-scoped"]["port"]
        auth = data["profyl-scoped"]["auth"]
        data["project"]
    except KeyError as e:
        raise ConfigError(f"Missing table or key from .profyl/config.toml: {e}")
     
    if auth:
        token = os.getenv("USER_TOKEN")
        if token is None:
            raise AuthError("Auth enabled, but USER_TOKEN not found")
    else:
        token = None
        
    command = ListDatasetsCommand(project=project, token=token)
    data = pickle.dumps(command)
    connect(host, port, data)

@cli.command()
def start_mcp(
    ctx: typer.Context,
    project: Annotated[str, typer.Option("--project", help="Project to list datasets for")] = "Namespacing not enabled"
):
    (host, port, token) = cmd_check(ctx, project)
    
    command = StartMCPCommand(project=project, token=token)
    data = pickle.dumps(command)
    connect(host, port, data)

@cli.command()
def schema_map(
    ctx: typer.Context,
    num_samples: int = typer.Option(25, help="Number of samples from each dataset"),
    project: Annotated[str, typer.Option("--project", help="Project to list datasets for")] = "Namespacing not enabled"
):
    (host, port, token) = cmd_check(ctx, project)
    
    command = SchemaMapCommand(project=project, token=token, num_samples=num_samples)
    data = pickle.dumps(command)
    connect(host, port, data)

def cmd_check(ctx: typer.Context, project: str) -> tuple[str, int, str]:
    with open(".profyl/config.toml", 'rb') as f:
        data = tomllib.load(f)
    
    try:
        host = data["profyl-scoped"]["host"]
        port = data["profyl-scoped"]["port"]
        namespacing = data["profyl-scoped"]["namespacing"]
        auth = data["profyl-scoped"]["auth"]
        data["project"]
    except KeyError as e:
        raise ConfigError(f"Missing table or key from .profyl/config.toml: {e}")
    
    if not namespacing and project != "Namespacing not enabled":
        raise ctx.fail("--project flag used when namespacing is not enabled")
    
    if namespacing:
        for proj in data["project"]:
            found = proj.get("name") == project
            if not found:
                raise ProjectError(f"Project doesn't exist: {project}")
    else:
        if data.get("project") is None:
            raise ProjectError("Number of projects has to be at least 1")
        elif len(data["project"]) > 1:
            raise ConfigError("Number of projects cannot be greater than 1 if namespacing is not enabled")
        elif data["project"][0]["name"] != "Namespacing not enabled":
            raise ProjectError("Name of project when namespacing is not enabled has to be 'Namespacing not enabled'")
    
    if auth:
        token = os.getenv("USER_TOKEN")
        if token is None:
            raise AuthError("Auth enabled, but USER_TOKEN not found")
    else:
        token = None
        
    return (host, port, token)

def connect(host: str, port: int, data: bytes):
    with socket.create_connection((host, port)) as s:
        s.sendall(struct.pack("!I", len(data)))
        s.sendall(data)
        recv = s.recv(1024)
        text = recv.decode("utf-8")
        print(text)
    
if __name__ == "__main__":
    cli()