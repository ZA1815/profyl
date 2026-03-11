# USE FILE WATCHER TO CHECK FOR CHANGES IN TOML CONFIG
# I DON'T HAVE DUPLICATE CHECKS YET FOR LOADING, MAKE SURE TO ADD THAT

import subprocess
import sys
import os
from pathlib import Path
from profyl.adapters.utils import add_sys_kwargs, init_util, list_util, load_util, register_util, remove_util, restore_util, schema_map_util, start_mcp_util
from profyl.core.abstractions.cache import CacheType
from profyl.core.abstractions.registry import DataSourceType, RegistryType
import typer
from typing import Annotated

cli = typer.Typer()

@cli.command()
def start(
    ctx: typer.Context,
    host: Annotated[str, typer.Option(help="Host for profyl daemon")] = "localhost",
    port: Annotated[int, typer.Option(help="Port for profyl daemon")] = 8000,
    namespacing: Annotated[bool, typer.Option(help="Enable namespacing")] = False,
    auth: Annotated[bool, typer.Option(help="Enable authentication")] = False,
    http: Annotated[bool, typer.Option(help="Start HTTP server")] = False,
    http_host: Annotated[str, typer.Option(help="Host for HTTP server (requires --http flag)")] = "0.0.0.0",
    http_port: Annotated[int, typer.Option(help="Port for HTTP server (requires --http flag)")] = 8001,
    watch: Annotated[bool, typer.Option(help="Start folder watch")] = False,
    folder_path: Annotated[str, typer.Option(help="Path to folder (requires --watch flag)")] = ""
):
    info_path = Path(".profyl/daemon/config.toml")
    daemon_out_path = Path(".profyl/daemon/out.log")
    daemon_err_path = Path(".profyl/daemon/err.log")
    daemon_pid_path = Path(".profyl/daemon/process.pid")
    if not daemon_pid_path.exists():
        info_path.parent.mkdir(parents=True, exist_ok=True)
        daemon_out_path.parent.mkdir(parents=True, exist_ok=True)
        daemon_err_path.parent.mkdir(parents=True, exist_ok=True)
        daemon_pid_path.parent.mkdir(parents=True, exist_ok=True)
        daemon_popen_kwargs = {
            "args": [sys.executable, "-u", "-m", "profyl.daemon", f"{host}", f"{port}", f"{namespacing}", "None"],
            "stdin": subprocess.DEVNULL,
            "stdout": open(daemon_out_path, 'w'),
            "stderr": open(daemon_err_path, 'w'),
            "close_fds": True
        }
        if auth:
            secret_key = os.getenv("SECRET_KEY")
            if secret_key is not None:
                daemon_popen_kwargs["args"][-1] = secret_key
            else:
                raise ctx.fail("--auth turned on, but JWT secret key not found in environment variable 'SECRET_KEY'")
        
        add_sys_kwargs(daemon_popen_kwargs)
            
        daemon = subprocess.Popen(**daemon_popen_kwargs)
        
        with open(info_path, 'w') as f:
            print(
f'''[profyl-scoped]
host = "{host}"
port = {port}
namespacing = {str(namespacing).lower()}
auth = {str(auth).lower()}''', file=f)
            
        with open(daemon_pid_path, 'w') as f:
            print(daemon.pid, file=f)
    elif not http and not watch:
        raise ctx.fail("Start command when daemon is running and neither --http nor --watch flags were used")
    
    if http:
        http_out_path = Path(".profyl/http/out.log")
        http_err_path = Path(".profyl/http/err.log")
        http_pid_path = Path(".profyl/http/process.pid")
        if http_pid_path.exists():
            raise ctx.fail("An HTTP server is already running while the --http flag was used")
        else:
            http_out_path.parent.mkdir(parents=True, exist_ok=True)
            http_err_path.parent.mkdir(parents=True, exist_ok=True)
            http_pid_path.parent.mkdir(parents=True, exist_ok=True)
            
        http_popen_kwargs = {
            "args": [sys.executable, "-u", "-m", "profyl.adapters.http", f"{http_host}", f"{http_port}"],
            "stdin": subprocess.DEVNULL,
            "stdout": open(http_out_path, 'w'),
            "stderr": open(http_err_path, 'w'),
            "close_fds": True
        }
        add_sys_kwargs(http_popen_kwargs)
        http = subprocess.Popen(**http_popen_kwargs)
        with open(http_pid_path, 'w') as f:
            print(http.pid, file=f)
    
    if watch:
        watch_out_path = Path(".profyl/watch/out.log")
        watch_err_path = Path(".profyl/watch/err.log")
        watch_pid_path = Path(".profyl/watch/process.pid")
        if watch_pid_path.exists():
            raise ctx.fail("A folder watch process is already running while the --watch flag was used")
        else:
            watch_out_path.parent.mkdir(parents=True, exist_ok=True)
            watch_err_path.parent.mkdir(parents=True, exist_ok=True)
            watch_pid_path.parent.mkdir(parents=True, exist_ok=True)
            
        watch_popen_kwargs = {
            "args": [sys.executable, "-u", "-m", "profyl.adapters.watch", f"{folder_path}"],
            "stdin": subprocess.DEVNULL,
            "stdout": open(watch_out_path, 'w'),
            "stderr": open(watch_err_path, 'w'),
            "close_fds": True
        }
        add_sys_kwargs(watch_popen_kwargs)
        watch = subprocess.Popen(**watch_popen_kwargs)
        with open(watch_pid_path, 'w') as f:
            print(watch.pid, file=f)

@cli.command()
def restore():
    restore_util()
    
@cli.command()
def init(
    registry: Annotated[RegistryType, typer.Argument(help="Registry type", case_sensitive=False)],
    cache: Annotated[CacheType, typer.Argument(help="Cache type", case_sensitive=False)],
    project: Annotated[str, typer.Option("--project", help="Project name (required if namespacing is enabled)")] = "Namespacing not enabled",
    authz: bool = typer.Option(False, "--authz", help="Enable authorization (requires namespacing enabled)"),
    allowed_users: list[int] = typer.Option([], "--allowed-users", help="User IDs of allowed users for project (requires authz enabled)")
):
    token = os.getenv("USER_TOKEN")
    init_util(registry=registry, cache=cache, project=project, authz=authz, allowed_users=allowed_users, token=token)
    
@cli.command()
def register(
    key: Annotated[str, typer.Argument(help="Dataset identifier")],
    source: Annotated[DataSourceType, typer.Argument(help="DataSource type", case_sensitive=False)],
    reference: Annotated[str, typer.Argument(help="String to access DataSource")],
    project: Annotated[str, typer.Option(help="Project name (only valid if namespacing is enabled")] = "Namespacing not enabled"
):
    token = os.getenv("USER_TOKEN")
    register_util(key=key, source=source, reference=reference, project=project, token=token)
    
@cli.command()
def load(
    key: Annotated[str, typer.Argument(help="Identifier for dataset")],
    project: Annotated[str, typer.Option(help="Project name (only valid if namespacing is enabled)")] = "Namespacing not enabled"
):
    token = os.getenv("USER_TOKEN")
    load_util(key=key, project=project, token=token)

@cli.command()
def remove(
    key: Annotated[str, typer.Argument(help="Identifier for dataset")],
    project: Annotated[str, typer.Option(help="Project name (only valid if namespacing is enabled)")] = "Namespacing not enabled"
):
    token = os.getenv("USER_TOKEN")
    remove_util(key=key, project=project, token=token)

@cli.command()
def list(
    project: Annotated[str, typer.Option("--project", help="Project to list datasets for")] = "All projects"
):
    token = os.getenv("USER_TOKEN")
    list_util(project=project, token=token)

@cli.command()
def start_mcp(
    project: Annotated[str, typer.Option("--project", help="Project to list datasets for")] = "Namespacing not enabled"
):
    token = os.getenv("USER_TOKEN")
    start_mcp_util(project=project, token=token)

@cli.command()
def schema_map(
    num_samples: int = typer.Option(25, help="Number of samples from each dataset"),
    project: Annotated[str, typer.Option("--project", help="Project to list datasets for")] = "Namespacing not enabled"
):
    token = os.getenv("USER_TOKEN")
    schema_map_util(num_samples=num_samples, project=project, token=token)

if __name__ == "__main__":
    cli()