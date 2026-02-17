import pickle
import socket
import struct
import subprocess
import sys
from pathlib import Path
from profyl.core.abstractions.cache import CacheType
from profyl.core.abstractions.registry import DataSourceType, RegistryType
from profyl.core.commands.commands import InitCommand, ListDatasetsCommand, LoadDatasetCommand, RegisterDatasetCommand, RemoveDatasetCommand, SchemaMapCommand, StartMCPCommand
import typer
from typer.params import Annotated

cli = typer.Typer()

@cli.command()
def start(
    ctx: typer.Context,
    host: Annotated[str, typer.Option(help="Host machine IP")] = "localhost",
    port: Annotated[int, typer.Option(help="Host machine port")] = 8000
):
    out_path = Path(".profyl/daemon_out.log")
    err_path = Path(".profyl/daemon_err.log")
    out_path.parent.mkdir(parents=True, exist_ok=True)
    err_path.parent.mkdir(parents=True, exist_ok=True)
    popen_kwargs = {
        "args": [sys.executable, "-u", "-m", "profyl.daemon", f"{host}", f"{port}"],
        "stdin": subprocess.DEVNULL,
        "stdout": open(out_path, 'w'),
        "stderr": open(err_path, 'w'),
        "close_fds": True
    }
    
    if sys.platform == "win32":
        popen_kwargs.update({ "creationflags": subprocess.DETACHED_PROCESS | subprocess.CREATE_NO_WINDOW })
        
    else:
        popen_kwargs.update({ "start_new_session": True })
    
    subprocess.Popen(**popen_kwargs)

@cli.command()
def init(
    ctx: typer.Context,
    registry: Annotated[RegistryType, typer.Argument(help="Registry type", case_sensitive=False)],
    cache: Annotated[CacheType, typer.Argument(help="Cache type", case_sensitive=False)],
    auth: bool = typer.Option(False, "--auth", help="Enable authentication"),
    namespacing: bool = typer.Option(False, "--namespacing", help="Enable project namespacing"),
    project: Annotated[str | None, typer.Option("--project", help="Project name (required if namespacing is on)")] = None,
    authz: bool = typer.Option(False, "--authz", help="Enable authorization (requires namespacing to be on)")
):
    if namespacing and not project:
        raise ctx.fail("The --project flag must be used if --namespacing is turned on")
    
    if not namespacing:
        if project and authz:
            raise ctx.fail("Neither the --project nor the --authz flags can be used without --namespacing turned on")
        if project:
            raise ctx.fail("The --project flag cannot be used without --namespacing turned on")
        if authz:
            raise ctx.fail("The --authz flag cannot be used without --namespacing turned on")
    
    command = InitCommand(registry, cache, auth, namespacing, project, authz)
    data = pickle.dumps(command)
    connect("localhost", 8000, data)

@cli.command()
def register(
    ctx: typer.Context,
    key: Annotated[str, typer.Argument(help="Dataset identifier")],
    source: Annotated[DataSourceType, typer.Argument(help="DataSource type", case_sensitive=False)],
    reference: Annotated[str, typer.Argument(help="String to access DataSource")],
    # Add project param here once I add persistent state through file and check if namespacing is on
):
    command = RegisterDatasetCommand("Namespacing not active", key, source, reference)
    data = pickle.dumps(command)
    connect("localhost", 8000, data)
    
@cli.command()
def load(
    key: Annotated[str, typer.Argument(help="Identifier for dataset")]
    # Add project param here once I add persistent state through file and check if namespacing is on
):
    command = LoadDatasetCommand("test project", key)
    data = pickle.dumps(command)
    connect("localhost", 8000, data)

@cli.command()
def remove(
    key: Annotated[str, typer.Argument(help="Identifier for dataset")]
    # Add project param here once I add persistent state through file and check if namespacing is on
):
    command = RemoveDatasetCommand("test project", key)
    data = pickle.dumps(command)
    connect("localhost", 8000, data)

@cli.command()
def list(
    key: str = typer.Option("All projects", "--project", help="Project to list datasets for")
    # Add project param here once I add persistent state through file and check if namespacing is on
):
   command = ListDatasetsCommand(key)
   data = pickle.dumps(command)
   connect("localhost", 8000, data)

@cli.command()
def start_mcp(
    # Add project param here once I add persistent state through file and check if namespacing is on
):
    command = StartMCPCommand("test project")
    data = pickle.dumps(command)
    connect("localhost", 8000, data)

@cli.command()
def schema_map(
    num_samples: int = typer.Option(25, help="Number of samples from each dataset")
    # Add project param here once I add persistent state through file and check if namespacing is on
):
    command = SchemaMapCommand("test project", num_samples)
    data = pickle.dumps(command)
    connect("localhost", 8000, data)

def connect(host: str, port: int, data: bytes):
    with socket.create_connection((host, port)) as s:
        s.sendall(struct.pack("!I", len(data)))
        s.sendall(data)
        recv = s.recv(1024)
        text = recv.decode("utf-8")
        print(text)
    
if __name__ == "__main__":
    cli()