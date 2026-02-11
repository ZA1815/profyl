from profyl.core.abstractions.cache import CacheType
from profyl.core.abstractions.registry import RegistryType
from profyl.core.commands.commands import InitCommand
import typer
from typer.params import Annotated, Optional

cli = typer.Typer()

@cli.command()
def init(
    ctx: typer.Context,
    registry: Annotated[RegistryType, typer.Argument(help="Registry type", case_sensitive=False)],
    cache: Annotated[CacheType, typer.Argument(help="Cache type", case_sensitive=False)],
    auth: bool  = typer.Option(False, "--auth", help="Enable authentication"),
    namespacing: bool = typer.Option(False, "--namespacing", help="Enable project namespacing"),
    project: Annotated[Optional[str], typer.Option("--project", help="Project name (required if namespacing is on)")] = None,
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
    
    InitCommand(registry, cache)
    
if __name__ == "__main__":
    cli()