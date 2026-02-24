from sys import argv
from typing import Any
from aiohttp import web
from profyl.adapters.utils import init_util, list_util, load_util, register_util, remove_util, schema_map_util, start_mcp_util
from profyl.core.abstractions.cache import CacheType
from profyl.core.abstractions.registry import DataSourceType, RegistryType
from profyl.error import PayloadError

async def init(request: web.Request):
    body = await request.json()
    if not isinstance(body, dict):
        return web.Response(text="[profyl] ERROR: Command payload is supposed to be an object", status=422)
        
    registry: Any | None = body.get("registry")
    if not isinstance(registry, str):
        return web.Response(text="[profyl] ERROR: RegistryType is a required field and must be a string", status=422)
    match registry.lower():
        case "dict":
            registry = RegistryType.Dict
        case _:
            return web.Response(text="[profyl] ERROR: RegistryType value isn't valid", status=422)
    
    cache: Any | None = body.get("cache")
    if not isinstance(cache, str):
        return web.Response(text="[profyl] ERROR: CacheType is a required field is a required field and must be a string", status=422)
    match cache.lower():
        case "redis":
            cache = CacheType.Redis
        case _:
            return web.Response(text="[profyl] ERROR: CacheType value isn't valid", status=422)
    
    project = body.get("project")
    if project is None:
        project = "Namespacing not enabled"
    else:
        if not isinstance(project, str):
            return web.Response(text="[profyl] ERROR: project must be a string", status=422)
            
    authz = body.get("authz")
    if authz is None:
        authz = False
    else:
        if not isinstance(authz, bool):
            return web.Response(text="[profyl] ERROR: authz must be a bool", status=422)
    
    allowed_users = body.get("allowedUsers")
    if allowed_users is None:
        allowed_users = []
    else:
        if not isinstance(allowed_users, list[int]):
            return web.Response(text="[profyl] ERROR: allowedUsers must be a list[int]", status=422)
            
    init_util(registry=registry, cache=cache, project=project, authz=authz, allowed_users=allowed_users)
    
async def register(request: web.Request):
    body = await request.json()
    if not isinstance(body, dict):
        return web.Response(text="[profyl] ERROR: Command payload is supposed to be an object", status=422)
    
    key: Any | None = body.get("key")
    if not isinstance(key, str):
        return web.Response(text="[profyl] ERROR: key is a required field and must be a string", status=422)
    
    source: Any | None = body.get("source")
    if not isinstance(source, str):
        return web.Response(text="[profyl] ERROR: source is a required field and must be a string", status=422)
    
    match source.lower():
        case "excel":
            source = DataSourceType.Excel
        case "mongodb":
            source = DataSourceType.MongoDB
        case _:
            return web.Response(text="[profyl] ERROR: DataSourceType value isn't valid", status=422)
    
    reference: Any | None = body.get("reference")
    if not isinstance(reference, str):
        return web.Response(text="[profyl] ERROR: reference is a required field and must be a string", status=422)
    
    project: Any | None = body.get("project")
    if project is None:
        project = "Namespacing not enabled"
    else:
        if not isinstance(project, str):
            return web.Response(text="[profyl] ERROR: project must be a string", status=422)
    
    register_util(key=key, source=source, reference=reference, project=project)
    
async def load(request: web.Request):
    body = await request.json()
    if not isinstance(body, dict):
        return web.Response(text="[profyl] ERROR: Command payload is supposed to be an object", status=422)
    
    key: Any | None = body.get("key")
    if not isinstance(key, str):
        return web.Response(text="[profyl] ERROR: key is a required field and must be a string", status=422)
    
    project: Any | None = body.get("project")
    if project is None:
        project = "Namespacing not enabled"
    else:
        if not isinstance(project, str):
            return web.Response(text="[profyl] ERROR: project must be a string", status=422)
            
    load_util(key=key, project=project)

async def remove(request: web.Request):
    body = await request.json()
    if not isinstance(body, dict):
        return web.Response(text="[profyl] ERROR: Command payload is supposed to be an object", status=422)
        
    key: Any | None = body.get("key")
    if not isinstance(key, str):
        return web.Response(text="[profyl] ERROR: key is a required field and must be a string", status=422)
        
    project: Any | None = body.get("project")
    if project is None:
        project = "Namespacing not enabled"
    else:
        if not isinstance(project, str):
            return web.Response(text="[profyl] ERROR: project must be a string", status=422)
    
    remove_util(key=key, project=project)

async def list_datasets(request: web.Request):
    body = await request.json()
    if not isinstance(body, dict):
        return web.Response(text="[profyl] ERROR: Command payload is supposed to be an object", status=422)
     
    project: Any | None = body.get("project")
    if project is None:
        project = "All projects"
    else:
        if not isinstance(project, str):
            return web.Response(text="[profyl] ERROR: project must be a string", status=422)
    
    list_util(project=project)

async def start_mcp(
    request: web.Request,
):
    body = await request.json()
    if not isinstance(body, dict):
        return web.Response(text="[profyl] ERROR: Command payload is supposed to be an object", status=422)
    
    project: Any | None = body.get("project")
    if project is None:
        project = "Namespacing not enabled"
    else:
        if not isinstance(project, str):
            return web.Response(text="[profyl] ERROR: project must be a string", status=422)
    
    start_mcp_util(project=project)

async def schema_map(
    request: web.Request,
):
    body = await request.json()
    if not isinstance(body, dict):
        return web.Response(text="[profyl] ERROR: Command payload is supposed to be an object", status=422)
    
    num_samples: Any | None = body.get("num_samples")
    if num_samples is None:
        num_samples = "Namespacing not enabled"
    else:
        if not isinstance(num_samples, int):
            return web.Response(text="[profyl] ERROR: num_samples must be an int", status=422)
                    
    project: Any | None = body.get("project")
    if project is None:
        project = "Namespacing not enabled"
    else:
        if not isinstance(project, str):
            return web.Response(text="[profyl] ERROR: project must be a string", status=422)
            
    schema_map_util(num_samples=num_samples, project=project)

server = web.Application()
server.add_routes([
    web.post("/init", init),
    web.post("/register", register),
    web.post("/load", load),
    web.post("/remove", remove),
    web.post("/list", list),
    web.post("/start-mcp", start_mcp),
    web.post("/schema-map", schema_map)
])
    
if __name__ == "__main__":
    web.run_app(app=server, host=argv[1], port=int(argv[2]))