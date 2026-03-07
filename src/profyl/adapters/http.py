from json.decoder import JSONDecodeError
from sys import argv
from typing import Any
from aiohttp import web
from profyl.adapters.utils import init_util, list_util, load_util, register_util, remove_util, schema_map_util, start_mcp_util
from profyl.core.abstractions.cache import CacheType
from profyl.core.abstractions.registry import DataSourceType, RegistryType
from profyl.error import AuthError, ConfigError, PayloadError, ProjectError

async def init(request: web.Request):
    try:
        body = await recv_body(request)
    except PayloadError as e:
        return web.Response(text=str(e), status=422)
        
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
        if not isinstance(allowed_users, list) and all(isinstance(x, int) for x in allowed_users):
            return web.Response(text="[profyl] ERROR: allowedUsers must be a list[int]", status=422)
    
    auth_header = request.headers.get("Authorization")
    if auth_header is not None:
        token = auth_header.removeprefix("Bearer ")
    else:
        token = None
        
    try:
        init_util(registry=registry, cache=cache, project=project, authz=authz, allowed_users=allowed_users, token=token)
        return web.Response(text=f"[profyl] SUCCESS: Project '{project}' created", status=200)
    except ConfigError as e:
        return web.Response(text=str(e), status=422)
    except PayloadError as e:
        return web.Response(text=str(e), status=422)
    except ProjectError as e:
        return web.Response(text=str(e), status=409)
    except AuthError as e:
        return web.Response(text=str(e), status=401)
    
async def register(request: web.Request):
    try:
        body = await recv_body(request)
    except PayloadError as e:
        return web.Response(text=str(e), status=422)
        
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
    
    auth_header = request.headers.get("Authorization")
    if auth_header is not None:
        token = auth_header.removeprefix("Bearer ")
    else:
        token = None
    
    try:
        register_util(key=key, source=source, reference=reference, project=project, token=token)
        return web.Response(text=f"[profyl] SUCCESS: Project '{project}' registered", status=200)
    except ConfigError as e:
        return web.Response(text=str(e), status=422)
    except PayloadError as e:
        return web.Response(text=str(e), status=422)
    except ProjectError as e:
        return web.Response(text=str(e), status=409)
    except AuthError as e:
        return web.Response(text=str(e), status=401) 
    
async def load(request: web.Request):
    try:
        body = await recv_body(request)
    except PayloadError as e:
        return web.Response(text=str(e), status=422)
    
    key: Any | None = body.get("key")
    if not isinstance(key, str):
        return web.Response(text="[profyl] ERROR: key is a required field and must be a string", status=422)
    
    project: Any | None = body.get("project")
    if project is None:
        project = "Namespacing not enabled"
    else:
        if not isinstance(project, str):
            return web.Response(text="[profyl] ERROR: project must be a string", status=422)
            
    auth_header = request.headers.get("Authorization")
    if auth_header is not None:
        token = auth_header.removeprefix("Bearer ")
    else:
        token = None
    
    try:
        load_util(key=key, project=project, token=token)
        return web.Response(text=f"[profyl] SUCCESS: Project '{project}' loaded", status=200)
    except ConfigError as e:
        return web.Response(text=str(e), status=422)
    except PayloadError as e:
        return web.Response(text=str(e), status=422)
    except ProjectError as e:
        return web.Response(text=str(e), status=409)
    except AuthError as e:
        return web.Response(text=str(e), status=401) 

async def remove(request: web.Request):
    try:
        body = await recv_body(request)
    except PayloadError as e:
        return web.Response(text=str(e), status=422)
        
    key: Any | None = body.get("key")
    if not isinstance(key, str):
        return web.Response(text="[profyl] ERROR: key is a required field and must be a string", status=422)
        
    project: Any | None = body.get("project")
    if project is None:
        project = "Namespacing not enabled"
    else:
        if not isinstance(project, str):
            return web.Response(text="[profyl] ERROR: project must be a string", status=422)
    
    auth_header = request.headers.get("Authorization")
    if auth_header is not None:
        token = auth_header.removeprefix("Bearer ")
    else:
        token = None
    
    try:
        remove_util(key=key, project=project, token=token)
        return web.Response(text=f"[profyl] SUCCESS: Project '{project}' removed", status=200)
    except ConfigError as e:
        return web.Response(text=str(e), status=422)
    except PayloadError as e:
        return web.Response(text=str(e), status=422)
    except ProjectError as e:
        return web.Response(text=str(e), status=409)
    except AuthError as e:
        return web.Response(text=str(e), status=401) 

async def list_datasets(request: web.Request):
    try:
        body = await recv_body(request)
    except PayloadError as e:
        return web.Response(text=str(e), status=422)
     
    project: Any | None = body.get("project")
    if project is None:
        project = "All projects"
    else:
        if not isinstance(project, str):
            return web.Response(text="[profyl] ERROR: project must be a string", status=422)
    
    auth_header = request.headers.get("Authorization")
    if auth_header is not None:
        token = auth_header.removeprefix("Bearer ")
    else:
        token = None
    
    try:
        text = list_util(project=project, token=token)
        return web.Response(text=text, status=200)
    except ConfigError as e:
        return web.Response(text=str(e), status=422)
    except PayloadError as e:
        return web.Response(text=str(e), status=422)
    except ProjectError as e:
        return web.Response(text=str(e), status=409)
    except AuthError as e:
        return web.Response(text=str(e), status=401) 

async def start_mcp(request: web.Request):
    try:
        body = await recv_body(request)
    except PayloadError as e:
        return web.Response(text=str(e), status=422)
    
    project: Any | None = body.get("project")
    if project is None:
        project = "Namespacing not enabled"
    else:
        if not isinstance(project, str):
            return web.Response(text="[profyl] ERROR: project must be a string", status=422)
    
    auth_header = request.headers.get("Authorization")
    if auth_header is not None:
        token = auth_header.removeprefix("Bearer ")
    else:
        token = None
    
    try:
        start_mcp_util(project=project, token=token)
        return web.Response(text=f"[profyl] SUCCESS: MCP server started for '{project}'", status=200)
    except ConfigError as e:
        return web.Response(text=str(e), status=422)
    except PayloadError as e:
        return web.Response(text=str(e), status=422)
    except ProjectError as e:
        return web.Response(text=str(e), status=409)
    except AuthError as e:
        return web.Response(text=str(e), status=401) 

async def schema_map(request: web.Request):
    try:
        body = await recv_body(request)
    except PayloadError as e:
        return web.Response(text=str(e), status=422)
    
    num_samples: Any | None = body.get("num_samples")
    if num_samples is None:
        num_samples = 25
    else:
        if not isinstance(num_samples, int):
            return web.Response(text="[profyl] ERROR: num_samples must be an int", status=422)
                    
    project: Any | None = body.get("project")
    if project is None:
        project = "Namespacing not enabled"
    else:
        if not isinstance(project, str):
            return web.Response(text="[profyl] ERROR: project must be a string", status=422)
    
    auth_header = request.headers.get("Authorization")
    if auth_header is not None:
        token = auth_header.removeprefix("Bearer ")
    else:
        token = None
            
    try:
        schema_map_util(num_samples=num_samples, project=project, token=token)
        return web.Response(text=f"[profyl] SUCCESS: Project '{project}' schema mapped", status=200)
    except ConfigError as e:
        return web.Response(text=str(e), status=422)
    except PayloadError as e:
        return web.Response(text=str(e), status=422)
    except ProjectError as e:
        return web.Response(text=str(e), status=409)
    except AuthError as e:
        return web.Response(text=str(e), status=401) 

async def recv_body(request: web.Request) -> dict:
    try:
        body = await request.json()
    except JSONDecodeError as e:
        raise PayloadError(f"[profyl] ERROR: {str(e)}")
        
    if not isinstance(body, dict):
        raise PayloadError("[profyl] ERROR: Command payload is supposed to be an object")
    
    return body

server = web.Application()
server.add_routes([
    web.post("/init", init),
    web.post("/register", register),
    web.post("/load", load),
    web.post("/remove", remove),
    web.post("/list", list_datasets),
    web.post("/start-mcp", start_mcp),
    web.post("/schema-map", schema_map)
])
    
if __name__ == "__main__":
    web.run_app(app=server, host=argv[1], port=int(argv[2]))