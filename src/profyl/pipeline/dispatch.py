from typing import Any
from profyl.core.abstractions.cache import CacheType
from profyl.core.abstractions.registry import RegistryType
from profyl.core.caches.redis_cache import RedisCache
from profyl.core.commands.commands import InitCommand, ListDatasetsCommand, LoadDatasetCommand, RegisterDatasetCommand, RemoveDatasetCommand, SchemaMapCommand, StartMCPCommand
from profyl.core.registries.dict_registry import DictRegistry
from profyl.manager.manager import Manager
from profyl.pipeline.authentication import check_auth
from profyl.pipeline.authorization import check_authz
from profyl.pipeline.project_scope import find_project

def dispatch_command(daemon, command: Any, buffer: bytearray):
    if isinstance(command, InitCommand):
        handle_init(daemon=daemon, init=command, buffer=buffer)
        
    elif isinstance(command, RegisterDatasetCommand):
        handle_register(projects=daemon.projects, register=command, secret_key=daemon.secret_key, buffer=buffer)
        
    elif isinstance(command, LoadDatasetCommand):
        handle_load(projects=daemon.projects, load=command, secret_key=daemon.secret_key, buffer=buffer)
        
    elif isinstance(command, RemoveDatasetCommand):
        handle_remove(projects=daemon.projects, remove=command, secret_key=daemon.secret_key, buffer=buffer)
        
    elif isinstance(command, ListDatasetsCommand):
        handle_list(projects=daemon.projects, list=command, secret_key=daemon.secret_key, buffer=buffer)
        
    elif isinstance(command, StartMCPCommand):
        handle_start_mcp(projects=daemon.projects, start_mcp=command, secret_key=daemon.secret_key, buffer=buffer)
        
    elif isinstance(command, SchemaMapCommand):
        handle_schema_map(projects=daemon.projects, schema_map=command, secret_key=daemon.secret_key, buffer=buffer)

def run_pipeline(token: str | None, secret_key: str | None, projects: dict[dict], project_name: str, buffer: bytearray) -> dict | None:
    if secret_key != "None":
        user_id = check_auth(token=token, secret_key=secret_key, buffer=buffer)
        if user_id == -1:
            return None
        
    project = find_project(projects, project_name)
    if project is None:
        error = f"[profyl] ERROR: Could not find project with name: {project_name}".encode("utf-8")
        buffer.extend(error)
        return None
        
    if project["authz"]:
        if not check_authz(user_id=user_id, allowed_users=project["allowed_users"], buffer=buffer):
            return None
    
    return project
    
def handle_init(daemon, init: InitCommand, buffer: bytearray):
    registry_type = init.registry
    match registry_type:
        case RegistryType.Dict:
            registry = DictRegistry()
        
    cache_type = init.cache
    match cache_type:
        case CacheType.Redis:
            cache = RedisCache()
    
    token = init.token
    if daemon.secret_key != "None":
        if check_auth(token=token, secret_key=daemon.secret_key, buffer=buffer) == -1:
            return
    
    project_name = init.project
    manager = Manager(registry, cache)
    
    dict_to_insert = {
        "manager": manager,
        "authz": False
    }
    
    if init.authz:
        dict_to_insert["authz"] = True
        dict_to_insert["allowed_users"] = init.allowed_users
    
    if daemon.namespacing:
        found = find_project(daemon.projects, project_name)
        if found is not None:
            error = f"[profyl] ERROR: Project with name '{project_name}' already exists".encode("utf-8")
            buffer.extend(error)
            return
            
        daemon.projects[project_name] = dict_to_insert
        
    else:
        if len(daemon.projects) == 0:
            daemon.projects["Namespacing not enabled"] = dict_to_insert
        else:
            # Will have to create some kind of modify operation for this to work (file watcher)
            error = "[profyl] ERROR: A project already exists, for more, turn on namespacing".encode("utf-8")
            buffer.extend(error)
            return
    
    message = f"[profyl] SUCCESS: Project created: Name: {project_name}, Registry: Dict, Cache: Redis".encode("utf-8")
    buffer.extend(message)
    

def handle_register(projects: dict[dict], register: RegisterDatasetCommand, secret_key: str | None, buffer: bytearray):
    key = register.key
    source = register.source
    reference = register.reference
    project_name = register.project
    token = register.token
    
    project = run_pipeline(token=token, secret_key=secret_key, projects=projects, project_name=project_name, buffer=buffer)
    if project is None:
        return
       
    manager: Manager = project["manager"]
    manager.register_dataset(key, source, reference)
    
    message = f"[profyl] SUCCESS: Project '{project_name}' registered successfully".encode("utf-8")
    buffer.extend(message)
    return

def handle_load(projects: dict[dict], load: LoadDatasetCommand, secret_key: str | None, buffer: bytearray):
    key = load.key
    project_name = load.project
    token = load.token
    
    project = run_pipeline(token=token, secret_key=secret_key, projects=projects, project_name=project_name, buffer=buffer)
    if project is None:
        return
            
    manager: Manager = project["manager"]
    manager.load_dataset(key)
    
    message = f"[profyl] SUCCESS: Project '{project_name}' loaded to cache successfully".encode("utf-8")
    buffer.extend(message)

def handle_remove(projects: dict[dict], remove: RemoveDatasetCommand, secret_key: str | None, buffer: bytearray):
    key = remove.key
    project_name = remove.project
    token = remove.token
    
    project = run_pipeline(token=token, secret_key=secret_key, projects=projects, project_name=project_name, buffer=buffer)
    if project is None:
        return
                 
    manager: Manager = project["manager"]
    manager.remove_dataset(key)
    
    message = f"[profyl] SUCCESS: Project '{project_name}' removed successfully".encode("utf-8")
    buffer.extend(message)
    
def handle_list(projects: dict[dict], list: ListDatasetsCommand, secret_key: str | None, buffer: bytearray):
    token = list.token
    if secret_key != "None":
        user_id = check_auth(token=token, secret_key=secret_key, buffer=buffer)
        
    project_name = list.project
    if project_name == "All projects":
        for name, details in projects.items():
            name = f"Project name: {name}"
            manager: Manager = details["manager"]
            if details["authz"]:
                if not check_authz(user_id=user_id, allowed_users=details["allowed_users"], buffer=buffer):
                    return
                    
            datasets = manager.list_datasets()
            for line in datasets:
                buffer.extend(line)
    else:
        project = find_project(projects, project_name)
        if project is None:
            error = f"[profyl] ERROR: Could not find project with name: {project_name}".encode("utf-8")
            buffer.extend(error)
            return
            
        if project["authz"]:
            if not check_authz(user_id=user_id, allowed_users=project["allowed_users"], buffer=buffer):
                return
        
        manager: Manager = project["manager"]
        datasets = manager.list_datasets()
        for line in datasets:
            buffer.extend(line)
            
def handle_start_mcp(projects: dict[dict], start_mcp: StartMCPCommand, secret_key: str | None, buffer: bytearray):
    project_name = start_mcp.project
    token = start_mcp.token
    
    project = run_pipeline(token=token, secret_key=secret_key, projects=projects, project_name=project_name, buffer=buffer)
    if project is None:
        return
        
    manager: Manager = project["manager"]
    manager.start_mcp()
    
    message = "[profyl] SUCCESS: MCP server started successfully".encode("utf-8")
    buffer.extend(message)

def handle_schema_map(projects: dict[dict], schema_map: SchemaMapCommand, secret_key: str | None, buffer: bytearray):
    num_samples = schema_map.num_samples
    project_name = schema_map.project
    token = schema_map.token
    
    project = run_pipeline(token=token, secret_key=secret_key, projects=projects, project_name=project_name, buffer=buffer)
    if project is None:
        return
    
    manager: Manager = project["manager"]
    map_payload = manager.build_schema_map_payload(num_samples)
    buffer.extend(map_payload.encode("utf-8"))