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

def dispatch_command(projects: list[dict], command: Any, buffer: bytearray):
    if isinstance(command, InitCommand):
        handle_init(projects, command, buffer)
        
    elif isinstance(command, RegisterDatasetCommand):
        handle_register(projects, command, buffer)
        
    elif isinstance(command, LoadDatasetCommand):
        handle_load(projects, command, buffer)
        
    elif isinstance(command, RemoveDatasetCommand):
        handle_remove(projects, command, buffer)
        
    elif isinstance(command, ListDatasetsCommand):
        handle_list(projects, command, buffer)
        
    elif isinstance(command, StartMCPCommand):
        handle_start_mcp(projects, command, buffer)
        
    elif isinstance(command, SchemaMapCommand):
        handle_schema_map(projects, command, buffer)
    
def run_pipeline(auth: bool, authz: bool):
    if auth:
        # Figure out best way to get API_KEY, maybe ENV var?
        api_key = ""
        check_auth(api_key)
        
    if authz:
        # Figure out best way to get username, probably stored with project right?
        username = ""
        check_authz(username)

def handle_init(projects: list[dict], init: InitCommand, buffer: bytearray):
    registry_type = init.registry
    match registry_type:
        case RegistryType.Dict:
            registry = DictRegistry()
        
    cache_type = init.cache
    match cache_type:
        case CacheType.Redis:
            cache = RedisCache()
    
    auth = init.auth
    namespacing = init.namespacing
    project_name = init.project
    authz = init.authz
    manager = Manager(registry, cache)
    
    dict_to_insert = {
        "name": "",
        "manager": manager,
        "namespacing": namespacing,
        "auth": auth,
        "authz": authz
    }
    if namespacing:
        found = find_project(projects, project_name)
        if found is not None:
            # Error here because project with name already exists
            error = f"[profyl] ERROR: Project with name '{project_name}' already exists".encode("utf-8")
            buffer.extend(error)
            return
            
        dict_to_insert["name"] = project_name
        projects.append(dict_to_insert)
        
    else:
        if len(projects) == 0:
            dict_to_insert["name"] = "Namespacing not active"
            projects.append(dict_to_insert)
        else:
            # Will have to create some kind of modify operation for this to work
            error = "[profyl] ERROR: A project already exists, for more, turn on namespacing".encode("utf-8")
            buffer.extend(error)
            return
    
    message = f"[profyl] SUCCESS: Project created: Name: {project_name}, Registry: Dict, Cache: Redis".encode("utf-8")
    buffer.extend(message)
    

def handle_register(projects: list[dict], register: RegisterDatasetCommand, buffer: bytearray):
    key = register.key
    source = register.source
    reference = register.reference
    project_name = register.project
    
    project = find_project(projects, project_name)
    if project is None:
        error = f"[profyl] ERROR: Could not find project with name: {project_name}".encode("utf-8")
        buffer.extend(error)
        return
    
    manager: Manager = project["manager"]
    manager.register_dataset(key, source, reference)
    
    message = f"[profyl] SUCCESS: Project '{project_name}' registered successfully"
    buffer.extend(message)
    return

def handle_load(projects: list[dict], load: LoadDatasetCommand, buffer: bytearray):
    key = load.key
    project_name = load.project
    
    project = find_project(projects, project_name)
    if project is None:
        error = f"[profyl] ERROR: Could not find project with name: {project_name}".encode("utf-8")
        buffer.extend(error)
        return 
            
    manager: Manager = project["manager"]
    manager.load_dataset(key)
    
    message = f"[profyl] SUCCESS: Project '{project_name}' loaded to cache successfully".encode("utf-8")
    buffer.extend(message)

def handle_remove(projects: list[dict], remove: RemoveDatasetCommand, buffer: bytearray):
    key = remove.key
    project_name = remove.project
    
    project = find_project(projects, project_name)
    if project is None:
        error = f"[profyl] ERROR: Could not find project with name: {project_name}".encode("utf-8")
        buffer.extend(error)
        return 
    
    manager: Manager = project["manager"]
    manager.remove_dataset(key)
    
    message = f"[profyl] SUCCESS: Project '{project_name}' removed successfully"
    buffer.extend(message)
    
def handle_list(projects: list[dict], list: ListDatasetsCommand, buffer: bytearray):
    project_name = list.project
    if project_name == "All projects":
        for proj in projects:
            name = f"Project name: {proj["name"]}"
            manager: Manager = proj["manager"]
            # Make list_datasets return an array of strings instead of printing
            manager.list_datasets()
    else:
        project = find_project(projects, project_name)
        if project is None:
            error = f"[profyl] ERROR: Could not find project with name: {project_name}".encode("utf-8")
            buffer.extend(error)
            return
        
        # Make list_datasets return an array of strings instead of printing
        manager: Manager = proj["manager"]
        manager.list_datasets()

def handle_start_mcp(projects: list[dict], start_mcp: StartMCPCommand, buffer: bytearray):
    project_name = start_mcp.project
    
    project = find_project(projects, project_name)
    if project is None:
        error = f"[profyl] ERROR: Could not find project with name: {project_name}".encode("utf-8")
        buffer.extend(error)
        return
        
    manager: Manager = project["manager"]
    manager.start_mcp()
    
    message = "[profyl] SUCCESS: MCP server started successfully".encode("utf-8")
    buffer.extend(message)

def handle_schema_map(projects: list[dict], schema_map: SchemaMapCommand, buffer: bytearray):
    # THIS IS WRONG, HAVE TO FIGURE OUT HOW TO CAUSE THE AI TO PROMPT BY CALLING THE METHOD IN .start_mcp()
    num_samples = schema_map.num_samples
    project_name = schema_map.project
    
    project = find_project(projects, project_name)
    if project is None:
        error = f"[profyl] ERROR: Could not find project with name: {project_name}".encode("utf-8")
        buffer.extend(error)
        return
    
    manager: Manager = project["manager"]
    manager.build_schema_map_payload(num_samples)