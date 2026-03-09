import asyncio
from asyncio import StreamReader, StreamWriter
from datetime import datetime
import json
import pickle
import struct
from sys import argv
from typing import Any
from profyl.core.abstractions.cache import CacheType
from profyl.core.abstractions.registry import DataSourceType, Entry, Registry, RegistryType, Status
from profyl.core.caches.redis_cache import RedisCache
from profyl.core.data_sources.excel import ExcelDataSource
from profyl.core.data_sources.mongodb import MongoDBDataSource
from profyl.core.registries.dict_registry import DictRegistry
from profyl.error import StateError
from profyl.manager.manager import Manager
from profyl.pipeline import dispatch_command

class Daemon:
    def __init__(self, namespacing: bool, secret_key: str | None) -> None:
        self.projects = {}
        self.namespacing = namespacing
        self.secret_key = secret_key
    
    async def run(self, host: str, port: int):
        server = await asyncio.start_server(self.handle_connections, host, port)
        async with server:
            await server.serve_forever()
    
    async def handle_connections(self, reader: StreamReader, writer: StreamWriter):
        length_bytes = await reader.readexactly(4)
        length = struct.unpack("!I", length_bytes)[0]
        bytes = await reader.readexactly(length)
        command = pickle.loads(bytes)
        buffer = bytearray()
        dispatch_command(self, command, buffer)
        writer.write(struct.pack("!I", len(buffer)))
        writer.write(buffer)
        await writer.drain()
        writer.close()
        
    def save(self):
        serialized_projects = {}
        with open(".profyl/state.json", 'w') as f:
            for (name, details) in self.projects.items():
                serialized_projects[name] = {}
                manager: Manager = details["manager"]
                serialized_projects[name]["registry"] = {}
                if isinstance(manager.reg, DictRegistry):
                    serialized_projects[name]["registry"]["type"] = RegistryType.Dict
                if isinstance(manager.cache, RedisCache):
                    serialized_projects[name]["cache_type"] = CacheType.Redis
                    
                serialized_projects[name]["registry"]["entries"] = {}
                entries = manager.reg.get_all()
                for (key_name, entry_details) in entries:
                    serialized_projects[name]["registry"]["entries"][key_name] = {}
                    if isinstance(entry_details.source, ExcelDataSource):
                        serialized_projects[name]["registry"]["entries"][key_name]["data_source_type"] = DataSourceType.Excel
                    elif isinstance(entry_details.source, MongoDBDataSource):
                        serialized_projects[name]["registry"]["entries"][key_name]["data_source_type"] = DataSourceType.MongoDB
                        
                    serialized_projects[name]["registry"]["entries"][key_name]["reference"] = entry_details.reference
                    serialized_projects[name]["registry"]["entries"][key_name]["source_num"] = entry_details.source_num
                    serialized_projects[name]["registry"]["entries"][key_name]["timestamp"] = entry_details.timestamp.isoformat()
                    serialized_projects[name]["registry"]["entries"][key_name]["status"] = entry_details.status
                    
                serialized_projects[name]["authz"] = details["authz"]
            
            json.dump(serialized_projects, f)
                
    def load(self):
        with open(".profyl/state.json", 'r') as f:
            deserialized_projects: dict[str, dict[str, Any]] = json.load(f)
            for (name, details) in deserialized_projects.items():
                self.projects[name] = {}
                registry_type = details["registry"]["type"]
                match registry_type:
                    case "Dict":
                        registry = DictRegistry()
                    case _:
                        raise StateError("Invalid RegistryType defined in state.json")
                cache_type = details["cache_type"]
                match cache_type:
                    case "Redis":
                        cache = RedisCache()
                    case _:
                        raise StateError("Invalid CacheType defined in state.json")
                        
                entries: dict[str, dict[str, str]] = details["registry"]["entries"]
                for (key_name, entry_details) in entries.items():
                    reference = entry_details["reference"]
                    data_source_type = entry_details["data_source_type"]
                    match data_source_type:
                        case "Excel":
                            data_source = ExcelDataSource()
                            data_source.load(reference)
                        # case "MongoDB":
                            # This case will take more work because it takes more parameters than Excel, have to refactor
                        #   data_source = MongoDBDataSource()
                        #   data_source
                        case _:
                            raise StateError("Invalid DataSourceType defined in state.json")
                    source_num = entry_details["source_num"]
                    timestamp = entry_details["timestamp"]
                    status = entry_details["status"]
                    registry.reg[key_name] = Entry(source=data_source, source_num=int(source_num), reference=reference, timestamp=datetime.fromisoformat(timestamp), status=Status(status))
                
                self.projects[name]["manager"] = Manager(registry=registry, cache=cache)
                self.projects[name]["authz"] = details["authz"]
    
async def main(argv: list[str]):
    daemon = Daemon(bool(argv[3]), argv[4])
    try:
        await daemon.run(argv[1], int(argv[2]))
    finally:
        daemon.save()

if __name__ == "__main__":
    asyncio.run(main(argv))