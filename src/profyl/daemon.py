import asyncio
from asyncio import StreamReader, StreamWriter
from dataclasses import asdict
from enum import Enum
import pickle
import re
import struct
from sys import argv

class Daemon:
    def __init__(self) -> None:
        self.managers = {}
    
    async def run(self, host: str, port: int):
        server = await asyncio.start_server(self.handle_connections, host, port)
        async with server:
            await server.serve_forever()
    
    async def handle_connections(self, reader: StreamReader, writer: StreamWriter):
        length_bytes = await reader.read(4)
        length = struct.unpack("!I", length_bytes)[0]
        bytes = await reader.read(length)
        payload = pickle.loads(bytes)
        print(f"Payload recieved: {parse_command(payload)}")
    
async def main(argv: list[str]):
    daemon = Daemon()
    await daemon.run(argv[1], int(argv[2]))

def parse_command(payload: object):
    dict = {}
    name = type(payload).__name__
    dict["name"] = name
    dict["fields"] = {}
    for key, value in asdict(payload).items():
        dict["fields"][key] = (value.value if isinstance(value, Enum) else value)
    return dict

if __name__ == "__main__":
    asyncio.run(main(argv))