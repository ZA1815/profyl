import asyncio
from asyncio import StreamReader, StreamWriter
import pickle
import struct
from sys import argv
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
    
async def main(argv: list[str]):
    daemon = Daemon(bool(argv[3]), argv[4])
    await daemon.run(argv[1], int(argv[2]))

if __name__ == "__main__":
    asyncio.run(main(argv))