import asyncio
from asyncio import StreamReader, StreamWriter
import pickle
from sys import argv

class Daemon:
    def __init__(self) -> None:
        self.managers = {}
    
    async def run(self, host: str, port: int):
        server = await asyncio.start_server(self.handle_connections, host, port)
        async with server:
            await server.serve_forever()
    
    async def handle_connections(self, reader: StreamReader, writer: StreamWriter):
        length = reader.read(4)
        bytes = reader.read(length)
        payload = pickle.loads(bytes)
        print(f"Payload recieved: {payload}")
    
async def main(argv: list[str]):
    daemon = Daemon()
    await daemon.run(argv[1], int(argv[2]))

if __name__ == "__main__":
    asyncio.run(main(argv))