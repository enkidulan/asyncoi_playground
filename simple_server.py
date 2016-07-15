import asyncio
import functools

trans = """
START TRANSACTION ISOLATION LEVEL READ COMMITTED;
BEGIN;
{}
COMMIT;
"""
insert = """
INSERT INTO tbl (data)
VALUES ('{}');
"""


class DWLogger():
    def __init__(self, loop):
        self._loop = loop

    async def log(self, data):
        if self.queue.full():
            self.purge_queue()
        await self.queue.put(data)

    def purge_queue(self):
        async with conn.cursor() as cur:
            await cur.execute(self.get_insert_query(self.queue))

    def get_insert_query(self, queue):
        records = []
        while not queue.empty():
            records.append(insert.format(queue.get_nowait()))
        return trans.format("\n".join(records))


class EchoServerClientProtocol(asyncio.Protocol):
    def __init__(self, logger, loop):
        self._loop = loop
        self._logger = logger

    def connection_made(self, transport):
        peername = transport.get_extra_info('peername')
        print('Connection from {}'.format(peername))
        self.transport = transport

    def data_received(self, data):
        message = data.decode()
        self._logger.log(message)
        print('Data received: {!r}'.format(message))

        print('Send: {!r}'.format(message))
        self.transport.write(data)

        print('Close the client socket')
        self.transport.close()

loop = asyncio.get_event_loop()
logger = DWLogger()
# Each client connection will create a new protocol instance
coro = loop.create_server(
    functools.partial(EchoServerClientProtocol, logger=logger, loop=loop),
    '127.0.0.1',
    8888)
server = loop.run_until_complete(coro)

# Serve requests until Ctrl+C is pressed
print('Serving on {}'.format(server.sockets[0].getsockname()))
try:
    loop.run_forever()
except KeyboardInterrupt:
    pass

# Close the server
server.close()
loop.run_until_complete(server.wait_closed())
loop.close()
