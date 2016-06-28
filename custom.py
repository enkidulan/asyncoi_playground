import asyncio
import uvloop
import httptools
import aiopg
import ujson
from functools import partial


dumps = ujson.dumps
qu = """
INSERT INTO tbl (data)
VALUES ('{}');
"""
trans = """
START TRANSACTION ISOLATION LEVEL READ COMMITTED;
BEGIN;
{}
COMMIT;
"""
# create unlogged table tbl (ID SERIAL PRIMARY KEY, data Json);
# ALTER TABLE tbl SET (autovacuum_enabled = false, toast.autovacuum_enabled = false);



async def consume():
    conn = await aiopg.connect(
        database='warehouse',
        user='enkidulan',
        password='secret',
        host='127.0.0.1')
    cnt = 0
    qw = []
    append = qw.append
    fformat = qu.format
    # async with pool.acquire() as conn:
    while True:
        value = await queue.get()
        # print(value)
        cnt += 1
        append(fformat(value))
        if cnt >= 10000:
            async with conn.cursor() as cur:
                await cur.execute(trans.format("\n".join(qw)))
            qw[:] = []
            cnt = 0


class HttpProtocol(asyncio.Protocol):
    __slots__ = (
        '_loop', '_transport', '_request', '_parser',
        '_url', '_headers')

    def __init__(self, loop):
        self._loop = loop
        self._reset()

    def _reset(self):
        self._request = None
        self._parser = None
        self._headers = []
        self._url = None

    def on_url(self, url):
        self._url = url

    def on_header(self, name, value):
        self._headers.append((name, value))

    def on_headers_complete(self):
        self._request = (
            self._url, self._headers,
            self._parser.get_http_version())

        # self._loop.call_soon(handle, self._request)
        # asyncio.ensure_future(queue.put(dumps(self._request)))
        self._loop.create_task(queue.put(dumps(self._request)))

        self._reset()

    def connection_made(self, transport):
        # peername = transport.get_extra_info('peername')
        # print('Connection from {}'.format(peername))
        self._transport = transport

    def connection_lost(self, exc):
        self._transport = None

    def data_received(self, data):
        # message = data.decode()
        # print('Data received: {!r}'.format(message))
        self._parser = httptools.HttpRequestParser(self)
        self._parser.feed_data(data)
        # resp = b''.join([
        #     'HTTP/1.1 200 OK\r\n'.encode('latin-1'),
        #     b'Content-Type: text/plain\r\n',
        #     'Content-Length: {}\r\n'.format(len(data)).encode('latin-1'),
        #     b'\r\n',
        #     data
        # ])
        resp = b''.join([
            'HTTP/1.1 200 OK\r\n'.encode('latin-1'),
            b'Content-Type: text/plain\r\n',
            'Content-Length: 0\r\n'.encode('latin-1'),
            b'\r\n'
        ])

        self._transport.write(resp)
        self._transport.close()
        self._transport = None

asyncio.set_event_loop_policy(uvloop.EventLoopPolicy())
loop = asyncio.get_event_loop()
queue = asyncio.Queue(loop=loop)
loop.create_task(consume())

addr = ('127.0.0.1', 8888)
app = loop.create_server(lambda: HttpProtocol(loop=loop), *addr)

_app = lambda: app

if __name__ == '__main__':
    server = loop.run_until_complete(app)
    try:
        loop.run_forever()
    finally:
        server.close()
        loop.close()


# message = 'Hello World!'
# coro = loop.create_connection(lambda: EchoClientProtocol(message, loop),
#                               '127.0.0.1', 8888)
# loop.run_until_complete(coro)
# loop.run_forever()
# loop.close()
