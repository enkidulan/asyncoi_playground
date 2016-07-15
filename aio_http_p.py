from aiohttp import web
import asyncio
import uvloop
import httptools
import aiopg
import ujson
from aiopg.sa import create_engine

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
# create table t_random as select s, uuid_generate_v4() from generate_Series(1,50000000) s;
# CREATE UNIQUE index on t_random (uuid_generate_v4);
# http://ec2-54-165-48-43.compute-1.amazonaws.com:8880/
# dat = b'1' * 5000


async def writer(loop, queue):
    conn = await aiopg.connect(
        database='warehouse',
        port='5433',
        user='mshalenyi',
        password='secret',
        host='127.0.0.1',
        loop=loop)
    cnt = 0
    qw = []
    fformat = qu.format
    async with conn.cursor() as cur:
        while True:
            value = await queue.get()
            # print(value)
            cnt += 1
            qw.append(fformat(value))
            if cnt >= 5000:
                await cur.execute(trans.format("\n".join(qw)))
                qw = []
                cnt = 0



async def handler(request):
    # loop.create_task(reader(loop=loop))
    # import pdb; pdb.set_trace()
    # asyncio.ensure_future
    await request.app.write_queue.put(3)
    async with request.app.pgconn.acquire() as conn:
        async with conn.cursor() as cur:
            await cur.execute("SELECT uuid_generate_v4 from t_random where uuid_generate_v4='888f1a6d-2aab-4a06-ae46-b227cf6d85db'")
            # import pdb; pdb.set_trace()
            rez = await cur.fetchone()
    # dat = bytes(rez[0].hex, 'utf-8')
    return web.Response(text=rez[0].hex)

loop = asyncio.get_event_loop()
write_queue = asyncio.Queue(loop=loop)

loop.create_task(writer(loop=loop, queue=write_queue))

pgconn = aiopg.create_pool(
        database='warehouse',
        port='5433',
        user='mshalenyi',
        password='secret',
        host='127.0.0.1',
        loop=loop)

app = web.Application(loop=loop)
app.write_queue = write_queue
app.pgconn = loop.run_until_complete(pgconn)
# app.pgcur = loop.run_until_complete(app.pgconn.cursor(loop=loop))
app.router.add_route('GET', '/', handler)
