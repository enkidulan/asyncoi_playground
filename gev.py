# from gevent import socket
# from gevent.pool import Pool
# from gevent import pywsgi

# from gevent.server import StreamServer


# def handle(sock, address):
#     # import pdb; pdb.set_trace()
#     sock.send(b"Hello from a telnet!\n")
#     sock.shutdown(socket.SHUT_WR)
#     sock.close()


# def hello_world(environ, start_response):
#     start_response(200, b'')
#     yield b'<b>Hello world!</b>\n'


# pool = Pool(1000)
# # server = StreamServer(('127.0.0.1', 8888), handle, spawn=pool)
# server = pywsgi.WSGIServer(('127.0.0.1', 8888), hello_world, spawn=pool)
# server.serve_forever()

# from __future__ import print_function
import gevent
import gevent.queue
from gevent.pywsgi import WSGIServer
import ujson
# import psycogreen.eventlet
import psycopg2
# psycogreen.eventlet.patch_psycopg()
# from gevent import monkey; monkey.patch_socket()

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


def writer(queue):
    conn = psycopg2.connect(
        database='warehouse',
        port='5433',
        user='mshalenyi',
        password='secret',
        host='127.0.0.1',)
    cnt = 0
    qw = []
    fformat = qu.format
    with conn.cursor() as cur:
        while True:
            value = queue.get()
            cnt += 1
            qw.append(fformat(value))
            if cnt >= 5000:
                cur.execute(trans.format("\n".join(qw)))
                qw = []
                cnt = 0


write_queue = gevent.queue.Queue()


gconn = psycopg2.connect(
    database='warehouse',
    port='5433',
    user='mshalenyi',
    password='secret',
    host='127.0.0.1',)
gcur = gconn.cursor()


def get_resp():
    # return b'ew'
    # with gconn.cursor() as cur:
    #     cur.execute("SELECT uuid_generate_v4 from t_random where uuid_generate_v4='888f1a6d-2aab-4a06-ae46-b227cf6d85db'")
    #     rez = cur.fetchone()
    #     return bytes(rez[0], 'utf-8')

    gcur.execute("SELECT uuid_generate_v4 from t_random where uuid_generate_v4='888f1a6d-2aab-4a06-ae46-b227cf6d85db'")
    rez = gcur.fetchone()
    return bytes(rez[0], 'utf-8')


def application(env, start_response):
    start_response('200 OK', [('Content-Type', 'text/html')])
    # print(env)
    write_queue.put(5)
    yield get_resp()


def app():
    gevent.spawn(writer, write_queue)
    return application

if __name__ == '__main__':
    print('Serving on 8088...')
    WSGIServer(('', 8888), app(), log=None).serve_forever()
