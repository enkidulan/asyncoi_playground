import bottle
from bottle import route, run
# from gevent import monkey; monkey.patch_socket()

import gevent
import gevent.queue
from gevent.pywsgi import WSGIServer
import ujson
# import psycogreen.eventlet
import psycopg2
import pyramid
# psycogreen.eventlet.patch_psycopg()
# from gevent import monkey; monkey.patch_socket()
from wsgiref.simple_server import make_server
from pyramid.config import Configurator
from pyramid.response import Response


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


def get_resp(registry):
    with registry['gconn'].cursor() as cur:
        cur.execute("SELECT uuid_generate_v4 from t_random where uuid_generate_v4='888f1a6d-2aab-4a06-ae46-b227cf6d85db'")
        rez = cur.fetchone()
        return bytes(rez[0], 'utf-8')


def hello_world(request):
    request.registry['write_queue'].put(5)
    return Response(get_resp(request.registry))


def app():
    gevent.spawn(writer, write_queue)
    gconn = psycopg2.connect(
        database='warehouse',
        port='5433',
        user='mshalenyi',
        password='secret',
        host='127.0.0.1',)

    config = Configurator()
    config.add_route('hello', '/')
    config.registry['gconn'] = gconn
    config.registry['write_queue'] = write_queue
    config.add_view(hello_world, route_name='hello')
    app = config.make_wsgi_app()
    return app
