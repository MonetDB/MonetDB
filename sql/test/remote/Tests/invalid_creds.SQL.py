from __future__ import print_function

import os
import socket
import sys
import tempfile
import threading

import pymonetdb

try:
    from MonetDBtesting import process
except ImportError:
    import process

NWORKERS = 2
TMPDIR = tempfile.mkdtemp()

MOVIES_TABLE_DEF = ''' (
    movie_id BIGINT PRIMARY KEY,
    release_year INT,
    title VARCHAR(200)
)
'''

RATINGS_TABLE_DEF = ''' (
    movie_id BIGINT,
    customer_id BIGINT,
    rating TINYINT,
    rating_date DATE
)
'''

# Complicated FK constraints on merge/remote tables do not work great
# currently (May 2018)
RATINGS_TABLE_DEF_FK = ''' (
    movie_id BIGINT,
    customer_id BIGINT,
    rating TINYINT,
    rating_date DATE
)
'''

# Find a free network port
def freeport():
    sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    sock.bind(('', 0))
    port = sock.getsockname()[1]
    sock.close()
    return port

# Create the remote tables and load the data. Note: the supervisor
# database should be already started and should contain the movies
# table.
def worker_load(in_filename, workerrec, cmovies, ratings_table_def_fk):
    c = workerrec['conn']
    screateq = "CREATE TABLE ratings {}".format(ratings_table_def_fk)
    load_data = "COPY INTO ratings FROM '{}' USING DELIMITERS ',','\n'".format(in_filename)
    c.execute(cmovies)
    c.execute(screateq)
    c.execute(load_data)

# Setup and start workers
def create_workers(fn_template, nworkers, cmovies, ratings_table_def_fk):
    workers = []
    for i in range(nworkers):
        workerport = freeport()
        workerdbname = 'worker_{}'.format(i)
        workerrec = {
            'num': i,
            'port': workerport,
            'dbname': workerdbname,
            'dbfarm': os.path.join(TMPDIR, workerdbname),
            'mapi': 'mapi:monetdb://localhost:{}/{}/sys/ratings'.format(workerport, workerdbname),
        }
        os.mkdir(workerrec['dbfarm'])
        workerrec['proc'] = process.server(mapiport=workerrec['port'], dbname=workerrec['dbname'], dbfarm=workerrec['dbfarm'], stdin=process.PIPE, stdout=process.PIPE)
        workerrec['conn'] = pymonetdb.connect(database=workerrec['dbname'], port=workerport, autocommit=True)
        filename = fn_template.format(workerrec['num'])
        t = threading.Thread(target=worker_load, args=[filename, workerrec, cmovies, ratings_table_def_fk])
        t.start()
        workerrec['loadthread'] = t
        workers.append(workerrec)

    for wrec in workers:
        wrec['loadthread'].join()

    return workers

# Start supervisor database
supervisorport = freeport()
os.mkdir(os.path.join(TMPDIR, "supervisor"))
try:
    supervisorproc = process.server(mapiport=supervisorport, dbname="supervisor", dbfarm=os.path.join(TMPDIR, "supervisor"), stdin=process.PIPE, stdout=process.PIPE)
    supervisorconn = pymonetdb.connect(database='supervisor', port=supervisorport, autocommit=True)
    supervisor_uri = "mapi:monetdb://localhost:{}/supervisor".format(supervisorport)
    c = supervisorconn.cursor()

    # Create the movies table and load the data
    movies_filename=os.getenv("TSTDATAPATH")+"/netflix_data/movies.csv"
    movies_create = "CREATE TABLE movies {}".format(MOVIES_TABLE_DEF)
    c.execute(movies_create)
    load_movies = "COPY INTO movies FROM '{}' USING DELIMITERS ',','\n','\"'".format(movies_filename)
    c.execute(load_movies)

    # Declare the ratings merge table on supervisor
    mtable = "CREATE MERGE TABLE ratings {}".format(RATINGS_TABLE_DEF)
    c.execute(mtable)

    # Create the workers and load the ratings data
    fn_template=os.getenv("TSTDATAPATH")+"/netflix_data/ratings_sample_{}.csv"
    cmovies = "CREATE REMOTE TABLE movies {} ON '{}' WITH USER 'nonexistent' PASSWORD 'badpass'".format(MOVIES_TABLE_DEF, supervisor_uri)
    try:
        workers = create_workers(fn_template, NWORKERS, cmovies, RATINGS_TABLE_DEF_FK)

        # Create the remote tables on supervisor
        for wrec in workers:
            rtable = "CREATE REMOTE TABLE ratings{} {} on '{}' WITH USER 'invaliduser' PASSWORD 'invalidpass'".format(wrec['num'], RATINGS_TABLE_DEF, wrec['mapi'])
            c.execute(rtable)

            atable = "ALTER TABLE ratings add table ratings{}".format(wrec['num'])
            c.execute(atable)

        # Run the queries
        try:
            c.execute("SELECT COUNT(*) FROM ratings0")
            print("{} rows in remote table".format(c.fetchall()[0][0]))
        except pymonetdb.OperationalError as e1:
            print("OperationalError:", file=sys.stderr)
            print("# " + e1.args[0], file=sys.stderr)

        try:
            c.execute("SELECT COUNT(*) FROM ratings")
            print("{} rows in merge table".format(c.fetchall()[0][0]))
        except pymonetdb.OperationalError as e2:
            print("OperationalError:", file=sys.stderr)
            print("# " + e2.args[0], file=sys.stderr)
    finally:
        for wrec in workers:
            wrec['proc'].communicate()
finally:
    supervisorproc.communicate()
