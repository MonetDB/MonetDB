import os
import tempfile
import threading
import socket
import sys

import pymonetdb

try:
    from MonetDBtesting import process
except ImportError:
    import process

NWORKERS = 2

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
    load_data = "COPY INTO ratings FROM r'{}' USING DELIMITERS ',','\n'".format(in_filename)
    c.execute(cmovies)
    c.execute(screateq)
    c.execute(load_data)

# Setup and start workers
def create_workers(tmpdir, workers, fn_template, nworkers, cmovies, ratings_table_def_fk):
    for i in range(nworkers):
        workerport = freeport()
        workerdbname = 'worker_{}'.format(i)
        workerrec = {
            'num': i,
            'port': workerport,
            'dbname': workerdbname,
            'dbfarm': os.path.join(tmpdir, workerdbname),
            'mapi': 'mapi:monetdb://localhost:{}/{}/sys/ratings'.format(workerport, workerdbname),
        }
        workers.append(workerrec)
        os.mkdir(workerrec['dbfarm'])
        workerrec['proc'] = process.server(mapiport=workerrec['port'], dbname=workerrec['dbname'], dbfarm=workerrec['dbfarm'], stdin=process.PIPE, stdout=process.PIPE, stderr=process.PIPE)
        workerrec['conn'] = pymonetdb.connect(database=workerrec['dbname'], port=workerport, autocommit=True)
        filename = fn_template.format(workerrec['num'])
        t = threading.Thread(target=worker_load, args=[filename, workerrec, cmovies, ratings_table_def_fk])
        t.start()
        workerrec['loadthread'] = t

    for wrec in workers:
        wrec['loadthread'].join()

# Start supervisor database
supervisorport = freeport()
supervisorproc = None
workers = []
with tempfile.TemporaryDirectory() as tmpdir:
    os.mkdir(os.path.join(tmpdir, "supervisor"))
    with process.server(mapiport=supervisorport, dbname="supervisor",
                        dbfarm=os.path.join(tmpdir, "supervisor"),
                        stdin=process.PIPE,
                        stdout=process.PIPE,
                        stderr=process.PIPE) as supervisorproc:
        supervisorconn = pymonetdb.connect(database='supervisor', port=supervisorport, autocommit=True)
        supervisor_uri = "mapi:monetdb://localhost:{}/supervisor".format(supervisorport)
        c = supervisorconn.cursor()

        # Create the movies table and load the data
        movies_filename = os.path.join(os.getenv("TSTDATAPATH"), "netflix_data", "movies.csv")
        movies_create = "CREATE TABLE movies {}".format(MOVIES_TABLE_DEF)
        c.execute(movies_create)
        load_movies = "COPY INTO movies FROM r'{}' USING DELIMITERS ',','\n','\"'".format(movies_filename)
        c.execute(load_movies)

        # Declare the ratings merge table on supervisor
        mtable = "CREATE MERGE TABLE ratings {}".format(RATINGS_TABLE_DEF)
        c.execute(mtable)

        # Create the workers and load the ratings data
        fn_template = os.path.join(os.getenv("TSTDATAPATH"), "netflix_data", "ratings_sample_{}.csv")
        cmovies = "CREATE REMOTE TABLE movies {} ON '{}' WITH USER 'monetdb' PASSWORD 'monetdb'".format(MOVIES_TABLE_DEF, supervisor_uri)
        try:
            create_workers(tmpdir, workers, fn_template, NWORKERS, cmovies, RATINGS_TABLE_DEF_FK)

            # Create the remote tables on supervisor
            for wrec in workers:
                rtable = "CREATE REMOTE TABLE ratings{} {} on '{}' WITH USER 'monetdb' PASSWORD 'monetdb'".format(wrec['num'], RATINGS_TABLE_DEF, wrec['mapi'])
                c.execute(rtable)

                atable = "ALTER TABLE ratings add table ratings{}".format(wrec['num'])
                c.execute(atable)

            # Run the queries
            c.execute("SELECT COUNT(*) FROM ratings0")
            if c.fetchall()[0][0] != 1000:
                sys.stderr.write("1000 rows in remote table expected")

            c.execute("SELECT COUNT(*) FROM ratings")
            if c.fetchall()[0][0] != 2000:
                sys.stderr.write("2000 rows in merge table expected")
            for wrec in workers:
                wrec['proc'].communicate()
            supervisorproc.communicate()
        finally:
            for wrec in workers:
                p = wrec.get('proc')
                if p is not None:
                    p.terminate()
