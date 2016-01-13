"""
Test if server doesn't crash when remote and local table definitions do not match

Current result is an mal error (compilation failed)
"""

import os, sys, socket, glob, monetdb.sql, threading, time, codecs, shutil, tempfile
try:
    from MonetDBtesting import process
except ImportError:
    import process

nworkers = 1

shardtable = 'sometable'

shardedtabledef = """ (
a integer
)
"""

shardedtabledefslightlydifferent = """ (
a bigint
)
"""

tabledata = """
INSERT INTO %SHARD% VALUES (42);
"""

def freeport():
    sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    sock.bind(('', 0))
    port = sock.getsockname()[1]
    sock.close()
    return port

tmpdir = tempfile.mkdtemp()

try:
    masterport = freeport()
    masterproc = process.server(mapiport=masterport, dbname="master", dbfarm=os.path.join(tmpdir, 'master'), stdin = process.PIPE, stdout = process.PIPE)
    masterconn = monetdb.sql.connect(database='', port=masterport, autocommit=True)

    # load data (in parallel)
    def worker_load(workerrec):
        c = workerrec['conn'].cursor()
        stable = shardtable + workerrec['tpf']

        screateq = 'create table ' + stable + ' ' + shardedtabledefslightlydifferent;
        c.execute(screateq)
        c.execute(tabledata.replace("%SHARD%", stable))


    # setup and start workers
    workers = []
    for i in range(nworkers):
        workerport = freeport()
        workerdbname = 'worker_' + str(i)
        workerrec = {
            'no'       : i,
            'port'     : workerport,
            'dbname'   : workerdbname,
            'dbfarm'   : os.path.join(tmpdir, workerdbname),
            'mapi'     : 'mapi:monetdb://localhost:'+str(workerport)+'/' + workerdbname,
            'tpf'      : '_' + str(i)
        }
        workerrec['proc'] = process.server(mapiport=workerrec['port'], dbname=workerrec['dbname'], dbfarm=workerrec['dbfarm'], stdin = process.PIPE, stdout = process.PIPE)
        workerrec['conn'] = monetdb.sql.connect(database=workerrec['dbname'], port=workerrec['port'], autocommit=True)
        t = threading.Thread(target=worker_load, args = [workerrec])
        t.start()
        workerrec['loadthread'] = t
        workers.append(workerrec)



    # wait until they are finished loading
    for workerrec in workers:
        workerrec['loadthread'].join()

    # glue everything together on the master
    mtable = 'create merge table ' + shardtable + ' ' + shardedtabledef
    c = masterconn.cursor()
    c.execute(mtable)
    for workerrec in workers:
        rtable = 'create remote table ' +  shardtable + workerrec['tpf'] + ' ' + shardedtabledef + ' on \'' + workerrec['mapi'] + '\''
        atable = 'alter table ' + shardtable + ' add table ' + shardtable + workerrec['tpf'];
        c.execute(rtable)
        c.execute(atable)

    c.execute("select * from " + shardtable + workers[0]['tpf'] )
    print str(c.fetchall())

finally:
    if os.path.exists(tmpdir):
        shutil.rmtree(tmpdir)
