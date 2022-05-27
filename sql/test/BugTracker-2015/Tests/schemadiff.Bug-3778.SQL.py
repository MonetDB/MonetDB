"""
Test if server doesn't crash when remote and local table definitions do not match

Current result is an mal error (compilation failed)
"""

import os, sys, pymonetdb, threading, tempfile
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

# load data (in parallel)
def worker_load(workerrec):
    c = workerrec['conn'].cursor()
    stable = shardtable + workerrec['tpf']

    screateq = 'create table ' + stable + ' ' + shardedtabledefslightlydifferent;
    c.execute(screateq)
    c.execute(tabledata.replace("%SHARD%", stable))


with tempfile.TemporaryDirectory() as tmpdir:
    os.mkdir(os.path.join(tmpdir, 'master'))

    with process.server(mapiport='0', dbname="master",
                        dbfarm=os.path.join(tmpdir, 'master'),
                        stdin=process.PIPE, stdout=process.PIPE) as masterproc:
        masterconn = pymonetdb.connect(database='', port=masterproc.dbport, autocommit=True)

        try:
            # setup and start workers
            workers = []
            for i in range(nworkers):
                workerdbname = 'worker_' + str(i)
                workerrec = {
                    'no'       : i,
                    'dbname'   : workerdbname,
                    'dbfarm'   : os.path.join(tmpdir, workerdbname),
                    'tpf'      : '_{}'.format(i)
                }
                workers.append(workerrec)
                os.mkdir(workerrec['dbfarm'])
                workerrec['proc'] = process.server(mapiport='0',
                                                   dbname=workerrec['dbname'],
                                                   dbfarm=workerrec['dbfarm'],
                                                   stdin=process.PIPE,
                                                   stdout=process.PIPE)
                workerrec['port'] = workerrec['proc'].dbport
                workerrec['mapi'] = 'mapi:monetdb://localhost:{}/{}'.format(workerrec['port'], workerdbname)
                workerrec['conn'] = pymonetdb.connect(database=workerrec['dbname'],
                                                      port=workerrec['port'],
                                                      autocommit=True)
                t = threading.Thread(target=worker_load, args=[workerrec])
                t.start()
                workerrec['loadthread'] = t


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

            try:
                c.execute("select * from " + shardtable + workers[0]['tpf'] )
                sys.stderr.write('Exception expected')
            except pymonetdb.DatabaseError as e:
                if 'Exception occurred in the remote server, please check the log there' not in str(e):
                   print(str(e))
            else:
                print(str(c.fetchall()))

            c.close()
            masterproc.communicate()
            for worker in workers:
                workerrec['proc'].communicate()
        finally:
            for worker in workers:
                workerrec['conn'].close()
                p = workerrec.get('proc')
                if p is not None:
                    p.terminate()
            masterconn.close()
