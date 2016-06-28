import os, sys, socket, glob, pymonetdb, threading, time, codecs, shutil, tempfile
try:
    from MonetDBtesting import process
except ImportError:
    import process

nworkers = 5
shardtable = 'lineorder'
repltable  = 'dwdate'

shardedtabledef = """ (
LO_ORDERKEY int,
LO_LINENUMBER int,
LO_CUSTKEY int,
LO_PARTKEY int,
LO_SUPPKEY int,
LO_ORDERDATE int,
LO_ORDERPRIORITY string,
LO_SHIPPRIORITY string,
LO_QUANTITY int,
LO_EXTENDEDPRICE int,
LO_ORDTOTALPRICE int,
LO_DISCOUNT int,
LO_REVENUE int,
LO_SUPPLYCOST int,
LO_TAX int,
LO_COMMITDATE int,
LO_SHIPMODE string)
"""

replicatedtabledef = """ (
D_DATEKEY int,
D_DATE string,
D_DAYOFWEEK string,
D_MONTH string,
D_YEAR int,
D_YEARMONTHNUM int,
D_YEARMONTH string,
D_DAYNUMINWEEK int,
D_DAYNUMINMONTH int,
D_DAYNUMINYEAR int,
D_MONTHNUMINYEAR int,
D_WEEKNUMINYEAR int,
D_SELLINGSEASON string,
D_LASTDAYINWEEKFL int,
D_LASTDAYINMONTHFL int,
D_HOLIDAYFL int,
D_WEEKDAYFL int)"""

dimensiontabledef = """
create table SUPPLIER (
S_SUPPKEY int,
S_NAME string,
S_ADDRESS string,
S_CITY string,
S_NATION string,
S_REGION string,
S_PHONE string);

create table CUSTOMER (
C_CUSTKEY int,
C_NAME string,
C_ADDRESS string,
C_CITY string,
C_NATION string,
C_REGION string,
C_PHONE string,
C_MKTSEGMENT string);

create table PART (
P_PARTKEY int,
P_NAME string,
P_MFGR string,
P_CATEGORY string,
P_BRAND1 string,
P_COLOR string,
P_TYPE string,
P_SIZE int,
P_CONTAINER string);
"""

def freeport():
    sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    sock.bind(('', 0))
    port = sock.getsockname()[1]
    sock.close()
    return port

ssbmpath = os.path.join(os.environ['TSTSRCBASE'], 'sql', 'benchmarks', 'ssbm', 'Tests')
ssbmdatapath = os.path.join(ssbmpath, 'SF-0.01')
tmpdir = tempfile.mkdtemp()

masterport = freeport()
masterproc = process.server(mapiport=masterport, dbname="master", dbfarm=os.path.join(tmpdir, 'master'), stdin = process.PIPE, stdout = process.PIPE)
masterconn = pymonetdb.connect(database='', port=masterport, autocommit=True)

# split lineorder table into one file for each worker
# this is as portable as an anvil
lineordertbl = os.path.join(ssbmdatapath, 'lineorder.tbl')
lineorderdir = os.path.join(tmpdir, 'lineorder')
if os.path.exists(lineorderdir):
    shutil.rmtree(lineorderdir)
if not os.path.exists(lineorderdir):
    os.makedirs(lineorderdir)
inputData = open(lineordertbl, 'r').read().split('\n')
linesperslice = len(inputData) / nworkers + 1
i = 0
for lines in range(0, len(inputData), linesperslice):
    outputData = inputData[lines:lines+linesperslice]
    outputStr = '\n'.join(outputData)
    if outputStr[-1] != '\n':
        outputStr += '\n'
    outputFile = open(os.path.join(lineorderdir, 'split-%d' % i), 'w')
    outputFile.write(outputStr)
    outputFile.close()
    i += 1
loadsplits =  glob.glob(os.path.join(lineorderdir, 'split-*'))
loadsplits.sort()

# load data (in parallel)
def worker_load(workerrec):
    c = workerrec['conn'].cursor()
    stable = shardtable + workerrec['tpf']

    screateq = 'create table %s %s' % (stable, shardedtabledef)
    sloadq = 'copy into %s from \'%s\'' % (stable, workerrec['split'].replace('\\', '\\\\'))
    c.execute(screateq)
    c.execute(sloadq)

    rtable = repltable + workerrec['tpf']
    rcreateq = 'create table %s %s' % (rtable, replicatedtabledef)
    rloadq = 'copy into %s from \'%s\'' % (rtable, workerrec['repldata'].replace('\\', '\\\\'))
    c.execute(rcreateq)
    c.execute(rloadq)

# setup and start workers
workers = []
for i in range(nworkers):
    workerport = freeport()
    workerdbname = 'worker_%d' % i
    workerrec = {
        'no'       : i,
        'port'     : workerport,
        'dbname'   : workerdbname,
        'dbfarm'   : os.path.join(tmpdir, workerdbname),
        'mapi'     : 'mapi:monetdb://localhost:%d/%s' % (workerport, workerdbname),
        'split'    : loadsplits[i],
        'repldata' : os.path.join(ssbmdatapath, 'date.tbl'),
        'tpf'      : '_%d' % i
    }
    workerrec['proc'] = process.server(mapiport=workerrec['port'], dbname=workerrec['dbname'], dbfarm=workerrec['dbfarm'], stdin = process.PIPE, stdout = process.PIPE)
    workerrec['conn'] = pymonetdb.connect(database=workerrec['dbname'], port=workerrec['port'], autocommit=True)
    t = threading.Thread(target=worker_load, args = [workerrec])
    t.start()
    workerrec['loadthread'] = t
    workers.append(workerrec)

# load dimension tables into master
c = masterconn.cursor()
c.execute(dimensiontabledef)
c.execute("""
COPY INTO SUPPLIER  FROM 'PWD/supplier.tbl';
COPY INTO CUSTOMER  FROM 'PWD/customer.tbl';
COPY INTO PART      FROM 'PWD/part.tbl';
""".replace('PWD', ssbmdatapath.replace('\\', '\\\\')))

# wait until they are finished loading
for workerrec in workers:
    workerrec['loadthread'].join()

# glue everything together on the master
mtable = 'create merge table %s %s' % (shardtable, shardedtabledef)
c.execute(mtable)
rptable = 'create replica table %s %s' % (repltable, replicatedtabledef)
c.execute(rptable)
for workerrec in workers:
    rtable = 'create remote table %s%s %s on \'%s\'' % (shardtable, workerrec['tpf'], shardedtabledef, workerrec['mapi'])
    atable = 'alter table %s add table %s%s' % (shardtable, shardtable, workerrec['tpf'])
    c.execute(rtable)
    c.execute(atable)
    rtable = 'create remote table %s%s %s on \'%s\'' % (repltable, workerrec['tpf'], replicatedtabledef, workerrec['mapi'])
    atable = 'alter table %s add table %s%s' % (repltable, repltable, workerrec['tpf'])
    c.execute(rtable)
    c.execute(atable)

# sanity check
c.execute("select count(*) from lineorder")
print str(c.fetchall()[0][0]) + ' rows in mergetable'

c.execute("select * from lineorder where lo_orderkey=356")
print str(c.fetchall()[0][0])

c.execute("select * from " + shardtable + workers[0]['tpf'] + " where lo_orderkey=356")
print str(c.fetchall()[0][0])

# run queries, use mclient so output is comparable
queries = glob.glob(os.path.join(ssbmpath, '[0-1][0-9].sql'))
queries.sort()
for q in queries:
    print '# Running Q %s' % os.path.basename(q).replace('.sql','')
    mc = process.client('sql', stdin=open(q), dbname='master', host='localhost', port=masterport, stdout=process.PIPE, stderr=process.PIPE, log=1)
    out, err = mc.communicate()
    sys.stdout.write(out)
    sys.stderr.write(err)
    # old way
    # c.execute(codecs.open(q, 'r', encoding='utf8').read())
    # print c.fetchall()


for workerrec in workers:
    workerrec['proc'].communicate()

masterproc.communicate()

shutil.rmtree(tmpdir)
