import os, sys, socket, glob, monetdb.sql, threading, time, codecs
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

ssbmpath = os.path.join(os.environ['TSTSRCBASE'], 'sql/benchmarks/ssbm/Tests')
ssbmdatapath = os.path.join(ssbmpath, 'SF-0.01')
tmpdir = os.path.join(os.environ.get('TMPDIR', '/tmp'), 'remotetest')
os.system('rm -rf ' + tmpdir)
os.system('mkdir -p ' + tmpdir)

masterport = freeport()
masterproc = process.server(mapiport=masterport, dbname="master", dbfarm=os.path.join(tmpdir, 'master'), stdin = process.PIPE, stdout = process.PIPE)
masterconn = monetdb.sql.connect(database='', port=masterport, autocommit=True)

# split lineorder table into one file for each worker
# this is as portable as an anvil
lineorder = os.path.join(ssbmdatapath, 'lineorder.tbl')
os.system('rm -rf ' + tmpdir + '/lineorder-split-*')
os.system('split -l $(( $( wc -l < ' + lineorder + ' ) / ' + str(nworkers) + ' + 1 )) ' + lineorder + ' ' + tmpdir + '/lineorder-split-')
loadsplits =  glob.glob(os.path.join(tmpdir, 'lineorder-split-*'))

# load data (in parallel)
def worker_load(workerrec):
    c = workerrec['conn'].cursor()
    stable = shardtable + workerrec['tpf']

    screateq = 'create table ' + stable + ' ' + shardedtabledef;
    sloadq = 'copy into ' + stable + ' from \'' + workerrec['split'] + '\''
    c.execute(screateq)
    c.execute(sloadq)

    rtable = repltable + workerrec['tpf']
    rcreateq = 'create table ' + rtable  + ' '+ replicatedtabledef
    rloadq = 'copy into ' + rtable + ' from \'' + workerrec['repldata'] + '\''
    c.execute(rcreateq)
    c.execute(rloadq)

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
        'split'    : loadsplits[i],
        'repldata' : os.path.join(ssbmdatapath, 'date.tbl'),
        'tpf'      : '_' + str(i)
    }
    workerrec['proc'] = process.server(mapiport=workerrec['port'], dbname=workerrec['dbname'], dbfarm=workerrec['dbfarm'], stdin = process.PIPE, stdout = process.PIPE)
    workerrec['conn'] = monetdb.sql.connect(database=workerrec['dbname'], port=workerrec['port'], autocommit=True)
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
""".replace('PWD', ssbmdatapath))

# wait until they are finished loading
for workerrec in workers:
    workerrec['loadthread'].join()

# glue everything together on the master
mtable = 'create merge table ' + shardtable + ' ' + shardedtabledef
c.execute(mtable)
rptable = 'create replica table ' +  repltable + ' ' + replicatedtabledef
c.execute(rptable)
for workerrec in workers:
    rtable = 'create remote table ' +  shardtable + workerrec['tpf'] + ' ' + shardedtabledef + ' on \'' + workerrec['mapi'] + '\''
    atable = 'alter table ' + shardtable + ' add table ' + shardtable + workerrec['tpf'];
    c.execute(rtable)
    c.execute(atable)
    rtable = 'create remote table ' +  repltable + workerrec['tpf'] + ' ' + replicatedtabledef + ' on \'' + workerrec['mapi'] + '\''
    atable = 'alter table ' + repltable + ' add table ' + repltable + workerrec['tpf'];
    c.execute(rtable)
    c.execute(atable)

# sanity check
c.execute("select count(*) from lineorder")
print str(c.fetchall()[0][0]) + ' rows in mergetable'

# run queries
queries = glob.glob(os.path.join(ssbmpath, '[0-1][0-9].sql'))
for q in queries:
    print os.path.basename(q).replace('.sql','')
    c.execute(codecs.open(q, 'r', encoding='utf8').read())
    print c.fetchall()
