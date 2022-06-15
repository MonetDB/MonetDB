import os
import tempfile

try:
    from MonetDBtesting import process
except ImportError:
    import process
from MonetDBtesting.sqltest import SQLTestCase

COUNT_QUERY = "SELECT COUNT(*) FROM orders WHERE o_comment LIKE '%%slyly%%';"

# Make sure that using a strimp returns the same number of rows as
# not using it.

with tempfile.TemporaryDirectory() as farm_dir:
    fdir = os.path.join(farm_dir, 'db1')
    os.mkdir(fdir)
    with process.server(mapiport='0', dbname='db1',
                        dbfarm=fdir,
                        stdin=process.PIPE,
                        stdout=process.PIPE,
                        stderr=process.PIPE) as s:
        with SQLTestCase() as mdb:
            mdb.connect(database='db1', port=s.dbport, username='monetdb', password='monetdb')
            mdb.execute("""CREATE TABLE orders (
                              o_orderkey       BIGINT NOT NULL,
                              o_custkey        INTEGER NOT NULL,
                              o_orderstatus    CHAR(1) NOT NULL,
                              o_totalprice     DECIMAL(15,2) NOT NULL,
                              o_orderdate      DATE NOT NULL,
                              o_orderpriority  CHAR(15) NOT NULL,
                              o_clerk          CHAR(15) NOT NULL,
                              o_shippriority   INTEGER NOT NULL,
                              o_comment        VARCHAR(79) NOT NULL);""").assertSucceeded()
            mdb.execute("""COPY 15000 RECORDS INTO orders from r'{}/sql/benchmarks/tpch/SF-0.01/orders.tbl' USING DELIMITERS '|','\n','"';""".format(os.getenv('TSTSRCBASE'))).assertSucceeded()
            mdb.execute("""COPY 15000 RECORDS INTO orders from r'{}/sql/benchmarks/tpch/SF-0.01/orders.tbl' USING DELIMITERS '|','\n','"';""".format(os.getenv('TSTSRCBASE'))).assertSucceeded()
            mdb.execute("""COPY 15000 RECORDS INTO orders from r'{}/sql/benchmarks/tpch/SF-0.01/orders.tbl' USING DELIMITERS '|','\n','"';""".format(os.getenv('TSTSRCBASE'))).assertSucceeded()
            mdb.execute("""COPY 15000 RECORDS INTO orders from r'{}/sql/benchmarks/tpch/SF-0.01/orders.tbl' USING DELIMITERS '|','\n','"';""".format(os.getenv('TSTSRCBASE'))).assertSucceeded()
            mdb.execute(COUNT_QUERY).assertSucceeded().assertDataResultMatch([(12896,)])
        s.communicate()

    with process.server(mapiport='0', dbname='db1',
                        dbfarm=fdir,
                        stdin=process.PIPE, stdout=process.PIPE, stderr=process.PIPE) as s:
        with SQLTestCase() as mdb:
            mdb.connect(database='db1', port=s.dbport, username='monetdb', password='monetdb')
            # Create strimp
            mdb.execute("ALTER TABLE orders SET READ ONLY;")
            mdb.execute("CREATE IMPRINTS INDEX o_comment_strimp ON orders(o_comment);")
            mdb.execute(COUNT_QUERY).assertSucceeded().assertDataResultMatch([(12896,)])
        s.communicate()
