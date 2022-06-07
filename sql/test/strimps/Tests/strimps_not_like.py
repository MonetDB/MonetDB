import os
import tempfile

try:
    from MonetDBtesting import process
except ImportError:
    import process
from MonetDBtesting.sqltest import SQLTestCase

# Test strimps with not like queries in the presence of null values
COUNT_NOT_LIKE_QUERY = "SELECT COUNT(*) FROM orders WHERE o_comment NOT LIKE '%%slyly%%';"

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
                              o_comment        VARCHAR(79));""").assertSucceeded()
            mdb.execute("""COPY 15000 RECORDS INTO orders from r'{}/sql/benchmarks/tpch/SF-0.01/orders.tbl' USING DELIMITERS '|','\n','"';""".format(os.getenv('TSTSRCBASE'))).assertSucceeded()
            mdb.execute("""COPY 15000 RECORDS INTO orders from r'{}/sql/benchmarks/tpch/SF-0.01/orders.tbl' USING DELIMITERS '|','\n','"';""".format(os.getenv('TSTSRCBASE'))).assertSucceeded()
            mdb.execute("""COPY 15000 RECORDS INTO orders from r'{}/sql/benchmarks/tpch/SF-0.01/orders.tbl' USING DELIMITERS '|','\n','"';""".format(os.getenv('TSTSRCBASE'))).assertSucceeded()
            mdb.execute("""COPY 15000 RECORDS INTO orders from r'{}/sql/benchmarks/tpch/SF-0.01/orders.tbl' USING DELIMITERS '|','\n','"';""".format(os.getenv('TSTSRCBASE'))).assertSucceeded()
            mdb.execute("""INSERT INTO orders VALUES
                           (1, 1, 'f', 12.2, '2020-01-01', 'foo', 'bar', 2, NULL),
                           (1, 1, 'f', 12.2, '2020-01-01', 'foo', 'bar', 2, NULL),
                           (1, 1, 'f', 12.2, '2020-01-01', 'foo', 'bar', 2, NULL);""").assertSucceeded()

            mdb.execute(COUNT_NOT_LIKE_QUERY).assertSucceeded().assertDataResultMatch([(47104,)])
        s.communicate()

    with process.server(mapiport='0', dbname='db1',
                        dbfarm=fdir,
                        stdin=process.PIPE,
                        stdout=process.PIPE,
                        stderr=process.PIPE) as s:
        with SQLTestCase() as mdb:
            # Create strimp
            mdb.connect(database='db1', port=s.dbport, username='monetdb', password='monetdb')
            mdb.execute("ALTER TABLE orders SET READ ONLY;").assertSucceeded()
            mdb.execute("CREATE IMPRINTS INDEX o_comment_strimp ON orders(o_comment);").assertSucceeded()
        s.communicate()


    with process.server(mapiport='0', dbname='db1',
                        dbfarm=fdir,
                        stdin=process.PIPE, stdout=process.PIPE, stderr=process.PIPE) as s:
        with SQLTestCase() as mdb:
            mdb.connect(database='db1', port=s.dbport, username='monetdb', password='monetdb')
            mdb.execute(COUNT_NOT_LIKE_QUERY).assertSucceeded().assertDataResultMatch([(47104,)])
        s.communicate()
