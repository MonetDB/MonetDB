import os, socket, tempfile

from MonetDBtesting.sqltest import SQLTestCase

try:
    from MonetDBtesting import process
except ImportError:
    import process

def freeport():
    sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    sock.bind(('', 0))
    port = sock.getsockname()[1]
    sock.close()
    return port


with tempfile.TemporaryDirectory() as farm_dir:
    os.mkdir(os.path.join(farm_dir, 'db1'))
    myport = freeport()

    with process.server(mapiport=myport, dbname='db1',
                        dbfarm=os.path.join(farm_dir, 'db1'),
                        stdin=process.PIPE, stdout=process.PIPE,
                        stderr=process.PIPE) as s:
        with SQLTestCase() as tc:
            tc.connect(username="monetdb", password="monetdb", port=myport, database='db1')
            tc.execute('CREATE MERGE TABLE testme (a int, b varchar(32)) PARTITION BY RANGE ON (a);').assertSucceeded()
            tc.execute('CREATE TABLE subtable1 (a int, b varchar(32));').assertSucceeded()
            tc.execute('CREATE TABLE subtable2 (a int, b varchar(32));').assertSucceeded()
            tc.execute('CREATE TABLE subtable3 (a int, b varchar(32));').assertSucceeded()
            tc.execute('CREATE TABLE subtable4 (a int, b varchar(32));').assertSucceeded()
            tc.execute('CREATE TABLE subtable5 (a int, b varchar(32));').assertSucceeded()
            tc.execute('ALTER TABLE testme ADD TABLE subtable1 AS PARTITION FROM 5 TO 10;').assertSucceeded()
            tc.execute('ALTER TABLE testme ADD TABLE subtable5 AS PARTITION FOR NULL VALUES;').assertSucceeded()
            tc.execute('CREATE MERGE TABLE anothertest (a int, b varchar(32)) PARTITION BY RANGE USING (a + 1);').assertSucceeded()
            tc.execute('ALTER TABLE anothertest ADD TABLE subtable3 AS PARTITION FROM 11 TO 20;').assertSucceeded()
            tc.execute('SELECT "minimum", "maximum" FROM range_partitions;').assertSucceeded().assertDataResultMatch([("5","10"),(None,None),("11","20")])
        s.communicate()
    with process.server(mapiport=myport, dbname='db1',
                        dbfarm=os.path.join(farm_dir, 'db1'),
                        stdin=process.PIPE, stdout=process.PIPE,
                        stderr=process.PIPE) as s:
        with SQLTestCase() as tc:
            tc.connect(username="monetdb", password="monetdb", port=myport, database='db1')
            tc.execute('SELECT "minimum", "maximum" FROM range_partitions;').assertSucceeded().assertDataResultMatch([("5","10"),(None,None),("11","20")])
            tc.execute('DROP TABLE subtable1;').assertFailed(err_message='DROP TABLE: unable to drop table subtable1 (there are database objects which depend on it)') # error, subtable1 is a child of testme
            tc.execute('DROP TABLE subtable3;').assertFailed(err_message='DROP TABLE: unable to drop table subtable3 (there are database objects which depend on it)') # error, subtable3 is a child of anothertest
            tc.execute('ALTER TABLE anothertest ADD TABLE subtable1 AS PARTITION FROM 11 TO 20;').assertFailed(err_message='ALTER TABLE: table \'sys.subtable1\' is already part of another table') # error, subtable1 is part of another table
            tc.execute('SELECT "minimum", "maximum" FROM range_partitions;').assertSucceeded().assertDataResultMatch([("5","10"),(None,None),("11","20")])
        s.communicate()
    with process.server(mapiport=myport, dbname='db1',
                        dbfarm=os.path.join(farm_dir, 'db1'),
                        stdin=process.PIPE, stdout=process.PIPE,
                        stderr=process.PIPE) as s:
        with SQLTestCase() as tc:
            tc.connect(username="monetdb", password="monetdb", port=myport, database='db1')
            tc.execute('SELECT "minimum", "maximum" FROM range_partitions;').assertSucceeded().assertDataResultMatch([("5","10"),(None,None),("11","20")])
            tc.execute('ALTER TABLE testme ADD TABLE subtable2 AS PARTITION FROM 11 TO 20;').assertSucceeded()
            tc.execute('ALTER TABLE anothertest ADD TABLE subtable4 AS PARTITION FROM 21 TO 30;').assertSucceeded()
            tc.execute("INSERT INTO testme VALUES (5, 'one'), (12, 'two'), (13, 'three'), (15, 'four'), (NULL, 'five');").assertSucceeded().assertRowCount(5)
            tc.execute("INSERT INTO anothertest VALUES (11, 'one'), (12, 'two'), (13, 'three'), (15, 'four');").assertSucceeded().assertRowCount(4)
            tc.execute('SELECT a,b FROM testme;').assertSucceeded().assertDataResultMatch([(5,"one"),(None,"five"),(12,"two"),(13,"three"),(15,"four")])
            tc.execute('SELECT a,b FROM anothertest;').assertSucceeded().assertDataResultMatch([(11,"one"),(12,"two"),(13,"three"),(15,"four")])
            tc.execute('SELECT "minimum", "maximum" FROM range_partitions;').assertSucceeded().assertDataResultMatch([("5","10"),(None,None),("11","20"),("11","20"),("21","30")])
            tc.execute('ALTER TABLE testme DROP TABLE subtable1;').assertSucceeded()
            tc.execute('ALTER TABLE testme DROP TABLE subtable2;').assertSucceeded()
            tc.execute('ALTER TABLE testme DROP TABLE subtable5;').assertSucceeded()
            tc.execute('ALTER TABLE anothertest DROP TABLE subtable3;').assertSucceeded()
            tc.execute('ALTER TABLE anothertest DROP TABLE subtable4;').assertSucceeded()
            tc.execute('SELECT "minimum", "maximum" FROM range_partitions;').assertSucceeded().assertDataResultMatch([])
            tc.execute('ALTER TABLE testme DROP COLUMN "a";').assertFailed(err_message='ALTER TABLE: cannot drop column \'a\': is the partitioned column on the table \'testme\'') # error, a is a partition column
            tc.execute('ALTER TABLE anothertest DROP COLUMN "a";').assertFailed(err_message='ALTER TABLE: cannot drop column \'a\': the expression used in \'anothertest\' depends on it') # error, a is used on partition expression
            tc.execute('DROP TABLE testme;').assertSucceeded()
            tc.execute('DROP TABLE subtable1;').assertSucceeded()
            tc.execute('DROP TABLE subtable2;').assertSucceeded()
            tc.execute('DROP TABLE anothertest;').assertSucceeded()
            tc.execute('DROP TABLE subtable3;').assertSucceeded()
            tc.execute('DROP TABLE subtable4;').assertSucceeded()
            tc.execute('DROP TABLE subtable5;').assertSucceeded()
        s.communicate()
    with process.server(mapiport=myport, dbname='db1',
                        dbfarm=os.path.join(farm_dir, 'db1'),
                        stdin=process.PIPE, stdout=process.PIPE,
                        stderr=process.PIPE) as s:
        with SQLTestCase() as tc:
            tc.connect(username="monetdb", password="monetdb", port=myport, database='db1')
            tc.execute('CREATE MERGE TABLE upsme (a int, b varchar(32)) PARTITION BY VALUES USING (a + 5);').assertSucceeded()
            tc.execute('CREATE TABLE subtable1 (a int, b varchar(32));').assertSucceeded()
            tc.execute('CREATE TABLE subtable2 (a int, b varchar(32));').assertSucceeded()
            tc.execute('CREATE TABLE subtable3 (a int, b varchar(32));').assertSucceeded()
            tc.execute('ALTER TABLE upsme ADD TABLE subtable3 AS PARTITION FOR NULL VALUES;').assertSucceeded()
            tc.execute("INSERT INTO upsme VALUES (NULL, 'one');").assertSucceeded().assertRowCount(1)
            tc.execute('ALTER TABLE upsme ADD TABLE subtable1 AS PARTITION IN (15, 25, 35);').assertSucceeded()
            tc.execute('ALTER TABLE upsme ADD TABLE subtable2 AS PARTITION IN (45, 55, 65);').assertSucceeded()
            tc.execute('SELECT "value" FROM value_partitions;').assertSucceeded().assertDataResultMatch([(None,),("15",),("25",),("35",),("45",),("55",),("65",)])
        s.communicate()
    with process.server(mapiport=myport, dbname='db1',
                        dbfarm=os.path.join(farm_dir, 'db1'),
                        stdin=process.PIPE, stdout=process.PIPE,
                        stderr=process.PIPE) as s:
        with SQLTestCase() as tc:
            tc.connect(username="monetdb", password="monetdb", port=myport, database='db1')
            tc.execute("INSERT INTO upsme VALUES (10, 'two'), (40, 'three'), (NULL, 'four');").assertSucceeded().assertRowCount(3)
            tc.execute("INSERT INTO subtable3 VALUES (NULL, 'five');").assertSucceeded().assertRowCount(1)
            tc.execute('SELECT a,b FROM upsme;').assertSucceeded().assertDataResultMatch([(10,"two"),(40,"three"),(None,"one"),(None,"four"),(None,"five")])
            tc.execute('SELECT a,b FROM subtable1;').assertSucceeded().assertDataResultMatch([(10,"two")])
            tc.execute('SELECT a,b FROM subtable2;').assertSucceeded().assertDataResultMatch([(40,"three")])
            tc.execute('SELECT a,b FROM subtable3;').assertSucceeded().assertDataResultMatch([(None,"one"),(None,"four"),(None,"five")])
            tc.execute('ALTER TABLE upsme DROP TABLE subtable1;').assertSucceeded()
            tc.execute('ALTER TABLE upsme DROP TABLE subtable2;').assertSucceeded()
            tc.execute('ALTER TABLE upsme DROP TABLE subtable3;').assertSucceeded()
            tc.execute('SELECT "value" FROM value_partitions;').assertSucceeded().assertDataResultMatch([])
            tc.execute('DROP TABLE upsme;').assertSucceeded()
            tc.execute('DROP TABLE subtable1;').assertSucceeded()
            tc.execute('DROP TABLE subtable2;').assertSucceeded()
            tc.execute('DROP TABLE subtable3;').assertSucceeded()
        s.communicate()
