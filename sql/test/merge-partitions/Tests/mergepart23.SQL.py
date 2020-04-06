import os, socket, sys, tempfile

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

def server_stop(s):
    out, err = s.communicate()
    sys.stdout.write(out)
    sys.stderr.write(err)


def client(input):
    with process.client('sql', port=myport, dbname='db1', stdin=process.PIPE, stdout=process.PIPE, stderr=process.PIPE) as c:
        out, err = c.communicate(input)
        sys.stdout.write(out)
        sys.stderr.write(err)


script1 = '''
CREATE MERGE TABLE testme (a int, b varchar(32)) PARTITION BY RANGE ON (a);
CREATE TABLE subtable1 (a int, b varchar(32));
CREATE TABLE subtable2 (a int, b varchar(32));
CREATE TABLE subtable3 (a int, b varchar(32));
CREATE TABLE subtable4 (a int, b varchar(32));
CREATE TABLE subtable5 (a int, b varchar(32));
ALTER TABLE testme ADD TABLE subtable1 AS PARTITION FROM 5 TO 10;
ALTER TABLE testme ADD TABLE subtable5 AS PARTITION FOR NULL VALUES;
CREATE MERGE TABLE anothertest (a int, b varchar(32)) PARTITION BY RANGE USING (a + 1);
ALTER TABLE anothertest ADD TABLE subtable3 AS PARTITION FROM 11 TO 20;
SELECT "minimum", "maximum" FROM range_partitions;
'''

script2 = '''
SELECT "minimum", "maximum" FROM range_partitions;
DROP TABLE subtable1; --error, subtable1 is a child of testme
DROP TABLE subtable3; --error, subtable3 is a child of anothertest
ALTER TABLE anothertest ADD TABLE subtable1 AS PARTITION FROM 11 TO 20; --error, subtable1 is part of another table
SELECT "minimum", "maximum" FROM range_partitions;
'''

script3 = '''
SELECT "minimum", "maximum" FROM range_partitions;
ALTER TABLE testme ADD TABLE subtable2 AS PARTITION FROM 11 TO 20;
ALTER TABLE anothertest ADD TABLE subtable4 AS PARTITION FROM 21 TO 30;
INSERT INTO testme VALUES (5, 'one'), (12, 'two'), (13, 'three'), (15, 'four'), (NULL, 'five');
INSERT INTO anothertest VALUES (11, 'one'), (12, 'two'), (13, 'three'), (15, 'four');
SELECT a,b FROM testme;
SELECT a,b FROM anothertest;
SELECT "minimum", "maximum" FROM range_partitions;
ALTER TABLE testme DROP TABLE subtable1;
ALTER TABLE testme DROP TABLE subtable2;
ALTER TABLE testme DROP TABLE subtable5;
ALTER TABLE anothertest DROP TABLE subtable3;
ALTER TABLE anothertest DROP TABLE subtable4;
SELECT "minimum", "maximum" FROM range_partitions;
ALTER TABLE testme DROP COLUMN "a"; --error, a is a partition column
ALTER TABLE anothertest DROP COLUMN "a"; --error, a is used on partition expression
DROP TABLE testme;
DROP TABLE subtable1;
DROP TABLE subtable2;
DROP TABLE anothertest;
DROP TABLE subtable3;
DROP TABLE subtable4;
DROP TABLE subtable5;
'''

script4 = '''
CREATE MERGE TABLE upsme (a int, b varchar(32)) PARTITION BY VALUES USING (a + 5);
CREATE TABLE subtable1 (a int, b varchar(32));
CREATE TABLE subtable2 (a int, b varchar(32));
CREATE TABLE subtable3 (a int, b varchar(32));
ALTER TABLE upsme ADD TABLE subtable3 AS PARTITION FOR NULL VALUES;
INSERT INTO upsme VALUES (NULL, 'one');
ALTER TABLE upsme ADD TABLE subtable1 AS PARTITION IN (15, 25, 35);
ALTER TABLE upsme ADD TABLE subtable2 AS PARTITION IN (45, 55, 65);
SELECT "value" FROM value_partitions;
'''

script5 = '''
INSERT INTO upsme VALUES (10, 'two'), (40, 'three'), (NULL, 'four');
INSERT INTO subtable3 VALUES (NULL, 'five');
SELECT a,b FROM upsme;
SELECT a,b FROM subtable1;
SELECT a,b FROM subtable2;
SELECT a,b FROM subtable3;
ALTER TABLE upsme DROP TABLE subtable1;
ALTER TABLE upsme DROP TABLE subtable2;
ALTER TABLE upsme DROP TABLE subtable3;
SELECT "value" FROM value_partitions;
DROP TABLE upsme;
DROP TABLE subtable1;
DROP TABLE subtable2;
DROP TABLE subtable3;
'''

with tempfile.TemporaryDirectory() as farm_dir:
    os.mkdir(os.path.join(farm_dir, 'db1'))
    myport = freeport()

    with process.server(mapiport=myport, dbname='db1',
                        dbfarm=os.path.join(farm_dir, 'db1'),
                        stdin=process.PIPE, stdout=process.PIPE,
                        stderr=process.PIPE) as s:
        client(script1)
        server_stop(s)
    with process.server(mapiport=myport, dbname='db1',
                        dbfarm=os.path.join(farm_dir, 'db1'),
                        stdin=process.PIPE, stdout=process.PIPE,
                        stderr=process.PIPE) as s:
        client(script2)
        server_stop(s)
    with process.server(mapiport=myport, dbname='db1',
                        dbfarm=os.path.join(farm_dir, 'db1'),
                        stdin=process.PIPE, stdout=process.PIPE,
                        stderr=process.PIPE) as s:
        client(script3)
        server_stop(s)
    with process.server(mapiport=myport, dbname='db1',
                        dbfarm=os.path.join(farm_dir, 'db1'),
                        stdin=process.PIPE, stdout=process.PIPE,
                        stderr=process.PIPE) as s:
        client(script4)
        server_stop(s)
    with process.server(mapiport=myport, dbname='db1',
                        dbfarm=os.path.join(farm_dir, 'db1'),
                        stdin=process.PIPE, stdout=process.PIPE,
                        stderr=process.PIPE) as s:
        client(script5)
        server_stop(s)
