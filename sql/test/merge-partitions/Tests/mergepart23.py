import sys

try:
    from MonetDBtesting import process
except ImportError:
    import process

def server_stop(s):
    out, err = s.communicate()
    sys.stdout.write(out)
    sys.stderr.write(err)

def client(input):
    c = process.client('sql', stdin = process.PIPE, stdout = process.PIPE, stderr = process.PIPE)
    out, err = c.communicate(input)
    sys.stdout.write(out)
    sys.stderr.write(err)

script1 = '''\
CREATE MERGE TABLE testme (a int, b varchar(32)) PARTITION BY RANGE ON (a);\
CREATE TABLE subtable1 (a int, b varchar(32));\
CREATE TABLE subtable2 (a int, b varchar(32));\
CREATE TABLE subtable3 (a int, b varchar(32));\
CREATE TABLE subtable4 (a int, b varchar(32));\
ALTER TABLE testme ADD TABLE subtable1 AS PARTITION BETWEEN 5 AND 10;\
CREATE MERGE TABLE anothertest (a int, b varchar(32)) PARTITION BY RANGE USING (a + 1);\
ALTER TABLE anothertest ADD TABLE subtable3 AS PARTITION BETWEEN 11 AND 20;
'''

script2 = '''\
ALTER TABLE anothertest ADD TABLE subtable1 AS PARTITION BETWEEN 11 AND 20;
'''

script3 = '''\
ALTER TABLE testme ADD TABLE subtable2 AS PARTITION BETWEEN 11 AND 20;\
ALTER TABLE anothertest ADD TABLE subtable4 AS PARTITION BETWEEN 11 AND 20;\
INSERT INTO testme VALUES (1, 'one'), (12, 'two'), (13, 'three'), (15, 'four');\
INSERT INTO anothertest VALUES (1, 'one'), (12, 'two'), (13, 'three'), (15, 'four');\
SELECT a,b FROM testme;\
SELECT a,b FROM anothertest;\
ALTER TABLE testme DROP TABLE subtable1;\
ALTER TABLE testme DROP TABLE subtable2;\
ALTER TABLE anothertest DROP TABLE subtable3;\
ALTER TABLE anothertest DROP TABLE subtable4;\
DROP TABLE testme;\
DROP TABLE subtable1;\
DROP TABLE subtable2;\
DROP TABLE anothertest;\
DROP TABLE subtable3;\
DROP TABLE subtable4;
'''

s = process.server(args = [], stdin = process.PIPE, stdout = process.PIPE, stderr = process.PIPE)
client(script1)
server_stop(s)
s = process.server(args = [], stdin = process.PIPE, stdout = process.PIPE, stderr = process.PIPE)
client(script2)
server_stop(s)
s = process.server(args = [], stdin = process.PIPE, stdout = process.PIPE, stderr = process.PIPE)
client(script3)
server_stop(s)
