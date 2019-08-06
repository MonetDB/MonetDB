import sys

try:
    from MonetDBtesting import process
except ImportError:
    import process


s = process.server(args=[], stdin=process.PIPE, stdout=process.PIPE, stderr=process.PIPE)
c1 = process.client('sql', stdin=process.PIPE, stdout=process.PIPE, stderr=process.PIPE)
c2 = process.client('sql', stdin=process.PIPE, stdout=process.PIPE, stderr=process.PIPE)

script1 = '''
CREATE TABLE tab1 (col1 tinyint);\
INSERT INTO tab1 VALUES (1);\
SELECT col1 FROM tab1;\
ALTER TABLE tab1 RENAME TO tab2;\
SELECT col1 FROM tab2;\
CREATE SCHEMA s2;\
ALTER SCHEMA s2 RENAME TO s3;\
CREATE TABLE s3.tab3 (col1 tinyint);\
INSERT INTO s3.tab3 VALUES (1);\
SELECT col1 FROM s3.tab3;\
CREATE TABLE tab4 (col1 tinyint, col3 int);\
ALTER TABLE tab4 RENAME COLUMN col1 TO col2;\
SELECT col2 FROM tab4;\
CREATE SCHEMA s4;\
CREATE TABLE tab5 (col1 int);\
INSERT INTO tab5 VALUES (1);\
ALTER TABLE tab5 SET SCHEMA s4;\
SELECT col1 FROM s4.tab5;
'''

script2 = '''
SELECT col1 FROM tab2;\
SELECT col1 FROM s3.tab3;\
SELECT col2 FROM tab4;\
SELECT col1 FROM s4.tab5;
'''

script3 = '''
DROP SCHEMA s3 CASCADE;\
DROP TABLE tab2;\
ALTER TABLE tab4 DROP COLUMN col2;\
DROP TABLE tab4;\
DROP TABLE s4.tab5;\
DROP SCHEMA s4 CASCADE;
'''

out, err = c1.communicate(script1)
sys.stdout.write(out)
sys.stderr.write(err)

c3 = process.client('sql', stdin=process.PIPE, stdout=process.PIPE, stderr=process.PIPE)

out, err = c2.communicate(script2)
sys.stdout.write(out)
sys.stderr.write(err)

out, err = c3.communicate(script2)
sys.stdout.write(out)
sys.stderr.write(err)

c4 = process.client('sql', stdin=process.PIPE, stdout=process.PIPE, stderr=process.PIPE)

out, err = c4.communicate(script3)
sys.stdout.write(out)
sys.stderr.write(err)

out, err = s.communicate()
sys.stdout.write(out)
sys.stderr.write(err)
