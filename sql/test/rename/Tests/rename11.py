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
SELECT col1 FROM tab2;
'''

script2 = '''
SELECT col1 FROM tab2;
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

out, err = s.communicate()
sys.stdout.write(out)
sys.stderr.write(err)
