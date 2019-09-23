import sys

try:
    from MonetDBtesting import process
except ImportError:
    import process


s = process.server(args = ['--set', 'embedded_py=3'], stdin = process.PIPE, stdout = process.PIPE, stderr = process.PIPE)

c = process.client(lang = 'sql', stdin = process.PIPE, stdout = process.PIPE, stderr = process.PIPE)
out, err = c.communicate('''\
CREATE LOADER json_loader() LANGUAGE PYTHON {\n\
    import json\n\
    _emit.emit(json.loads('{"col1": ["apple", "peer"], "col2": ["orange", "banana nananana"]}'))\n\
};\
CREATE TABLE tbl FROM LOADER json_loader();\
SELECT * FROM tbl;
''')
sys.stdout.write(out)
sys.stderr.write(err)

c = process.client(lang = 'sqldump', stdin = process.PIPE, stdout = process.PIPE, stderr = process.PIPE)
out, err = c.communicate()
sys.stdout.write(out)
sys.stderr.write(err)

c = process.client(lang = 'sql', stdin = process.PIPE, stdout = process.PIPE, stderr = process.PIPE)
out, err = c.communicate('''\
DROP TABLE tbl;\
DROP LOADER json_loader;
''')
sys.stdout.write(out)
sys.stderr.write(err)

out, err = s.communicate()
sys.stdout.write(out)
sys.stderr.write(err)
