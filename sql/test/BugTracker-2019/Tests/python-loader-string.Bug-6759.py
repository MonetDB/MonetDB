import sys

try:
    from MonetDBtesting import process
except ImportError:
    import process

with process.client(lang = 'sql', stdin = process.PIPE, stdout = process.PIPE, stderr = process.PIPE) as c:
    out, err = c.communicate('''\
CREATE LOADER json_loader() LANGUAGE PYTHON {
    import json
    _emit.emit(json.loads('{"col1": ["apple", "peer"], "col2": ["orange", "banana nananana"]}'))
};
CREATE TABLE tbl FROM LOADER json_loader();
SELECT * FROM tbl;
''')
    sys.stdout.write(out)
    sys.stderr.write(err)

with process.client(lang = 'sqldump', stdin = process.PIPE, stdout = process.PIPE, stderr = process.PIPE) as c:
    out, err = c.communicate()
    sys.stdout.write(out)
    sys.stderr.write(err)

with process.client(lang = 'sql', stdin = process.PIPE, stdout = process.PIPE, stderr = process.PIPE) as c:
    out, err = c.communicate('''\
DROP TABLE tbl;
DROP LOADER json_loader;
''')
    sys.stdout.write(out)
    sys.stderr.write(err)
