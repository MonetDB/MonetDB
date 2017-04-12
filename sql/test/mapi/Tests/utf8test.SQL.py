import sys
try:
    from MonetDBtesting import process
except ImportError:
    import process

def client(args, universal_newlines = True):
    clt = process.client('sql', args = args,
                         stdout = process.PIPE, stderr = process.PIPE,
                         universal_newlines = universal_newlines)
    return clt.communicate()

funny = u'\u00e0\u00e1\u00e2\u00e3\u00e4\u00e5'
if sys.version_info[0] == 2:
    funny = funny.encode('utf-8')

out, err = client(['-s', 'create table utf8test (s varchar(50))'])
sys.stdout.write(out)
sys.stderr.write(err)
out, err = client(['-s', "insert into utf8test values ('value without special characters')"])
sys.stdout.write(out)
sys.stderr.write(err)
out, err = client(['-s', "insert into utf8test values ('funny characters: %s')" % funny])
sys.stdout.write(out)
sys.stderr.write(err)
out, err = client(['-fraw', '-s', 'select * from utf8test'])
sys.stdout.write(out)
sys.stderr.write(err)
out, err = client(['-fsql', '-s', 'select * from utf8test'])
sys.stdout.write(out)
sys.stderr.write(err)
out, err = client(['-fraw', '-Eiso-8859-1', '-s', 'select * from utf8test'],
                  universal_newlines = False)
out = out.decode('iso-8859-1')
err = err.decode('iso-8859-1')
if sys.version_info[0] == 2:
    out = out.encode('utf-8')
    err = err.encode('utf-8')
sys.stdout.write(out)
sys.stderr.write(err)
out, err = client(['-fsql', '-Eiso-8859-1', '-s', 'select * from utf8test'],
                  universal_newlines = False)
out = out.decode('iso-8859-1')
err = err.decode('iso-8859-1')
if sys.version_info[0] == 2:
    out = out.encode('utf-8')
    err = err.encode('utf-8')
sys.stdout.write(out)
sys.stderr.write(err)
out, err = client(['-fraw', '-Eus-ascii', '-s', 'select * from utf8test'],
                  universal_newlines = False)
out = out.decode('us-ascii')
err = err.decode('us-ascii')
if sys.version_info[0] == 2:
    out = out.encode('utf-8')
    err = err.encode('utf-8')
sys.stdout.write(out)
sys.stderr.write(err)
out, err = client(['-fsql', '-Eus-ascii', '-s', 'select * from utf8test'],
                  universal_newlines = False)
out = out.decode('us-ascii')
err = err.decode('us-ascii')
if sys.version_info[0] == 2:
    out = out.encode('utf-8')
    err = err.encode('utf-8')
sys.stdout.write(out)
sys.stderr.write(err)
out, err = client(['-s', 'drop table utf8test'])
sys.stdout.write(out)
sys.stderr.write(err)
