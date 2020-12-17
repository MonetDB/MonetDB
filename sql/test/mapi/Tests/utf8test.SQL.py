import sys
import locale
try:
    from MonetDBtesting import process
except ImportError:
    import process

def client(args, encoding=None, format=None):
    with process.client('sql', args=args, echo=False,
                        stdout=process.PIPE, stderr=process.PIPE,
                        encoding=encoding, format=format) as clt:
        return clt.communicate()

def printit(file, string):
    string = string.replace('\r', '')
    file.write(string)
    if not string.endswith('\n'):
        file.write('\n')

funny = u'\u00e0\u00e1\u00e2\u00e3\u00e4\u00e5'

text1 = 'value without special characters'
text2 = 'funny characters: %s' % funny

expectraw = f'''% sys.utf8test # table_name
% s # name
% varchar # type
% 32 # length
[ "{text1}"\t]
[ "{text2}"\t]
'''
expectsql = f'''+----------------------------------+
| s                                |
+==================================+
| {text1} |
| {text2}         |
+----------------------------------+
2 tuples
'''
expecterr = 'write error: stream stdout: invalid multibyte sequence\n'

out, err = client(['-s', 'create table utf8test (s varchar(50))'])
out, err = client(['-s', f"insert into utf8test values ('{text1}')"])
out, err = client(['-s', f"insert into utf8test values ('{text2}')"], encoding=locale.getpreferredencoding())
out, err = client(['-s', 'select * from utf8test'], encoding='utf-8', format='raw')
if out != expectraw:
    sys.stdout.write('utf-8, raw:\n')
    sys.stdout.write(out)
out, err = client(['-s', 'select * from utf8test'], encoding='utf-8', format='sql')
if out != expectsql:
    sys.stdout.write('utf-8, sql:\n')
    sys.stdout.write(out)
out, err = client(['-s', 'select * from utf8test'],
                  encoding='iso-8859-1', format='raw')
if out != expectraw:
    sys.stdout.write('iso-8859-1, raw:\n')
    sys.stdout.write(out)
out, err = client(['-s', 'select * from utf8test'],
                  encoding='iso-8859-1', format='sql')
if out != expectsql:
    sys.stdout.write('iso-8859-1, sql:\n')
    sys.stdout.write(out)
out, err = client(['-s', 'select * from utf8test'],
                  encoding='us-ascii', format='raw')
if err != expecterr:
    sys.stdout.write('us-ascii, raw:\n')
    sys.stdout.write(err)
out, err = client(['-s', 'select * from utf8test'],
                  encoding='us-ascii', format='sql')
if err != expecterr:
    sys.stdout.write('us-ascii, sql:\n')
    sys.stdout.write(err)
out, err = client(['-s', 'drop table utf8test'])
