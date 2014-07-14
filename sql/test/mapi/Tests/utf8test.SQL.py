import sys
try:
    from MonetDBtesting import process
except ImportError:
    import process

def client(args):
    clt = process.client('sql', args = args,
                         stdout = process.PIPE, stderr = process.PIPE)
    out, err = clt.communicate()
    sys.stdout.write(out)
    sys.stderr.write(err)

client(['-s', 'create table utf8test (s varchar(50))'])
client(['-s', "insert into utf8test values ('value without special characters')"])
client(['-s', "insert into utf8test values ('funny characters: \303\240\303\241\303\242\303\243\303\244\303\245')"])
client(['-fraw', '-s', 'select * from utf8test'])
client(['-fsql', '-s', 'select * from utf8test'])
client(['-fraw', '-Eiso-8859-1', '-s', 'select * from utf8test'])
client(['-fsql', '-Eiso-8859-1', '-s', 'select * from utf8test'])
client(['-fraw', '-Eus-ascii', '-s', 'select * from utf8test'])
client(['-fsql', '-Eus-ascii', '-s', 'select * from utf8test'])
client(['-s', 'drop table utf8test'])
