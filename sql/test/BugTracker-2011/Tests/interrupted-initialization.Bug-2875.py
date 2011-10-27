import os, sys
try:
    from MonetDBtesting import process
except ImportError:
    import process

dbname = os.getenv('TSTDB') + '-bug2875'

s = process.server(stdin = process.PIPE,
                   stdout = process.PIPE,
                   stderr = process.PIPE,
                   dbname = dbname)
out, err = s.communicate()
sys.stdout.write(out)
sys.stderr.write(err)
s = process.server(stdin = process.PIPE,
                   stdout = process.PIPE,
                   stderr = process.PIPE,
                   dbname = dbname)
c = process.client(lang = 'sqldump',
                   stdin = process.PIPE,
                   stdout = process.PIPE,
                   stderr = process.PIPE,
                   dbname = dbname)
out, err = c.communicate()
sys.stdout.write(out)
sys.stderr.write(err)
out, err = s.communicate()
sys.stdout.write(out)
sys.stderr.write(err)
