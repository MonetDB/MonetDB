import sys
try:
    from MonetDBtesting import process
except ImportError:
    import process

c = process.client(lang = 'sql',
                   args = ['-fsql',
                           '-s', 'create table t2589 as select * from tables with data'],
                   stdout = process.PIPE, stderr = process.PIPE)
out, err = c.communicate()
sys.stdout.write(out)
sys.stderr.write(err)

c = process.client(lang = 'sql',
                   args = ['-fsql',
                           '-s', 'drop table t2589'],
                   stdout = process.PIPE, stderr = process.PIPE)
out, err = c.communicate()
sys.stdout.write(out)
sys.stderr.write(err)
