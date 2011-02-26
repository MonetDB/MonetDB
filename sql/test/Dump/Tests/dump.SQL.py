import os, sys
try:
    from MonetDBtesting import process
except ImportError:
    import process

p = process.client('sqldump', stdout = process.PIPE, stderr = process.PIPE)
dump, err = p.communicate()

f = open(os.path.join(os.environ['TSTTRGDIR'], 'dumpoutput.sql'), 'w')
f.write(dump)
f.close()

sys.stdout.write(dump)
sys.stderr.write(err)
