import os, sys, time
from MonetDBtesting import process

RELSRCDIR = os.environ['RELSRCDIR']
TSTDB = os.environ['TSTDB']

def client(args):
    t = time.strftime('# %H:%M:%S >',time.localtime(time.time()))
    Mlog = "\n%s\n%s  mclient %s\n%s\n\n" % (t, t, ' '.join(args), t)
    sys.stdout.write(Mlog)
    sys.stdout.flush()
    sys.stderr.write(Mlog)
    sys.stderr.flush()
    clt = process.client('sql', args = args)
    clt.communicate()

os.environ['LANG'] = 'en_US.UTF-8'

client(['-d', TSTDB,
        os.path.join(RELSRCDIR, 'local_temp_table_data.SF-1865953.sql')])
client(['-d', TSTDB])
