import os, sys
try:
    from MonetDBtesting import process
except ImportError:
    import process

d = os.environ['RELSRCDIR']

def client(file, user, passwd):
    sys.stdout.flush()
    sys.stderr.flush()
    c = process.client(lang = 'sql',
                       user = user, passwd = passwd,
                       args = [os.path.join(d, os.pardir, file)],
                       log = True)
    c.communicate()

client('VOCschema.sql', 'monetdb', 'monetdb')
