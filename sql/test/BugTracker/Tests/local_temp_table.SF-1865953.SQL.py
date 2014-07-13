import os, sys
try:
    from MonetDBtesting import process
except ImportError:
    import process

def client(args):
    clt = process.client('sql', args = args, log = True,
                         stdout = process.PIPE, stderr = process.PIPE)
    out, err = clt.communicate()
    sys.stdout.write(out)
    sys.stderr.write(err)

client([os.path.join(os.environ['RELSRCDIR'],
                     'local_temp_table_data.SF-1865953.sql')])
client([os.devnull])
