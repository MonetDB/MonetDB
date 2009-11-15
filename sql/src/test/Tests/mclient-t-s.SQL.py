import os, sys
from MonetDBtesting import process

def client(args):
    clt = process.client('sql', args = args,
                         stdout = process.PIPE, stderr = process.PIPE)
    out, err = clt.communicate()
    sys.stdout.write(out)
    sys.stderr.write(err)

sys.stdout.write('#~BeginVariableOutput~#\n')
client(['-t', '-s', 'select 123;'])
sys.stdout.write('#~EndVariableOutput~#\n')
