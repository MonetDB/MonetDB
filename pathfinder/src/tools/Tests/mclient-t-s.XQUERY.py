import os, sys
from MonetDBtesting import process

def client(args):
    clt = process.client('xquery', args = args,
                         stdout = process.PIPE, stderr = process.PIPE)
    out, err = clt.communicate()
    sys.stdout.write(out)
    sys.stderr.write(err)

sys.stdout.write('#~BeginVariableOutput~#\n')
sys.stderr.write('#~BeginVariableOutput~#\n')
client(['-t', '-s', '(123)'])
sys.stdout.write('#~EndVariableOutput~#\n')
sys.stderr.write('#~EndVariableOutput~#\n')
