import os, sys
try:
    from MonetDBtesting import process
except ImportError:
    import process

def client(args):
    clt = process.client('mil', args = args,
                         stdout = process.PIPE, stderr = process.PIPE)
    out, err = clt.communicate()
    sys.stdout.write(out)
    sys.stderr.write(err)

sys.stderr.write('#~BeginVariableOutput~#\n')
client(['-t', '-s', 'print(123);'])
sys.stderr.write('#~EndVariableOutput~#\n')
