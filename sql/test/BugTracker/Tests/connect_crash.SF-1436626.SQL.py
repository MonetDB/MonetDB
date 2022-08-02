import os, sys
try:
    from MonetDBtesting import process
except ImportError:
    import process

with process.client('sql', stdin = process.PIPE, stdout = process.PIPE, stderr = process.PIPE) as clt:
    out, err = clt.communicate('select 1;\n')
    sys.stdout.write(out)
    sys.stderr.write(err)
