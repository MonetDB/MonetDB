import os, sys
from MonetDBtesting import process

srv = process.server('sql', stdin = process.PIPE, stdout = process.PIPE, stderr = process.PIPE)

clt = process.client('sql', stdin = process.PIPE, stdout = process.PIPE, stderr = process.PIPE)
out, err = clt.communicate('select 1;\n')
sys.stdout.write(out)
sys.stderr.write(err)

out, err = srv.communicate()
sys.stdout.write(out)
sys.stderr.write(err)
