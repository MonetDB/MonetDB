import os, sys
try:
    from MonetDBtesting import process
except ImportError:
    import process

clt = process.client('sql', user = 'testuser', passwd = 'testpassword',
                     stdin = process.PIPE,
                     stdout = process.PIPE, stderr = process.PIPE)
out, err = clt.communicate('select count(*) from testschema.smallstring;\n')
sys.stdout.write(out)
sys.stderr.write(err)
