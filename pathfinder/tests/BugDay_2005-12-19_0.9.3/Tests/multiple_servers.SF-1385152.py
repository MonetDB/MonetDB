import os, sys
try:
    from MonetDBtesting import process
except ImportError:
    import process

srv = process.server('mil',
                     dbinit = 'module(pathfinder);module(sql_server);mil_start();',
                     stdin = process.PIPE,
                     stdout = process.PIPE, stderr = process.PIPE)
clt = process.client('mil', stdout = process.PIPE, stderr = process.PIPE)
out, err = clt.communicate()
sys.stdout.write(out)
sys.stderr.write(err)
out, err = srv.communicate()
sys.stdout.write(out)
sys.stderr.write(err)
