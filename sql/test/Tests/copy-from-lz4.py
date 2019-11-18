import os, sys, tempfile

try:
    from MonetDBtesting import process
except ImportError:
    import process

(fd, tmpf) = tempfile.mkstemp(suffix='.lz4', text=True)
os.close(fd)
os.unlink(tmpf)

s = process.server(args=[], stdin=process.PIPE, stdout=process.PIPE, stderr=process.PIPE)

data = open(os.path.join(os.getenv('TSTSRCDIR'), 'lz4-dump.sql')).read()

c = process.client('sql', stdin=process.PIPE,
                   stdout=process.PIPE, stderr=process.PIPE, log=True)
out, err = c.communicate(data.replace('/tmp/testing-dump.lz4', tmpf))
sys.stdout.write(out)
sys.stderr.write(err)

out, err = s.communicate()
sys.stdout.write(out)
sys.stderr.write(err)

try:
    os.unlink(tmpf)
except:
    pass
