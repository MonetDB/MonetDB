import os, sys, tempfile

try:
    from MonetDBtesting import process
except ImportError:
    import process

(fd, tmpf) = tempfile.mkstemp(suffix='.lz4', text=True)
try:
    os.close(fd)
    os.unlink(tmpf)

    with process.server(args=[], stdin=process.PIPE, stdout=process.PIPE, stderr=process.PIPE) as s:
        data = open(os.path.join(os.getenv('TSTSRCDIR'), 'lz4-dump.sql')).read()
        with process.client('sql', stdin=process.PIPE, log=True,
                            stdout=process.PIPE, stderr=process.PIPE) as c:
            out, err = c.communicate(data.replace('/tmp/testing-dump.lz4', tmpf))
            sys.stdout.write(out)
            sys.stderr.write(err)

        out, err = s.communicate()
        sys.stdout.write(out)
        sys.stderr.write(err)
finally:
    try:
        os.unlink(tmpf)
    except:
        pass
