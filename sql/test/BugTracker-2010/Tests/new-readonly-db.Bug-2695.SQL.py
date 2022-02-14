import os, sys, tempfile
try:
    from MonetDBtesting import process
except ImportError:
    import process

with tempfile.TemporaryDirectory() as farm_dir:
    os.mkdir(os.path.join(farm_dir, 'db1'))
    with process.server(args=['--readonly'], mapiport='0', dbname='db1',
                        dbfarm=os.path.join(farm_dir, 'db1'),
                        stdin=process.PIPE,
                        stdout=process.PIPE, stderr=process.PIPE) as s:
        out, err = s.communicate()
        if s.returncode == 0 or not err or 'Fatal error during initialization' not in err:
            print("Test failed: expected a fatal error during initialization", file=sys.stderr)
            print(file=sys.stderr)
            sys.stderr.write(err)
            sys.stdout.write(out)
            sys.exit(1)
