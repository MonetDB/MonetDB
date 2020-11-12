import sys
try:
    from MonetDBtesting import process
except ImportError:
    import process

out = err = None
try:

    with process.client(lang='sql',
                        args=['-fsql',
                            '-s', 'create table t2589 as select * from tables with data'],
                        stdout=process.PIPE, stderr=process.PIPE) as c:
        out, err = c.communicate()
        if c.returncode != 0:
            raise SystemExit("TEST FAILED: expected returncode to be 0")
        if err:
            raise SystemExit("TEST FAILED: unexpected data on stderr")
        if 'operation successful' not in out:
            raise SystemExit("TEST FAILED: expected 'operation successful' on stdout")

    with process.client(lang='sql',
                        args=['-fsql',
                            '-s', 'drop table t2589'],
                        stdout=process.PIPE, stderr=process.PIPE) as c:
        out, err = c.communicate()
        if c.returncode != 0:
            raise SystemExit("TEST FAILED: expected returncode to be 0")
        if err:
            raise SystemExit("TEST FAILED: unexpected data on stderr")
        if 'operation successful' not in out:
            raise SystemExit("TEST FAILED: expected 'operation successful' on stdout")
except Exception:
    if out:
        print("", "", "ACTUAL STDOUT:", out, sep="\n", file=sys.stderr)
finally:
    if err:
        print("", "", "ACTUAL STDERR:", err, sep="\n", file=sys.stderr)
