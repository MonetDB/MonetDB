import os, sys
try:
    from MonetDBtesting import process
except ImportError:
    import process

def main():
    with process.client('sql',
                        args = [os.path.join(os.getenv('TSTSRCBASE'),
                                             os.getenv('TSTDIR'),
                                             'Tests',
                                             'zones2.sql')],
                        stdin = process.PIPE, stdout = process.PIPE, stderr = process.PIPE) as c:
        out, err = c.communicate()
        sys.stdout.write(out)
        sys.stderr.write(err)

main()
main()
