try:
    from MonetDBtesting import process
except ImportError:
    import process

import sys

s1 = process.server(stdin = process.PIPE, stdout = process.PIPE,
                    stderr = process.PIPE, dbextra='/tmp/dbextra/')



s2 = process.server(stdin = process.PIPE, stdout = process.PIPE,
                    stderr = process.PIPE, dbextra='/tmp/dbextra/')

out, err = s1.communicate()
sys.stdout.write(out)
sys.stderr.write(err)

out, err = s2.communicate()
sys.stdout.write(out)
sys.stderr.write(err)
