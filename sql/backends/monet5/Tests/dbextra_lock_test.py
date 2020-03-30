try:
    from MonetDBtesting import process
except ImportError:
    import process

import sys
import os

p = os.path.join(os.getenv('GDK_DBFARM'), 'transient_dir')

s1 = s2 = None
try:
    s1 = process.server(stdin = process.PIPE, stdout = process.PIPE,
                        stderr = process.PIPE, dbextra=p)
    s2 = process.server(stdin = process.PIPE, stdout = process.PIPE,
                        stderr = process.PIPE, dbextra=p)

    out, err = s1.communicate()
    sys.stdout.write(out)
    sys.stderr.write(err)

    out, err = s2.communicate()
    sys.stdout.write(out)
    sys.stderr.write(err)
finally:
    if s1 is not None:
        s1.terminate()
    if s2 is not None:
        s2.terminate()
