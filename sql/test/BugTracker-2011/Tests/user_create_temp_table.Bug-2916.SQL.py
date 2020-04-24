import os, sys
try:
    from MonetDBtesting import process
except ImportError:
    import process

with process.client('sql',
                    args=['-s', 'CREATE USER "testuser" WITH PASSWORD \'password\' NAME \'Test User\' SCHEMA "sys";'],
                    stdout=process.PIPE,
                    stderr=process.PIPE) as c:
    out, err = c.communicate()
    if out:
        sys.stdout.write(out)
    if err:
        sys.stderr.write(err)

with process.client('sql',
                    args=['-s', 'CREATE GLOBAL TEMPORARY TABLE TempTable (i int);' ],
                    user='testuser', passwd='password',
                    stdout=process.PIPE,
                    stderr=process.PIPE) as c:
    out, err = c.communicate()
    if out:
        sys.stdout.write(out)
    if err:
        sys.stderr.write(err)

with process.client('sql',
                    args=['-s', 'CREATE LOCAL TEMPORARY TABLE TempTable (i int); '],
                    user='testuser', passwd='password',
                    stdout=process.PIPE,
                    stderr=process.PIPE) as c:
    out, err = c.communicate()
    if out:
        sys.stdout.write(out)
    if err:
        sys.stderr.write(err)

# undo damage
with process.client('sql',
                    args=['-s', 'DROP USER "testuser"'],
                    stdout=process.PIPE,
                    stderr=process.PIPE) as c:
    out, err = c.communicate()
    if out:
        sys.stdout.write(out)
    if err:
        sys.stderr.write(err)
