import os, sys, re
try:
    from MonetDBtesting import process
except ImportError:
    import process

TSTSRCBASE = os.environ['TSTSRCBASE']
TSTDIR = os.environ['TSTDIR']
SRCDIR = os.path.join(TSTSRCBASE,TSTDIR,"Tests")

def new_client():
    return process.client('sql', stdin = process.PIPE, stdout = process.PIPE, stderr = process.PIPE)


def exec_sql_file(file, mapping={}):
    c = new_client()

    with open(os.path.join(SRCDIR, "sql", file), "r") as f:
        for line in f:
            towrite = line
            for key in mapping:
                towrite = towrite.replace(key, mapping[key])
            c.stdin.write(towrite)

    out, err = c.communicate()
    sys.stdout.write(out)
    sys.stderr.write(err)
