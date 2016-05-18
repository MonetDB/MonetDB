import os, sys
try:
    from MonetDBtesting import process
except ImportError:
    import process

TSTSRCBASE = os.environ['TSTSRCBASE']
SRCDIR = os.path.join(TSTSRCBASE, "sql", "benchmarks", "tpch")
DATADIR = os.path.join(SRCDIR,"SF-0.01")

c = process.client('sql', stdin = process.PIPE, stdout = process.PIPE, stderr = process.PIPE)
c.stdin.write("CREATE TABLE REGION ( R_REGIONKEY INTEGER NOT NULL, R_NAME CHAR(25) NOT NULL, R_COMMENT VARCHAR(152));\n")
c.stdin.write("COPY 5 RECORDS INTO region from '%s' USING DELIMITERS '|', '|\\n';\n" % os.path.join(DATADIR, 'region.tbl').replace('\\', r'\\'))
c.stdin.write("select count(*) from region;\n")
c.stdin.write("CREATE USER copyuser WITH PASSWORD 'copyuser' name 'copyuser' schema sys;\n")
c.stdin.write("GRANT INSERT, SELECT on region to copyuser;\n")
out, err = c.communicate()
sys.stdout.write(out)
sys.stderr.write(err.replace(DATADIR, '$DATADIR').replace('DIR\\', 'DIR/'))

c = process.client('sql', user = 'copyuser', passwd = 'copyuser', stdin = process.PIPE, stdout = process.PIPE, stderr = process.PIPE)
c.stdin.write("COPY 5 RECORDS INTO region from '%s' USING DELIMITERS '|', '|\\n';\n" % os.path.join(DATADIR, 'region.tbl').replace('\\', r'\\'))
c.stdin.write("select count(*) from region;\n")
out, err = c.communicate()
sys.stdout.write(out)
sys.stderr.write(err.replace(DATADIR, '$DATADIR').replace('DIR\\', 'DIR/'))

c = process.client('sql', stdin = process.PIPE, stdout = process.PIPE, stderr = process.PIPE)
c.stdin.write("GRANT COPY FROM, COPY INTO to copyuser;\n")
out, err = c.communicate()
sys.stdout.write(out)
sys.stderr.write(err.replace(DATADIR, '$DATADIR').replace('DIR\\', 'DIR/'))

c = process.client('sql', user = 'copyuser', passwd = 'copyuser', stdin = process.PIPE, stdout = process.PIPE, stderr = process.PIPE)
c.stdin.write("COPY 5 RECORDS INTO region from '%s' USING DELIMITERS '|', '|\\n';\n" % os.path.join(DATADIR, 'region.tbl').replace('\\', r'\\'))
c.stdin.write("select count(*) from region;\n")
out, err = c.communicate()
sys.stdout.write(out)
sys.stderr.write(err.replace(DATADIR, '$DATADIR').replace('DIR\\', 'DIR/'))

c = process.client('sql', stdin = process.PIPE, stdout = process.PIPE, stderr = process.PIPE)
c.stdin.write("REVOKE COPY FROM, COPY INTO from copyuser;\n")
out, err = c.communicate()
sys.stdout.write(out)
sys.stderr.write(err)

