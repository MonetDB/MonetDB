import os, sys
try:
    from MonetDBtesting import process
except ImportError:
    import process

TSTSRCBASE = os.environ['TSTSRCBASE']
SRCDIR = os.path.join(TSTSRCBASE, "sql", "benchmarks", "tpch")
DATADIR = os.path.join(SRCDIR,"SF-0.01") + os.sep.replace('\\', r'\\')

c = process.client('sql', stdin = process.PIPE, stdout = process.PIPE, stderr = process.PIPE)
c.stdin.write("CREATE TABLE REGION ( R_REGIONKEY INTEGER NOT NULL, R_NAME CHAR(25) NOT NULL, R_COMMENT VARCHAR(152));")
c.stdin.write("COPY 5 RECORDS INTO region from '%s/region.tbl' USING DELIMITERS '|', '|\n';" % DATADIR)
c.stdin.write("select count(*) from region;");
c.stdin.write("CREATE USER copyuser WITH PASSWORD 'copyuser' name 'copyuser' schema sys;")
c.stdin.write("GRANT INSERT, SELECT on region to copyuser;")
out, err = c.communicate()
sys.stdout.write(out)
sys.stderr.write(err)

c = process.client('sql', user = 'copyuser', passwd = 'copyuser', stdin = process.PIPE, stdout = process.PIPE, stderr = process.PIPE)
c.stdin.write("COPY 5 RECORDS INTO region from '%s/region.tbl' USING DELIMITERS '|', '|\n';" % DATADIR)
c.stdin.write("select count(*) from region;")
out, err = c.communicate()
sys.stdout.write(out)
sys.stderr.write(err)

c = process.client('sql', stdin = process.PIPE, stdout = process.PIPE, stderr = process.PIPE)
c.stdin.write("GRANT COPY FROM, COPY INTO to copyuser;")
out, err = c.communicate()
sys.stdout.write(out)
sys.stderr.write(err)

c = process.client('sql', user = 'copyuser', passwd = 'copyuser', stdin = process.PIPE, stdout = process.PIPE, stderr = process.PIPE)
c.stdin.write("COPY 5 RECORDS INTO region from '%s/region.tbl' USING DELIMITERS '|', '|\n';" % DATADIR)
c.stdin.write("select count(*) from region;")
out, err = c.communicate()
sys.stdout.write(out)
sys.stderr.write(err)

c = process.client('sql', stdin = process.PIPE, stdout = process.PIPE, stderr = process.PIPE)
c.stdin.write("REVOKE COPY FROM, COPY INTO from copyuser;")
out, err = c.communicate()
sys.stdout.write(out)
sys.stderr.write(err)

