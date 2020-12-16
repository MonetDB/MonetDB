import sys
from MonetDBtesting.sqltest import SQLTestCase
try:
    from MonetDBtesting import process
except ImportError:
    import process


with SQLTestCase() as tc:
    tc.connect(username="monetdb", password="monetdb")
    tc.execute("create table nonutf8 (s string);").assertSucceeded()

# input is a byte string because it contains broken utf-8
INPUT1 = b"""
insert into nonutf8 values ('\x7A\x77\x61\x61\x72\x20\x6C\x61\x6E\x67\x65\x20\x67\x6F\x6C\x66\x20\x70\x69\x65\x6B\x20\x2D\x64\x61\x6C\x20\xB1\x31\x30\x63\x6D\x20\x76\x61\x6B\x35');
"""
with process.client('sql', text=False, stdin=process.PIPE, stdout=process.PIPE, stderr=process.PIPE) as c:
    out, err = c.communicate(INPUT1)
    retcode = c.returncode

    if retcode == 0:
        sys.stderr.write("Expected nonzero return code")
    if not err or b'invalid start of UTF-8 sequence' not in err:
        sys.stderr.write("Expected stderr to contain 'invalid start of UTF-8 sequence'")

# input is a byte string because it contains broken utf-8
INPUT2 = b"""
copy 2 records into nonutf8 from stdin;
\x7A\x77\x61\x61\x72\x20\x6C\x61\x6E\x67\x65\x20\x67\x6F\x6C\x66\x20\x70\x69\x65\x6B\x20\x2D\x64\x61\x6C\x20\xB1\x31\x30\x63\x6D\x20\x76\x61\x6B\x35\x0A\xB1\x31\x37\x20\x25

"""
with process.client('sql', text=False, stdin=process.PIPE, stdout=process.PIPE, stderr=process.PIPE) as c:
    out, err = c.communicate(INPUT2)
    retcode = c.returncode

    if retcode == 0:
        sys.stderr.write("Expected nonzero return code")
    if not err or b'input not properly encoded UTF-8' not in err:
        sys.stderr.write("Expected stderr to contain 'input not properly encoded UTF-8'")

with SQLTestCase() as tc:
    tc.connect(username="monetdb", password="monetdb")
    tc.execute("select * from nonutf8;").assertSucceeded().assertRowCount(0).assertDataResultMatch([])
    tc.execute("drop table nonutf8;").assertSucceeded()
