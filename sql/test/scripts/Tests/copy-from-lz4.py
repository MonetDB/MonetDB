import os, tempfile

from MonetDBtesting.sqltest import SQLTestCase

(fd, tmpf) = tempfile.mkstemp(suffix='.lz4', text=True)
try:
    os.close(fd)
    os.unlink(tmpf)

    with SQLTestCase() as tc:
        tc.connect(username="monetdb", password="monetdb")
        tc.execute("CREATE TABLE outputlz4 (a bigint, b real, c clob);").assertSucceeded()
        tc.execute("CREATE TABLE readlz4 (a bigint, b real, c clob);").assertSucceeded()
        tc.execute("""COPY 4 RECORDS INTO outputlz4 (a, b, c) FROM STDIN USING DELIMITERS ',',E'\n','"' NULL AS '';
1,2.0,"another"
2,2.1,"test"
3,2.2,"to perform"
,1.0,
""").assertSucceeded().assertRowCount(4)
        tc.execute("SELECT a, b, c FROM outputlz4;").assertSucceeded().assertRowCount(4) \
            .assertDataResultMatch([(1,2,"another"),(2,2.1,"test"),(3,2.2,"to perform"),(None,1,None)])
        tc.execute("COPY (SELECT a, b, c FROM outputlz4) INTO '%s' USING DELIMITERS ',',E'\n','\"' NULL AS '';" % tmpf).assertSucceeded()
        tc.execute("COPY 4 RECORDS INTO readlz4 (a, b, c) FROM '%s' USING DELIMITERS ',',E'\n','\"' NULL AS '';" % tmpf).assertSucceeded()
        tc.execute("SELECT a, b, c FROM readlz4;").assertSucceeded().assertRowCount(4) \
            .assertDataResultMatch([(1,2,"another"),(2,2.1,"test"),(3,2.2,"to perform"),(None,1,None)])
        tc.execute("drop table outputlz4;").assertSucceeded()
        tc.execute("drop table readlz4;").assertSucceeded()
finally:
    try:
        os.unlink(tmpf)
    except:
        pass
