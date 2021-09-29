import os
from decimal import Decimal

from MonetDBtesting.sqltest import SQLTestCase

port = os.environ['MAPIPORT']
db = os.environ['TSTDB']

with SQLTestCase() as cli:
    cli.connect(username="monetdb", password="monetdb")
    cli.execute("""
    START TRANSACTION;
    CREATE TABLE "t0" ("c0" INTERVAL SECOND NOT NULL, "c1" JSON);
    INSERT INTO "t0" VALUES (INTERVAL '9' SECOND, '""');

    CREATE TABLE "t1" ("c0" BINARY LARGE OBJECT,"c1" BIGINT);
    INSERT INTO "t1" VALUES (NULL, 1),(NULL, 6),(NULL, 0),(BINARY LARGE OBJECT '50', NULL),(BINARY LARGE OBJECT 'ACBC2EDEF0', NULL),
    (BINARY LARGE OBJECT '65', NULL),(BINARY LARGE OBJECT 'EF43C0', NULL),(BINARY LARGE OBJECT '90', NULL),(BINARY LARGE OBJECT '', NULL);

    CREATE TABLE "t2" ("c0" TINYINT NOT NULL,"c2" DATE);
    INSERT INTO "t2" VALUES (-7, NULL),(0, NULL),(-11, DATE '1970-01-01'),(8, DATE '1970-01-01'),(5, DATE '1970-01-01'),(1, DATE '1970-01-01'),
    (0, NULL),(1, NULL),(7, NULL),(5, NULL);

    CREATE TABLE "t3" ("c0" BIGINT,"c1" INTERVAL MONTH);
    INSERT INTO "t3" VALUES (1, INTERVAL '9' MONTH),(5, INTERVAL '6' MONTH),(5, NULL),(7, NULL),(2, INTERVAL '1' MONTH),(2, INTERVAL '1' MONTH);
    COMMIT;

    START TRANSACTION;
    CREATE REMOTE TABLE "rt1" ("c0" BINARY LARGE OBJECT,"c1" BIGINT) ON 'mapi:monetdb://localhost:%s/%s/sys/t1';
    CREATE REMOTE TABLE "rt2" ("c0" TINYINT NOT NULL,"c2" DATE) ON 'mapi:monetdb://localhost:%s/%s/sys/t2';
    CREATE REMOTE TABLE "rt3" ("c0" BIGINT,"c1" INTERVAL MONTH) ON 'mapi:monetdb://localhost:%s/%s/sys/t3';
    COMMIT;""" % (port, db, port, db, port, db)).assertSucceeded()

    cli.execute("START TRANSACTION;")
    cli.execute('SELECT json."integer"(JSON \'1\') FROM t3;') \
        .assertSucceeded().assertDataResultMatch([(1,),(1,),(1,),(1,),(1,),(1,)])
    cli.execute('SELECT json."integer"(JSON \'1\') FROM rt3;') \
        .assertSucceeded().assertDataResultMatch([(1,),(1,),(1,),(1,),(1,),(1,)])
    cli.execute('SELECT c0 BETWEEN 10 AND 11 FROM t3;') \
        .assertSucceeded().assertDataResultMatch([(False,),(False,),(False,),(False,),(False,),(False,)])
    cli.execute('SELECT c0 BETWEEN 10 AND 11 FROM rt3;') \
        .assertSucceeded().assertDataResultMatch([(False,),(False,),(False,),(False,),(False,),(False,)])
    cli.execute('SELECT c0 > 10 as myt, 4 BETWEEN 4 AND 4, c0 = 10 as myp, c0 BETWEEN 1 AND 1 as myp2 FROM t3 where t3.c0 = 1;') \
        .assertSucceeded().assertDataResultMatch([(False,True,False,True)])
    cli.execute('SELECT c0 > 10 as myt, 4 BETWEEN 4 AND 4, c0 = 10 as myp, c0 BETWEEN 1 AND 1 as myp2 FROM rt3 where rt3.c0 = 1;') \
        .assertSucceeded().assertDataResultMatch([(False,True,False,True)])
    cli.execute('SELECT c0 BETWEEN 2 AND 5 AS myproj FROM t3 ORDER BY myproj;') \
        .assertSucceeded().assertDataResultMatch([(False,),(False,),(True,),(True,),(True,),(True,)])
    cli.execute('SELECT c0 BETWEEN 2 AND 5 AS myproj FROM rt3 ORDER BY myproj;') \
        .assertSucceeded().assertDataResultMatch([(False,),(False,),(True,),(True,),(True,),(True,)])
    cli.execute('SELECT c0 > 4 AS myproj FROM t3 ORDER BY myproj;') \
        .assertSucceeded().assertDataResultMatch([(False,),(False,),(False,),(True,),(True,),(True,)])
    cli.execute('SELECT c0 > 4 AS myproj FROM rt3 ORDER BY myproj;') \
        .assertSucceeded().assertDataResultMatch([(False,),(False,),(False,),(True,),(True,),(True,)])
    cli.execute('MERGE INTO t0 USING (SELECT 1 FROM rt1) AS mergejoined(c0) ON TRUE WHEN NOT MATCHED THEN INSERT (c0) VALUES (INTERVAL \'5\' SECOND);') \
        .assertSucceeded().assertRowCount(0)
    cli.execute('SELECT 1 FROM (values (0)) mv(vc0) LEFT OUTER JOIN (SELECT 1 FROM rt1) AS sub0(c0) ON 2 = 0.05488666234725814;') \
        .assertSucceeded().assertDataResultMatch([(1,),])
    cli.execute('SELECT c1 FROM rt1 WHERE rt1.c1 NOT BETWEEN 1 AND NULL;') \
        .assertSucceeded().assertDataResultMatch([(0,),])
    cli.execute('SELECT c1 FROM rt1 WHERE rt1.c1 NOT BETWEEN SYMMETRIC 1 AND NULL;') \
        .assertSucceeded().assertDataResultMatch([])
    cli.execute('SELECT 1 FROM (SELECT TIME \'01:00:00\' FROM rt1) va(vc1) WHERE greatest(va.vc1, TIME \'01:01:01\') <= TIME \'01:01:02\';') \
        .assertSucceeded().assertDataResultMatch([(1,),(1,),(1,),(1,),(1,),(1,)])
    cli.execute('SELECT 3 > (rt2.c0 ^ CAST(2 AS TINYINT)) * rt2.c0 FROM rt2;') \
        .assertSucceeded().assertDataResultMatch([(False,),(True,),(False,),(False,),(False,),(False,),(True,),(False,),(False,),(False,)])
    cli.execute("SELECT r'\"' from t3 where t3.c0 = 1;") \
        .assertSucceeded().assertDataResultMatch([("\"",)])
    cli.execute("SELECT r'\"' from rt3 where rt3.c0 = 1;") \
        .assertSucceeded().assertDataResultMatch([("\"",)])
    cli.execute("SELECT r'\\' from t3 where t3.c0 = 1;") \
        .assertSucceeded().assertDataResultMatch([("\\",)])
    cli.execute("SELECT r'\\' from rt3 where rt3.c0 = 1;") \
        .assertSucceeded().assertDataResultMatch([("\\",)])
    cli.execute("SELECT 1 as \"ups\\\", 2 as \"\\\", 3 as \"\"\"\", 4 as \"\"\"\\\", 5 as \"\\\"\"\" from t3 where t3.c0 = 1;") \
        .assertSucceeded().assertDataResultMatch([(1,2,3,4,5)])
    cli.execute("SELECT 1 as \"ups\\\", 2 as \"\\\", 3 as \"\"\"\", 4 as \"\"\"\\\", 5 as \"\\\"\"\" from rt3 where rt3.c0 = 1;") \
        .assertSucceeded().assertDataResultMatch([(1,2,3,4,5)])
    cli.execute("SELECT \"current_schema\", current_user from t3 where t3.c0 = 1;") \
        .assertSucceeded().assertDataResultMatch([("sys","monetdb")])
    cli.execute("SELECT \"current_schema\", current_user from rt3 where rt3.c0 = 1;") \
        .assertSucceeded().assertDataResultMatch([("sys","monetdb")])
    cli.execute("SELECT sql_min(t3.c0 || t3.c0, 3) as x from t3 ORDER BY x;") \
        .assertSucceeded().assertDataResultMatch([("11",),("22",),("22",),("3",),("3",),("3",)])
    cli.execute("SELECT sql_min(rt3.c0 || rt3.c0, 3) as x from rt3 ORDER BY x;") \
        .assertSucceeded().assertDataResultMatch([("11",),("22",),("22",),("3",),("3",),("3",)])
    cli.execute("SELECT CASE WHEN 1 BETWEEN 1 AND 2 THEN 3*6 END FROM t3 where t3.c0 = 1;") \
        .assertSucceeded().assertDataResultMatch([(18,)])
    cli.execute("SELECT CASE WHEN 1 BETWEEN 1 AND 2 THEN 3*6 END FROM rt3 where rt3.c0 = 1;") \
        .assertSucceeded().assertDataResultMatch([(18,)])
    cli.execute("SELECT 3 / 0.84 FROM t3 where t3.c0 = 1;") \
        .assertSucceeded().assertDataResultMatch([(Decimal('3.571'),)])
    cli.execute("SELECT 3 / 0.84 FROM rt3 where rt3.c0 = 1;") \
        .assertSucceeded().assertDataResultMatch([(Decimal('3.571'),)])
    cli.execute("SELECT CAST(2 AS DECIMAL) * 0.010 FROM t3 where t3.c0 = 1;") \
        .assertSucceeded().assertDataResultMatch([(Decimal('0.02000'),)])
    cli.execute("SELECT CAST(2 AS DECIMAL) * 0.010 FROM rt3 where rt3.c0 = 1;") \
        .assertSucceeded().assertDataResultMatch([(Decimal('0.02000'),)])
    cli.execute("SELECT t3.c0 FROM t3 INNER JOIN t3 myx ON t3.c0 = myx.c0 ORDER BY t3.c0;") \
        .assertSucceeded().assertDataResultMatch([(1,),(2,),(2,),(2,),(2,),(5,),(5,),(5,),(5,),(7,)])
    cli.execute("SELECT rt3.c0 FROM rt3 INNER JOIN rt3 myx ON rt3.c0 = myx.c0 ORDER BY rt3.c0;") \
        .assertSucceeded().assertDataResultMatch([(1,),(2,),(2,),(2,),(2,),(5,),(5,),(5,),(5,),(7,)])
    cli.execute("""
    CREATE FUNCTION testremote(a int) RETURNS INT
    BEGIN
        DECLARE b INT, "😀" INT, res1 INT, res2 INT;
        SET b = 2;
        SET "😀" = 4;
        SELECT a + b + "😀" + count(*) INTO res1 FROM t3;
        SELECT a + b + "😀" + count(*) INTO res2 FROM rt3;
        RETURN res1 + res2;
    END;
    """).assertSucceeded()
    cli.execute("SELECT testremote(1);").assertSucceeded().assertDataResultMatch([(26,)])

    # Issues related to digits and scale propagation in the sql layer
    cli.execute("SELECT CAST(2 AS DECIMAL) & CAST(3 AS DOUBLE) FROM t3 where t3.c0 = 1;") \
        .assertSucceeded().assertDataResultMatch([(Decimal('0.002'),)])
    cli.execute("SELECT CAST(2 AS DECIMAL) & CAST(3 AS DOUBLE) FROM rt3 where rt3.c0 = 1;") \
        .assertSucceeded().assertDataResultMatch([(Decimal('0.002'),)])
    cli.execute("SELECT greatest('69', splitpart('', '191', 2)) FROM t3 where t3.c0 = 1;") \
        .assertSucceeded().assertDataResultMatch([('69',)])
    cli.execute("SELECT greatest('69', splitpart('', '191', 2)) FROM rt3 where rt3.c0 = 1;") \
        .assertSucceeded().assertDataResultMatch([('69',)])

    # Issues related to comparisons not being correctly delimited on plans, which causes ambiguity
    cli.execute("SELECT TRUE BETWEEN (TRUE BETWEEN FALSE AND FALSE) AND TRUE FROM t3 where t3.c0 = 1;") \
        .assertSucceeded().assertDataResultMatch([(True,)])
    cli.execute("SELECT TRUE BETWEEN (TRUE BETWEEN FALSE AND FALSE) AND TRUE FROM rt3 where rt3.c0 = 1;") \
        .assertSucceeded().assertDataResultMatch([(True,)])
    cli.execute("SELECT 1 FROM t3 WHERE (t3.c0 BETWEEN t3.c0 AND t3.c0) IS NULL;") \
        .assertSucceeded().assertDataResultMatch([])
    cli.execute("SELECT 2 FROM rt3 WHERE (rt3.c0 BETWEEN rt3.c0 AND rt3.c0) IS NULL;") \
        .assertSucceeded().assertDataResultMatch([])
    cli.execute("ROLLBACK;")

    cli.execute("""
    START TRANSACTION;
    DROP TABLE rt1;
    DROP TABLE rt2;
    DROP TABLE rt3;
    DROP TABLE t0;
    DROP TABLE t1;
    DROP TABLE t2;
    DROP TABLE t3;
    COMMIT;""").assertSucceeded()
