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
    CREATE TABLE "t4" ("c0" BIGINT PRIMARY KEY,"c1" INTERVAL MONTH);
    INSERT INTO "t4" VALUES (1, INTERVAL '9' MONTH),(5, INTERVAL '6' MONTH),(10, NULL),(7, NULL),(2, INTERVAL '1' MONTH),(11, INTERVAL '1' MONTH);
    CREATE TABLE "t5" ("c0" DECIMAL(18,3),"c1" BOOLEAN);
    INSERT INTO "t5" VALUES (0.928, NULL),(0.974, NULL),(NULL, false),(3.000, NULL),(NULL, false),(NULL, false),(NULL, true),(0.897, NULL),
    (0.646, NULL),(0.145, true),(0.848, false),(NULL, false);
    COMMIT;

    START TRANSACTION;
    CREATE REMOTE TABLE "rt1" ("c0" BINARY LARGE OBJECT,"c1" BIGINT) ON 'mapi:monetdb://localhost:%s/%s/sys/t1';
    CREATE REMOTE TABLE "rt2" ("c0" TINYINT NOT NULL,"c2" DATE) ON 'mapi:monetdb://localhost:%s/%s/sys/t2';
    CREATE REMOTE TABLE "rt3" ("c0" BIGINT,"c1" INTERVAL MONTH) ON 'mapi:monetdb://localhost:%s/%s/sys/t3';
    CREATE REMOTE TABLE "rt4" ("c0" BIGINT PRIMARY KEY,"c1" INTERVAL MONTH) ON 'mapi:monetdb://localhost:%s/%s/sys/t4';
    CREATE REMOTE TABLE "rt5" ("c0" DECIMAL(18,3),"c1" BOOLEAN) ON 'mapi:monetdb://localhost:%s/%s/sys/t5';
    COMMIT;""" % (port, db, port, db, port, db, port, db, port, db)).assertSucceeded()

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
    cli.execute("SELECT r'\"', r'\\', ' ', '' as \"'\", '''' as \" \" from t3 where t3.c0 = 1;") \
        .assertSucceeded().assertDataResultMatch([("\"","\\"," ","","'")])
    cli.execute("SELECT r'\"', r'\\', ' ', '' as \"'\", '''' as \" \" from rt3 where rt3.c0 = 1;") \
        .assertSucceeded().assertDataResultMatch([("\"","\\"," ","","'")])
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
    cli.execute("SELECT sql_min(4, 7 - 0.5207499) FROM t3 where t3.c0 = 1;") \
        .assertSucceeded().assertDataResultMatch([(Decimal('4.0000000'),)])
    cli.execute("SELECT sql_min(4, 7 - 0.5207499) FROM rt3 where rt3.c0 = 1;") \
        .assertSucceeded().assertDataResultMatch([(Decimal('4.0000000'),)])
    cli.execute("SELECT \"insert\"('99', 5, 8, '10S') FROM t3 where t3.c0 = 1;") \
        .assertSucceeded().assertDataResultMatch([("9910S",)])
    cli.execute("SELECT \"insert\"('99', 5, 8, '10S') FROM rt3 where rt3.c0 = 1;") \
        .assertSucceeded().assertDataResultMatch([("9910S",)])
    cli.execute("SELECT greatest('69', splitpart('', '191', 2)) FROM t3 where t3.c0 = 1;") \
        .assertSucceeded().assertDataResultMatch([('69',)])
    cli.execute("SELECT greatest('69', splitpart('', '191', 2)) FROM rt3 where rt3.c0 = 1;") \
        .assertSucceeded().assertDataResultMatch([('69',)])
    cli.execute("SELECT CAST(trim('14', 'abc') AS STRING(408)), CAST(trim('14', 'abc') AS VARCHAR(408)), CAST(trim('14', 'abc') AS CLOB) FROM t3 where t3.c0 = 1;") \
        .assertSucceeded().assertDataResultMatch([('14','14','14')])
    cli.execute("SELECT CAST(trim('14', 'abc') AS STRING(408)), CAST(trim('14', 'abc') AS VARCHAR(408)), CAST(trim('14', 'abc') AS CLOB) FROM rt3 where rt3.c0 = 1;") \
        .assertSucceeded().assertDataResultMatch([('14','14','14')])
    cli.execute("SELECT NULL, 'NULL', 'null', cast(NULL as clob), cast('NULL' as clob), cast('null' as clob) FROM t3 where t3.c0 = 1;") \
        .assertSucceeded().assertDataResultMatch([(None,'NULL','null',None,'NULL','null')])
    cli.execute("SELECT NULL, 'NULL', 'null', cast(NULL as clob), cast('NULL' as clob), cast('null' as clob) FROM rt3 where rt3.c0 = 1;") \
        .assertSucceeded().assertDataResultMatch([(None,'NULL','null',None,'NULL','null')])
    cli.execute("SELECT t3.c0 FROM t3 where (t3.c0) NOT IN (0.07564294, 211.0, 1, 2) ORDER BY t3.c0;") \
        .assertSucceeded().assertDataResultMatch([(5,),(5,),(7,)])
    cli.execute("SELECT rt3.c0 FROM rt3 where (rt3.c0) NOT IN (0.07564294, 211.0, 1, 2) ORDER BY rt3.c0;") \
        .assertSucceeded().assertDataResultMatch([(5,),(5,),(7,)])
    cli.execute("SELECT t3.c0 FROM t3 INNER JOIN t3 myx ON t3.c0 = myx.c0 ORDER BY t3.c0;") \
        .assertSucceeded().assertDataResultMatch([(1,),(2,),(2,),(2,),(2,),(5,),(5,),(5,),(5,),(7,)])
    cli.execute("SELECT rt3.c0 FROM rt3 INNER JOIN rt3 myx ON rt3.c0 = myx.c0 ORDER BY rt3.c0;") \
        .assertSucceeded().assertDataResultMatch([(1,),(2,),(2,),(2,),(2,),(5,),(5,),(5,),(5,),(7,)])
    cli.execute("SELECT t4.c0 FROM t4 ORDER BY t4.c0 DESC NULLS FIRST;") \
        .assertSucceeded().assertDataResultMatch([(11,),(10,),(7,),(5,),(2,),(1,)])
    cli.execute("SELECT rt4.c0 FROM rt4 ORDER BY rt4.c0 DESC NULLS FIRST;") \
        .assertSucceeded().assertDataResultMatch([(11,),(10,),(7,),(5,),(2,),(1,)])
    cli.execute("SELECT t4.c1 FROM t4 ORDER BY t4.c1 ASC NULLS LAST;") \
        .assertSucceeded().assertDataResultMatch([(1,),(1,),(6,),(9,),(None,),(None,)])
    cli.execute("SELECT rt4.c1 FROM rt4 ORDER BY rt4.c1 ASC NULLS LAST;") \
        .assertSucceeded().assertDataResultMatch([(1,),(1,),(6,),(9,),(None,),(None,)])
    cli.execute("SELECT t4.c1 + INTERVAL '2' MONTH AS myx FROM t4 ORDER BY myx ASC NULLS LAST;") \
        .assertSucceeded().assertDataResultMatch([(3,),(3,),(8,),(11,),(None,),(None,)])
    cli.execute("SELECT rt4.c1 + INTERVAL '2' MONTH AS myx FROM rt4 ORDER BY myx ASC NULLS LAST;") \
        .assertSucceeded().assertDataResultMatch([(3,),(3,),(8,),(11,),(None,),(None,)])
    cli.execute("SELECT t4.c1 + INTERVAL '5' MONTH AS myx FROM t4 GROUP BY myx ORDER BY myx;") \
        .assertSucceeded().assertDataResultMatch([(None,),(6,),(11,),(14,)])
    cli.execute("SELECT rt4.c1 + INTERVAL '5' MONTH AS myx FROM rt4 GROUP BY myx ORDER BY myx;") \
        .assertSucceeded().assertDataResultMatch([(None,),(6,),(11,),(14,)])
    cli.execute("SELECT TRUE BETWEEN (TRUE BETWEEN FALSE AND FALSE) AND TRUE FROM t3 where t3.c0 = 1;") \
        .assertSucceeded().assertDataResultMatch([(True,)])
    cli.execute("SELECT TRUE BETWEEN (TRUE BETWEEN FALSE AND FALSE) AND TRUE FROM rt3 where rt3.c0 = 1;") \
        .assertSucceeded().assertDataResultMatch([(True,)])
    cli.execute("SELECT 1 FROM t3 WHERE (t3.c0 BETWEEN t3.c0 AND t3.c0) IS NULL;") \
        .assertSucceeded().assertDataResultMatch([])
    cli.execute("SELECT 2 FROM rt3 WHERE (rt3.c0 BETWEEN rt3.c0 AND rt3.c0) IS NULL;") \
        .assertSucceeded().assertDataResultMatch([])
    cli.execute("SELECT upper(count(*)) FROM t3;") \
        .assertSucceeded().assertDataResultMatch([("6",)])
    cli.execute("SELECT upper(count(*)) FROM rt3;") \
        .assertSucceeded().assertDataResultMatch([("6",)])
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
    cli.execute("SELECT testremote(1);") \
       .assertSucceeded().assertDataResultMatch([(26,)])
    cli.execute("""
    CREATE FUNCTION testremote2(a int) RETURNS INT
    BEGIN
        DECLARE b INT, res INT;
        SET b = 2;
        IF a = 1 THEN
            DECLARE b INT;
            SET b = 3;
            SELECT b + count(*) INTO res FROM rt3;
        ELSE
            IF a = 2 THEN
                SELECT b + count(*) INTO res FROM rt3;
            ELSE
                DECLARE c INT;
                SET c = 5;
                SELECT c + b + count(*) INTO res FROM rt3;
            END IF;
        END IF;
        RETURN res;
    END;
    """).assertSucceeded()
    cli.execute("SELECT testremote2(1), testremote2(2), testremote2(3);") \
        .assertSucceeded().assertDataResultMatch([(9,8,13)])
    cli.execute("""
    CREATE FUNCTION testremote3(\" ugh \"\" _ , !😂?, \" INT) RETURNS INT
    BEGIN
        DECLARE \" \" INT,\"\"\"\" INT, \"\\\" INT, res INT;
        SET \" \" = 2;
        SET \"\"\"\" = 4;
        SET \"\\\" = 10;
        SELECT \" \" + \"\"\"\" + \"\\\" + count(*) + \" ugh \"\" _ , !😂?, \"
                + CASE \"current_user\" WHEN 'monetdb' THEN 7 ELSE 7 END INTO res FROM rt3;
        RETURN res;
    END;
    """).assertSucceeded()
    cli.execute("""
    CREATE FUNCTION testremote4(a UUID, b JSON, c INT) RETURNS INT
    BEGIN
        RETURN SELECT (CASE a WHEN UUID '39FcCcEE-5033-0d81-42Eb-Ac6fFaA9EF2d' THEN 1 END) +
            (CASE b WHEN JSON '\"\"' THEN 2 END) + (CASE c WHEN 3 THEN 3 END) + count(*) FROM rt3;
    END;
    """).assertSucceeded()
    cli.execute("SELECT testremote3(1), testremote4(UUID '39FcCcEE-5033-0d81-42Eb-Ac6fFaA9EF2d', JSON '\"\"', 3);") \
        .assertSucceeded().assertDataResultMatch([(30,12)])
    cli.execute("""
    CREATE FUNCTION testremote5(a INET, b JSON, c DATE) RETURNS INT
    BEGIN
        RETURN SELECT (CASE a WHEN INET '192.168.1.0/26' THEN 1 END) +
            (CASE b WHEN JSON '[1]' THEN 2 END) + (CASE c WHEN DATE '2010-01-01' THEN 3 END) + count(*) FROM rt3;
    END;
    """).assertSucceeded()
    cli.execute("SELECT testremote5(INET '192.168.1.0/26', JSON '[1]', DATE '2010-01-01'), testremote5(NULL, NULL, NULL);") \
        .assertSucceeded().assertDataResultMatch([(12,None)])
    cli.execute("""
    CREATE FUNCTION testremote6(a BLOB) RETURNS INT
    BEGIN
        RETURN SELECT (CASE a WHEN BLOB 'AABB' THEN 1 ELSE 10 END) + count(*) FROM rt3;
    END;
    """).assertSucceeded()
    cli.execute("SELECT testremote6(BLOB 'AABB'), testremote6(BLOB 'CCDD'), testremote6(NULL);") \
        .assertSucceeded().assertDataResultMatch([(7,16,16)])
    cli.execute("""
    CREATE FUNCTION testremote7("😀😀😀😀😀😀😀😀😀😀😀😀😀😀😀" INT) RETURNS INT
    BEGIN
        RETURN SELECT (CASE "😀😀😀😀😀😀😀😀😀😀😀😀😀😀😀" WHEN 2 THEN 1 ELSE 10 END) + count(*) FROM rt3;
    END;
    """).assertSucceeded()
    cli.execute("SELECT testremote7(2), testremote7(3);") \
        .assertSucceeded().assertDataResultMatch([(7,16)])
    cli.execute("create view v0(vc0, vc1) as (values (interval '2' second, 0.5));").assertSucceeded()
    cli.execute("select 1 from rt1, v0, rt3 where \"right_shift_assign\"(inet '150.117.219.77', inet '1.188.46.21/12');") \
        .assertSucceeded().assertDataResultMatch([])
    cli.execute("select 1 from t1, v0, t3 where \"right_shift_assign\"(inet '150.117.219.77', inet '1.188.46.21/12');") \
        .assertSucceeded().assertDataResultMatch([])
    cli.execute("create view v1(vc0) as (select greatest(sql_sub(time '01:00:00', interval '0' second), time '01:00:00') from t3 where false);") \
        .assertSucceeded()
    cli.execute("select 1 from (select distinct 1 from v1, rt3) as v1(vc1) where sql_min(true, true);") \
        .assertSucceeded().assertDataResultMatch([])
    cli.execute("select 1 from (select distinct 1 from v1, t3) as v1(vc1) where sql_min(true, true);") \
        .assertSucceeded().assertDataResultMatch([])
    cli.execute("SELECT count(*) FROM ((select 7 from rt3, (values (1)) y(y)) union (select 3)) x(x);") \
        .assertSucceeded().assertDataResultMatch([(2,)])
    cli.execute("SELECT count(*) FROM ((select 7 from t3, (values (1)) y(y)) union (select 3)) x(x);") \
        .assertSucceeded().assertDataResultMatch([(2,)])
    cli.execute("SELECT count(*) FROM ((select 7 from rt3, (values (1)) y(y)) union all (select 3)) x(x);") \
        .assertSucceeded().assertDataResultMatch([(7,)])
    cli.execute("SELECT count(*) FROM ((select 7 from t3, (values (1)) y(y)) union all (select 3)) x(x);") \
        .assertSucceeded().assertDataResultMatch([(7,)])
    cli.execute("create view v2(vc0) as ((select 3 from rt3) intersect (select 2 from t3));") \
        .assertSucceeded()
    cli.execute("create view v3(vc0) as (select 1 from rt3, v2 where \"right_shift_assign\"(inet '228.236.62.235/6', inet '82.120.56.164'));") \
        .assertSucceeded()
    cli.execute("create view v4(vc0, vc1, vc2) as (select 1, 2, 3);") \
        .assertSucceeded()
    cli.execute("create view v5(vc0) as ((select time '01:00:00') intersect (select time '01:00:00' from v3));") \
        .assertSucceeded()
    cli.execute("create view v6(vc0) as ((select 1) union all (select 2));") \
        .assertSucceeded()
    cli.execute("select 1 from v4, v5, v6;") \
        .assertSucceeded().assertDataResultMatch([])
    cli.execute("create view v7(vc0) as (select case '201' when ',' then rt3.c0 when '' then cast(rt3.c0 as bigint) end from rt3);") \
        .assertSucceeded()
    cli.execute("SELECT 1 FROM v7 CROSS JOIN ((SELECT 1) UNION ALL (SELECT 2)) AS sub0(c0);") \
        .assertSucceeded().assertDataResultMatch([(1,),(1,),(1,),(1,),(1,),(1,),(1,),(1,),(1,),(1,),(1,),(1,)])
    cli.execute("""
        SELECT 1 FROM (VALUES (2),(3)) x(x) FULL OUTER JOIN (SELECT t1.c1 <= CAST(t1.c1 AS INT) FROM t1) AS sub0(c0) ON true WHERE sub0.c0
        UNION ALL
        SELECT 1 FROM (VALUES (2),(3)) x(x) FULL OUTER JOIN (SELECT t1.c1 <= CAST(t1.c1 AS INT) FROM t1) AS sub0(c0) ON true;
        """).assertSucceeded() \
        .assertDataResultMatch([(1,),(1,),(1,),(1,),(1,),(1,),(1,),(1,),(1,),(1,),(1,),(1,),(1,),(1,),(1,),(1,),(1,),(1,),(1,),(1,),(1,),(1,),(1,),(1,)])
    cli.execute("""
        SELECT 1 FROM (VALUES (2),(3)) x(x) FULL OUTER JOIN (SELECT rt1.c1 <= CAST(rt1.c1 AS INT) FROM rt1) AS sub0(c0) ON true WHERE sub0.c0
        UNION ALL
        SELECT 1 FROM (VALUES (2),(3)) x(x) FULL OUTER JOIN (SELECT rt1.c1 <= CAST(rt1.c1 AS INT) FROM rt1) AS sub0(c0) ON true;
        """).assertSucceeded() \
        .assertDataResultMatch([(1,),(1,),(1,),(1,),(1,),(1,),(1,),(1,),(1,),(1,),(1,),(1,),(1,),(1,),(1,),(1,),(1,),(1,),(1,),(1,),(1,),(1,),(1,),(1,)])
    cli.execute("SELECT count(0.3121149) FROM (select case when 2 > 1 then 0.3 end from (select 1 from t3) x(x)) v100(vc1), t3 WHERE 5 >= sinh(CAST(v100.vc1 AS REAL));") \
        .assertSucceeded().assertDataResultMatch([(36,)])
    cli.execute("SELECT count(0.3121149) FROM (select case when 2 > 1 then 0.3 end from (select 1 from rt3) x(x)) v100(vc1), rt3 WHERE 5 >= sinh(CAST(v100.vc1 AS REAL));") \
        .assertSucceeded().assertDataResultMatch([(36,)])
    cli.execute("SELECT CAST(2 AS REAL) BETWEEN 2 AND (t5.c0 / t5.c0)^5 AS X FROM t5 ORDER BY x NULLS LAST;") \
        .assertSucceeded().assertDataResultMatch([(True,),(True,),(True,),(True,),(True,),(True,),(True,),(None,),(None,),(None,),(None,)])
    cli.execute("SELECT CAST(2 AS REAL) BETWEEN 2 AND (rt5.c0 / rt5.c0)^5 AS X FROM rt5 ORDER BY x NULLS LAST;") \
        .assertSucceeded().assertDataResultMatch([(True,),(True,),(True,),(True,),(True,),(True,),(True,),(None,),(None,),(None,),(None,)])
    cli.execute("ROLLBACK;")

    cli.execute("CREATE FUNCTION mybooludf(a bool) RETURNS BOOL RETURN a;")
    # At the moment I take this as a feature. Later we could replace the algebra.fetch call with something more appropriate
    cli.execute("SELECT 1 FROM rt3 HAVING (min(TIME '02:00:00') IN (TIME '02:00:00')) IS NULL;") \
        .assertFailed(err_message="Illegal argument: cannot fetch a single row from an empty input")
    cli.execute("SELECT 1 FROM rt3 HAVING mybooludf(min(false));") \
        .assertFailed(err_message="Illegal argument: cannot fetch a single row from an empty input")

    cli.execute("""
    START TRANSACTION;
    DROP TABLE rt1;
    DROP TABLE rt2;
    DROP TABLE rt3;
    DROP TABLE rt4;
    DROP TABLE rt5;
    DROP TABLE t0;
    DROP TABLE t1;
    DROP TABLE t2;
    DROP TABLE t3;
    DROP TABLE t4;
    DROP TABLE t5;
    DROP FUNCTION mybooludf(bool);
    COMMIT;""").assertSucceeded()
