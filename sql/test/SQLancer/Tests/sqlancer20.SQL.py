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
    CREATE TABLE "t1" ("c0" BIGINT,"c1" INTERVAL MONTH);
    INSERT INTO "t1" VALUES (1, INTERVAL '9' MONTH),(5, INTERVAL '6' MONTH),(5, NULL),(7, NULL),(2, INTERVAL '1' MONTH),(2, INTERVAL '1' MONTH);
    COMMIT;

    START TRANSACTION;
    CREATE REMOTE TABLE "rt1" ("c0" BIGINT,"c1" INTERVAL MONTH) ON 'mapi:monetdb://localhost:%s/%s/sys/t1';
    COMMIT;""" % (port, db)).assertSucceeded()

    cli.execute("""
        SELECT 1 FROM t0 FULL OUTER JOIN (SELECT rt1.c0 <= CAST(rt1.c0 AS INT) FROM rt1) AS sub0(c0) ON true WHERE sub0.c0
        UNION ALL
        SELECT 1 FROM t0 FULL OUTER JOIN (SELECT rt1.c0 <= CAST(rt1.c0 AS INT) FROM rt1) AS sub0(c0) ON true;
        """).assertSucceeded().assertDataResultMatch([(1,),(1,),(1,),(1,),(1,),(1,),(1,),(1,),(1,),(1,),(1,),(1,)])

    # Issues related to digits and scale propagation in the sql layer
    cli.execute("SELECT CAST(2 AS DECIMAL) & CAST(3 AS DOUBLE) FROM t1 where t1.c0 = 1;") \
        .assertSucceeded().assertDataResultMatch([(Decimal('0.002'),)])
    cli.execute("SELECT CAST(2 AS DECIMAL) & CAST(3 AS DOUBLE) FROM rt1 where rt1.c0 = 1;") \
        .assertSucceeded().assertDataResultMatch([(Decimal('0.002'),)])

    cli.execute("""
    START TRANSACTION;
    DROP TABLE rt1;
    DROP TABLE t0;
    DROP TABLE t1;
    COMMIT;""").assertSucceeded()
