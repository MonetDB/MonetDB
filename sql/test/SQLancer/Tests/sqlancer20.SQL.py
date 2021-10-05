import os
from decimal import Decimal

from MonetDBtesting.sqltest import SQLTestCase

port = os.environ['MAPIPORT']
db = os.environ['TSTDB']

with SQLTestCase() as cli:
    cli.connect(username="monetdb", password="monetdb")
    cli.execute("""
    START TRANSACTION;
    CREATE TABLE "t1" ("c0" BIGINT,"c1" INTERVAL MONTH);
    INSERT INTO "t1" VALUES (1, INTERVAL '9' MONTH),(5, INTERVAL '6' MONTH),(5, NULL),(7, NULL),(2, INTERVAL '1' MONTH),(2, INTERVAL '1' MONTH);
    COMMIT;

    START TRANSACTION;
    CREATE REMOTE TABLE "rt1" ("c0" BIGINT,"c1" INTERVAL MONTH) ON 'mapi:monetdb://localhost:%s/%s/sys/t1';
    COMMIT;""" % (port, db)).assertSucceeded()

    # Issues related to digits and scale propagation in the sql layer
    cli.execute("SELECT CAST(2 AS DECIMAL) & CAST(3 AS DOUBLE) FROM t1 where t1.c0 = 1;") \
        .assertSucceeded().assertDataResultMatch([(Decimal('0.002'),)])
    cli.execute("SELECT CAST(2 AS DECIMAL) & CAST(3 AS DOUBLE) FROM rt1 where rt1.c0 = 1;") \
        .assertSucceeded().assertDataResultMatch([(Decimal('0.002'),)])
    cli.execute("SELECT greatest('69', splitpart('', '191', 2)) FROM t1 where t1.c0 = 1;") \
        .assertSucceeded().assertDataResultMatch([('69',)])
    cli.execute("SELECT greatest('69', splitpart('', '191', 2)) FROM rt1 where rt1.c0 = 1;") \
        .assertSucceeded().assertDataResultMatch([('69',)])

    # Issues related to comparisons not being correctly delimited on plans, which causes ambiguity
    cli.execute("SELECT TRUE BETWEEN (TRUE BETWEEN FALSE AND FALSE) AND TRUE FROM t1 where t1.c0 = 1;") \
        .assertSucceeded().assertDataResultMatch([(True,)])
    cli.execute("SELECT TRUE BETWEEN (TRUE BETWEEN FALSE AND FALSE) AND TRUE FROM rt1 where rt1.c0 = 1;") \
        .assertSucceeded().assertDataResultMatch([(True,)])
    cli.execute("SELECT 1 FROM t1 WHERE (t1.c0 BETWEEN t1.c0 AND t1.c0) IS NULL;") \
        .assertSucceeded().assertDataResultMatch([])
    cli.execute("SELECT 2 FROM rt1 WHERE (rt1.c0 BETWEEN rt1.c0 AND rt1.c0) IS NULL;") \
        .assertSucceeded().assertDataResultMatch([])
    cli.execute("SELECT 1 FROM rt1 HAVING (min(TIME '02:00:00') IN (TIME '02:00:00')) IS NULL;") \
        .assertSucceeded().assertDataResultMatch([])

    cli.execute("""
    START TRANSACTION;
    DROP TABLE rt1;
    DROP TABLE t1;
    COMMIT;""").assertSucceeded()
