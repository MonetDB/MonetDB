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
    CREATE FUNCTION mybooludf(a bool) RETURNS BOOL RETURN a;
    COMMIT;

    START TRANSACTION;
    CREATE REMOTE TABLE "rt1" ("c0" BIGINT,"c1" INTERVAL MONTH) ON 'mapi:monetdb://localhost:%s/%s/sys/t1';
    COMMIT;""" % (port, db)).assertSucceeded()

    # Issues related to scale propagation in the sql layer
    cli.execute("SELECT CAST(2 AS DECIMAL) & CAST(3 AS DOUBLE) FROM t1 where t1.c0 = 1;") \
        .assertSucceeded().assertDataResultMatch([(Decimal('0.002'),)])
    cli.execute("SELECT CAST(2 AS DECIMAL) & CAST(3 AS DOUBLE) FROM rt1 where rt1.c0 = 1;") \
        .assertSucceeded().assertDataResultMatch([(Decimal('0.002'),)])
    cli.execute("SELECT scale_down(146.0, 1) FROM t1 where t1.c0 = 1;") \
        .assertSucceeded().assertDataResultMatch([(Decimal('14.6'),)])
    cli.execute("SELECT scale_down(146.0, 1) FROM rt1 where rt1.c0 = 1;") \
        .assertSucceeded().assertDataResultMatch([(Decimal('14.6'),)])
    cli.execute("SELECT greatest(\"lower\"('D4Idf '), 'x x') FROM t1 where t1.c0 = 1;") \
        .assertSucceeded().assertDataResultMatch([("x x",)])
    cli.execute("SELECT greatest(\"lower\"('D4Idf '), 'x x') FROM rt1 where rt1.c0 = 1;") \
        .assertSucceeded().assertDataResultMatch([("x x",)])
    cli.execute("SELECT 1 FROM t1 where t1.c0 = 1 AND abs(0.4) = 0;") \
        .assertSucceeded().assertDataResultMatch([])
    cli.execute("SELECT 1 FROM rt1 where rt1.c0 = 1 AND abs(0.4) = 0;") \
        .assertSucceeded().assertDataResultMatch([])

    # We should replace algebra.fetch call with something more appropriate
    cli.execute("SELECT 1 FROM t1 HAVING (min(TIME '02:00:00') IN (TIME '02:00:00')) IS NULL;") \
        .assertSucceeded().assertDataResultMatch([])
    cli.execute("SELECT 1 FROM rt1 HAVING (min(TIME '02:00:00') IN (TIME '02:00:00')) IS NULL;") \
        .assertSucceeded().assertDataResultMatch([])
    cli.execute("SELECT 1 FROM t1 HAVING mybooludf(min(false));") \
        .assertSucceeded().assertDataResultMatch([])
    cli.execute("SELECT 1 FROM rt1 HAVING mybooludf(min(false));") \
        .assertSucceeded().assertDataResultMatch([])

    cli.execute("""
    START TRANSACTION;
    DROP TABLE rt1;
    DROP TABLE t1;
    DROP FUNCTION mybooludf(bool);
    COMMIT;""").assertSucceeded()
