import os

from MonetDBtesting.sqltest import SQLTestCase

port = os.environ['MAPIPORT']
db = os.environ['TSTDB']

with SQLTestCase() as cli:
    cli.connect(username="monetdb", password="monetdb")
    cli.execute("""
    START TRANSACTION;
    CREATE MERGE TABLE "mt2" ("c0" CHAR(78),"c1" UUID NOT NULL,CONSTRAINT "mt2_c1_pkey" PRIMARY KEY ("c1"),CONSTRAINT "mt2_c1_unique" UNIQUE ("c1"));

    CREATE TABLE "mct21" ("c0" CHAR(78),"c1" UUID NOT NULL,CONSTRAINT "mct21_c1_pkey" PRIMARY KEY ("c1"),CONSTRAINT "mct21_c1_unique" UNIQUE ("c1"));
    INSERT INTO "mct21" VALUES ('2\\\\5LTC', 'efcdc386-d403-cf6d-4d34-79e08cefad9b');

    CREATE TABLE "mct20" ("c0" CHAR(78),"c1" UUID NOT NULL,CONSTRAINT "mct20_c1_pkey" PRIMARY KEY ("c1"),CONSTRAINT "mct20_c1_unique" UNIQUE ("c1"));
    INSERT INTO "mct20" VALUES ('gC', '7ffeefe2-5ad2-9a6b-71e5-9ecbb8b52ce9'),('3', 'd4bb47ec-0ccf-2daf-3997-bfa94b409fae'),('o', 'd7c126c0-bb8b-f457-50e0-dcaf5e68e6be'),
    ('55', 'bf940cb2-f98d-67ae-1cae-17c8ed046ab6'),('#~Ew', 'afa1c3a9-b09d-92a0-e1ef-ed27bb663c2d'),(NULL, 'b991d4fe-abba-c4ea-c282-c19c2dd9f08d'),
    (NULL, 'da1bfd50-14d3-43fa-b6c1-cd95ee6f2f17'),(NULL, 'b408ad8d-bfe4-e2a9-f2b1-bf7bb2310226'),('', '15fed7bd-387b-475e-03b4-03da2cafbad7'),
    ('3', 'fb1f40ff-fa29-da45-f90b-0562639de03c'),(NULL, 'dac78eac-8483-46d4-ccd0-fb61eedaac02');
    COMMIT;

    START TRANSACTION;
    CREATE REMOTE TABLE "rmct20" ("c0" CHAR(78),"c1" UUID NOT NULL,CONSTRAINT "rmct20_c1_pkey" PRIMARY KEY ("c1"),CONSTRAINT "rmct20_c1_unique" UNIQUE ("c1")) ON 'mapi:monetdb://localhost:%s/%s/sys/mct20';
    CREATE REMOTE TABLE "rmct21" ("c0" CHAR(78),"c1" UUID NOT NULL,CONSTRAINT "rmct21_c1_pkey" PRIMARY KEY ("c1"),CONSTRAINT "rmct21_c1_unique" UNIQUE ("c1")) ON 'mapi:monetdb://localhost:%s/%s/sys/mct21';
    ALTER TABLE "mt2" ADD TABLE "rmct20";
    ALTER TABLE "mt2" ADD TABLE "rmct21";
    COMMIT;""" % (port, db, port, db)).assertSucceeded()

    cli.execute("START TRANSACTION;")
    cli.execute('(select 0) intersect (select 0 from mt2, mct20 where mct20.c0 like mt2.c0);') \
        .assertSucceeded().assertDataResultMatch([(0,)])
    cli.execute('(select 0) intersect (select 1 from mt2, mct20 where mct20.c0 like mt2.c0);') \
        .assertSucceeded().assertDataResultMatch([])
    cli.execute("ROLLBACK;")

    cli.execute("""
    START TRANSACTION;
    ALTER TABLE mt2 DROP TABLE rmct20;
    ALTER TABLE mt2 DROP TABLE rmct21;
    DROP TABLE rmct20;
    DROP TABLE rmct21;
    DROP TABLE mct20;
    DROP TABLE mct21;
    DROP TABLE mt2;
    COMMIT;""").assertSucceeded()

    # testing remote tables with transaction isolation
    cli.execute("""
    START TRANSACTION;
    CREATE TABLE rt0(c0 INT);
    CREATE REMOTE TABLE rrt0(c0 INT) on 'mapi:monetdb://localhost:%s/%s/sys/rt0';
    COMMIT;""" % (port, db)).assertSucceeded()

    cli.execute('SELECT "setmasklen"(INET \'9.49.240.200/13\', 48061431) FROM rrt0;') \
        .assertFailed(err_message="Exception occurred in the remote server, please check the log there")
    cli.execute('INSERT INTO rt0(c0) VALUES(1);') \
        .assertSucceeded().assertRowCount(1)
    cli.execute('ALTER TABLE rt0 ADD CONSTRAINT con3 UNIQUE(c0);') \
        .assertSucceeded()

    cli.execute("""
    START TRANSACTION;
    DROP TABLE rrt0;
    DROP TABLE rt0;
    COMMIT;""").assertSucceeded()

    cli.execute("""
    START TRANSACTION;
    CREATE TABLE t0(c0 INT);
    INSERT INTO t0 VALUES (1),(2),(3);
    CREATE MERGE TABLE mt2 (c0 INT);
    CREATE TABLE mct20 (c0 INT);
    CREATE TABLE rmct21 (c0 INT);
    CREATE REMOTE TABLE rrmct21 (c0 INT) ON 'mapi:monetdb://localhost:%s/%s/sys/rmct21';
    ALTER TABLE mt2 ADD TABLE mct20;
    ALTER TABLE mt2 ADD TABLE rrmct21;
    COMMIT;""" % (port, db)).assertSucceeded()

    cli.execute("""SELECT 1 FROM (SELECT 1 FROM t0, mt2) vx(vc0) LEFT OUTER JOIN
        (SELECT 2 FROM t0) AS sub0(c0) ON CASE 5 WHEN 2 THEN sub0.c0 <> ALL(VALUES (3), (4)) END;""") \
        .assertSucceeded().assertDataResultMatch([])

    cli.execute("""
    START TRANSACTION;
    DROP TABLE t0;
    ALTER TABLE mt2 DROP TABLE rrmct21;
    ALTER TABLE mt2 DROP TABLE mct20;
    DROP TABLE mt2;
    DROP TABLE mct20;
    DROP TABLE rmct21;
    DROP TABLE rrmct21;
    COMMIT;""").assertSucceeded()

    # remote tables with replica tables
    cli.execute("""
    START TRANSACTION;
    CREATE TABLE rt0 (c0 INTEGER);
    INSERT INTO rt0 VALUES (1),(2);
    CREATE REMOTE TABLE rrt0 (c0 INTEGER) ON 'mapi:monetdb://localhost:%s/%s/sys/rt0';
    CREATE TABLE rt2 (c0 TIMESTAMP);
    INSERT INTO rt2 VALUES (TIMESTAMP '1980-06-11 14:05:31'),(TIMESTAMP '1970-01-09 22:12:27');
    CREATE REPLICA TABLE rrt2 (c0 TIMESTAMP);
    ALTER TABLE rrt2 ADD TABLE rt2;
    COMMIT;""" % (port, db)).assertSucceeded()

    cli.execute("SELECT rrt0.c0 FROM rrt2, rrt0 ORDER BY rrt0.c0;") \
        .assertSucceeded().assertDataResultMatch([(1,),(1,),(2,),(2,),])

    cli.execute("""
    START TRANSACTION;
    ALTER TABLE rrt2 DROP TABLE rt2;
    DROP TABLE rrt2;
    DROP TABLE rt2;
    DROP TABLE rrt0;
    DROP TABLE rt0;
    COMMIT;""").assertSucceeded()

# testing temporary tables
with SQLTestCase() as mdb1:
    mdb1.connect(username="monetdb", password="monetdb")
    mdb1.execute("CREATE GLOBAL TEMPORARY TABLE tt2(c0 JSON, c1 DATE) ON COMMIT PRESERVE ROWS;").assertSucceeded()
    mdb1.execute("INSERT INTO tmp.tt2(c1, c0) VALUES(DATE '2010-10-10', JSON 'true');").assertSucceeded()

with SQLTestCase() as mdb2:
    mdb2.connect(username="monetdb", password="monetdb")
    mdb2.execute("TRUNCATE TABLE tmp.tt2;").assertSucceeded()
    mdb2.execute("DROP TABLE tmp.tt2;").assertSucceeded()
