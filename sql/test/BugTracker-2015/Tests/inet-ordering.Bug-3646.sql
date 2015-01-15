CREATE TABLE "testing" ( "addr" inet );
INSERT INTO "testing" VALUES('192.168.0.1');
INSERT INTO "testing" VALUES('255.255.255.0');
SELECT * FROM "testing" ORDER BY addr;
DROP TABLE "testing";
