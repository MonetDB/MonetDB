CREATE TABLE "PKeyNotNull" (
	"PK_COL1" varchar(4) NOT NULL,
	"PK_COL2" varchar(4) NOT NULL,
	"DataCol" int DEFAULT NULL,
	PRIMARY KEY ("PK_COL1","PK_COL2") );

\d "PKeyNotNull"

INSERT INTO "PKeyNotNull" values ('C12', NULL, 1);
-- Error: INSERT INTO: NOT NULL constraint violated for column PKeyNotNull.PK_COL2

ALTER TABLE "PKeyNotNull" ALTER "PK_COL2" SET NULL;
-- this is allowed but should NOT be allowed
INSERT INTO "PKeyNotNull" values ('C12', NULL, 2);

SELECT * FROM "PKeyNotNull";

DROP TABLE "PKeyNotNull";



CREATE TABLE "PKeyImplicitNotNull" (
	"PK_COL1" varchar(4) NULL,
	"PK_COL2" varchar(4) NULL,
	"DataCol" int DEFAULT NULL,
	PRIMARY KEY ("PK_COL1","PK_COL2") );

\d "PKeyImplicitNotNull"

INSERT INTO "PKeyImplicitNotNull" values ('C12', NULL, 1);
-- Error: INSERT INTO: NOT NULL constraint violated for column PKeyImplicitNotNull.PK_COL2

ALTER TABLE "PKeyImplicitNotNull" ALTER "PK_COL2" SET NULL;
-- this is allowed but should NOT be allowed
INSERT INTO "PKeyImplicitNotNull" values ('C12', NULL, 2);

SELECT * FROM "PKeyImplicitNotNull";

DROP TABLE "PKeyImplicitNotNull";
