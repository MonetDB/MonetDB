START TRANSACTION;
SET SCHEMA "sys";
CREATE TABLE "sys"."applied_credit" (
	"directory"    VARCHAR(512)  NOT NULL,
	"passed"       INTEGER,
	"allowed"      INTEGER       NOT NULL,
	"multiplicity" INTEGER       NOT NULL       DEFAULT 0,
	CONSTRAINT "applied_credit_directory_allowed_multiplicity_pkey" PRIMARY KEY ("directory", "allowed", "multiplicity")
);
CREATE TABLE "sys"."success_credit" (
	"directory" VARCHAR(512)  NOT NULL,
	"passed"    INTEGER,
	"allowed"   INTEGER       NOT NULL,
	CONSTRAINT "success_credit_directory_allowed_pkey" PRIMARY KEY ("directory", "allowed")
);
COPY 2 RECORDS INTO "sys"."success_credit" FROM stdin USING DELIMITERS '\t','\n','"';
"foo"	1270047915	0
"bar"	1270047915	0
CREATE TABLE "sys"."success_setting" (
	"directory"         VARCHAR(512)  NOT NULL,
	"rate"              INTEGER,
	"heartbeat"         INTEGER,
	"success_rate"      INTEGER,
	"success_heartbeat" INTEGER,
	"concurrency"       INTEGER,
	CONSTRAINT "success_setting_directory_pkey" PRIMARY KEY ("directory")
);
COPY 2 RECORDS INTO "sys"."success_setting" FROM stdin USING DELIMITERS '\t','\n','"';
"foo"	341	300	341	300	5
"bar"	341	300	341	300	5

INSERT INTO applied_credit (directory, allowed, multiplicity)
SELECT success_credit.directory,
       success_credit.allowed,
       count(applied_credit.directory)
FROM success_credit
     LEFT OUTER JOIN applied_credit
     ON applied_credit.directory = success_credit.directory
     AND applied_credit.allowed >= success_credit.allowed
GROUP BY success_credit.directory, success_credit.allowed;

ROLLBACK;
