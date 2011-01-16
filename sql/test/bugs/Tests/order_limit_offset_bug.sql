START TRANSACTION;
CREATE SEQUENCE "seq_3978" AS INTEGER;
CREATE TABLE "sys"."entrants" (
    "id" int NOT NULL DEFAULT next value for "sys"."seq_3978",
    "name" varchar(255) NOT NULL,
    "course_id" int NOT NULL,
    CONSTRAINT "entrants_id_pkey" PRIMARY KEY ("id")
);
COPY 3 RECORDS INTO "entrants" FROM stdin USING DELIMITERS '\t', '\n', '"';
3	"Java Lover"	2
2	"Ruby Guru"	1
1	"Ruby Developer"	1
COMMIT;
SELECT * FROM "entrants"    ORDER BY id ASC;
SELECT * FROM "entrants"    ORDER BY id ASC LIMIT 2 OFFSET 2;

drop table entrants;
--drop sequence "seq_3978";

START TRANSACTION;
CREATE SEQUENCE "seq_3901" AS INTEGER;
CREATE TABLE "sys"."developers" (
    "id" int NOT NULL DEFAULT next value for "sys"."seq_3901",
    "name" varchar(100),
    "salary" int DEFAULT 70000,
    "created_at" timestamp(7),
    "updated_at" timestamp(7),
    CONSTRAINT "developers_id_pkey" PRIMARY KEY ("id")
);
COPY 11 RECORDS INTO "developers" FROM stdin USING DELIMITERS '\t', '\n', '"';
5	"fixture_5"	100000	2008-06-10 19:13:40.000000	2008-06-10 19:13:40.000000
6	"fixture_6"	100000	2008-06-10 19:13:40.000000	2008-06-10 19:13:40.000000
7	"fixture_7"	100000	2008-06-10 19:13:40.000000	2008-06-10 19:13:40.000000
11	"Jamis"	9000	2008-06-10 19:13:40.000000	2008-06-10 19:13:40.000000
8	"fixture_8"	100000	2008-06-10 19:13:40.000000	2008-06-10 19:13:40.000000
9	"fixture_9"	100000	2008-06-10 19:13:40.000000	2008-06-10 19:13:40.000000
2	"Jamis"	150000	2008-06-10 19:13:40.000000	2008-06-10 19:13:40.000000
10	"fixture_10"	100000	2008-06-10 19:13:40.000000	2008-06-10 19:13:40.000000
3	"fixture_3"	100000	2008-06-10 19:13:40.000000	2008-06-10 19:13:40.000000
1	"David"	80000	2008-06-10 19:13:40.000000	2008-06-10 19:13:40.000000
4	"fixture_4"	100000	2008-06-10 19:13:40.000000	2008-06-10 19:13:40.000000
COMMIT;

SELECT * FROM "developers"   WHERE (salary = 100000)  ORDER BY id DESC; 

SELECT * FROM "developers"   WHERE (salary = 100000)  ORDER BY id DESC LIMIT 3 OFFSET 7;

drop table developers;
--drop sequence "seq_3901";
