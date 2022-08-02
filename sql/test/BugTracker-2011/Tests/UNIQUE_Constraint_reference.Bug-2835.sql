CREATE TABLE "Bug_2835_0" ("id" INT PRIMARY KEY );
INSERT INTO "Bug_2835_0" VALUES (0);
INSERT INTO "Bug_2835_0" VALUES (1);
INSERT INTO "Bug_2835_0" VALUES (2);
select * from "Bug_2835_0";

CREATE TABLE "Bug_2835_1" ("ref" INT REFERENCES "Bug_2835_0" , CONSTRAINT "unique" UNIQUE ("ref") );
INSERT INTO "Bug_2835_1" VALUES (0);
INSERT INTO "Bug_2835_1" VALUES (0);
select * from "Bug_2835_1";

create table "Bug_2835_2" (id int unique references "Bug_2835_0");
insert into "Bug_2835_2" values (1);
insert into "Bug_2835_2" values (1);
select * from "Bug_2835_2";

CREATE TABLE "Bug_2835_3" (
        "id" INTEGER,
        CONSTRAINT "Bug_2835_6_id_unique" UNIQUE ("id"),
        CONSTRAINT "Bug_2835_6_id_fkey" FOREIGN KEY ("id") REFERENCES "Bug_2835_0"
("id")
);
insert into "Bug_2835_3" values (2);
insert into "Bug_2835_3" values (2);
select * from "Bug_2835_3";

drop table "Bug_2835_3";
drop table "Bug_2835_2";
drop table "Bug_2835_1";
drop table "Bug_2835_0";
