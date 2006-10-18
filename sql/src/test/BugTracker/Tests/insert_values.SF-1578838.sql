start transaction;

create table "examines" ( "docID" string, "vicID" string);
create table "timelines" ("vicID" string, "time" string, "event" string);

create table "doctors" ( "docID" string, "name" string);
insert into "doctors" values ( 'doc1', 'Dr. doc 1'); 
create table "victims" ( "vicID" string, "name" string);
insert into "victims" values ( 'vic1', 'vic 1'); 

INSERT INTO "examines" ("docID", "vicID") VALUES
(
(
SELECT "docID"
FROM "doctors"
WHERE "name" LIKE 'Dr. doc%'
),
(
SELECT "vicID"
FROM "victims"
WHERE "name" LIKE 'vic%'
)
);

select * from "examines";

INSERT INTO "timelines" ("vicID", "time", "event") VALUES
(
(
SELECT "vicID"
FROM "victims"
WHERE "name" LIKE 'vic%'
),
'11:00 PM',
'Polly is seen walking etc...'
);

select * from "timelines";

insert into "victims" values ( 'vic2', 'vic 2'); 

savepoint x1;
savepoint x2;

INSERT INTO "examines" ("docID", "vicID") VALUES
(
(
SELECT "docID"
FROM "doctors"
WHERE "name" LIKE 'Dr. doc%'
),
(
SELECT "vicID"
FROM "victims"
WHERE "name" LIKE 'vic%'
)
);

rollback to savepoint x2;

select * from "doctors";
select * from "victims";
select * from "examines";
select * from "timelines";

INSERT INTO "timelines" ("vicID", "time", "event") VALUES
(
(
SELECT "vicID"
FROM "victims"
WHERE "name" LIKE 'vic%'
),
'11:00 PM',
'Polly is seen walking etc...'
);

rollback;
