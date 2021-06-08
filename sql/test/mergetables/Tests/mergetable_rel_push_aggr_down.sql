start transaction;

CREATE SCHEMA "myschema";

CREATE MERGE TABLE "myschema"."mymerge" (
"col1"  CHARACTER LARGE OBJECT,
"col2"  CHARACTER LARGE OBJECT,
"col3"  CHARACTER LARGE OBJECT,
"col4"  CHARACTER LARGE OBJECT,
"col5"  CHARACTER LARGE OBJECT,
"col6"  BIGINT,
"col7"  BIGINT
);

CREATE TABLE "myschema"."subt1" (
"col1"  CHARACTER LARGE OBJECT,
"col2"  CHARACTER LARGE OBJECT,
"col3"  CHARACTER LARGE OBJECT,
"col4"  CHARACTER LARGE OBJECT,
"col5"  CHARACTER LARGE OBJECT,
"col6"  BIGINT,
"col7"  BIGINT
);

CREATE TABLE "myschema"."subt2" (
"col1"  CHARACTER LARGE OBJECT,
"col2"  CHARACTER LARGE OBJECT,
"col3"  CHARACTER LARGE OBJECT,
"col4"  CHARACTER LARGE OBJECT,
"col5"  CHARACTER LARGE OBJECT,
"col6"  BIGINT,
"col7"  BIGINT
);

alter table "myschema"."mymerge" add table "myschema"."subt1";
alter table "myschema"."mymerge" add table "myschema"."subt2";

select count(*) FROM (
    SELECT myalias.col5 AS field1,
           myalias.col4 AS field2, 
           myalias.col3 AS field3, 
           myalias.col1 AS field4, 
           myalias.col2 AS field5, 
           (COUNT(*)),
           'bb7fd938-43b0-11ea-b44c-845ddc3cb4be' AS MyID 
    FROM "myschema"."mymerge" myalias
    WHERE myalias.col7 >= 1577914380 AND myalias.col7 <= 1580423692
    GROUP BY field3, field5, field1, field4, field2) AS mycount;

select count(*) FROM (
    SELECT myalias.col5 AS field1,
           myalias.col4 AS field2,
           (COUNT(*))
    FROM "myschema"."mymerge" myalias
    GROUP BY field1, field2) AS mycount;

-- These statements are related to the same issue
create merge table merge_table (mcol1 int, mcol2 int, mcol3 int);
create table child1 (mcol1 int, mcol2 int, mcol3 int);
insert into child1 values (1,1,1);
create table child2 (mcol1 int, mcol2 int, mcol3 int);
insert into child2 values (2,2,2);
alter table merge_table add table child1;
alter table merge_table add table child2;
select cast(sum(mcol1) as bigint), cast(sum(mcol2) as bigint) from merge_table group by mcol1, mcol2, mcol3 order by mcol1 limit 2;

rollback;
