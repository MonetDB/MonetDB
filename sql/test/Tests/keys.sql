START TRANSACTION;
CREATE TABLE allnewtriples (id integer NOT NULL, subject integer NOT
NULL, predicate integer NOT NULL, "object" integer NOT NULL, explicit
boolean NOT NULL, CONSTRAINT unique_key UNIQUE(subject, predicate, "object"));
CREATE INDEX allnewtriples_subject_idx ON allnewtriples (subject);
CREATE INDEX allnewtriples_predicate_idx ON allnewtriples (predicate);
CREATE INDEX allnewtriples_object_idx ON allnewtriples ("object");

SELECT idxs.name, idxs."type", keys.name, keys."type" 
FROM sys.idxs LEFT JOIN sys.keys on idxs.name = keys.name
ORDER BY idxs.name, keys.name;
SELECT idxs.name, idxs."type", keys.name, keys."type" 
FROM sys.idxs JOIN sys.keys on idxs.name = keys.name
ORDER BY idxs.name, keys.name;

/* test elimination of distinct restriction on aggregates */
create table dummyme (a int primary key, b int);
insert into dummyme values (1,1), (2,1), (3,1);

/* eliminated */
plan select count(distinct a) from dummyme;
plan select count(distinct a) from dummyme group by b;

/* not eliminated */
plan select count(distinct b) from dummyme;
plan select count(distinct a + 1) from dummyme;
plan select count(distinct a + b) from dummyme;
plan select count(distinct abs(a)) from dummyme;
ROLLBACK;
