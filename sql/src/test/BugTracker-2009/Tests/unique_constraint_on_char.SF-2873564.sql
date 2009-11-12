CREATE TABLE keytest (
date_added int NOT NULL,
hash_key char(32) NOT NULL,
dimension1 int NOT NULL,
dimension2 int NOT NULL,
metric1 int NOT NULL,
metric2 int NOT NULL,
CONSTRAINT key_test UNIQUE (hash_key,date_added)
);

insert into keytest (date_added, hash_key, dimension1, dimension2, metric1, metric2) VALUES (1, 'a', 1, 1, 2, 2);
insert into keytest (date_added, hash_key, dimension1, dimension2, metric1, metric2) VALUES (1, 'a', 1, 1, 2, 2);

select * from keytest;

CREATE UNIQUE INDEX unique_key_test ON test.keytest (date_added, hash_key);
insert into keytest (date_added, hash_key, dimension1, dimension2, metric1, metric2) VALUES (1, 'a', 1, 1, 2, 2);
insert into keytest (date_added, hash_key, dimension1, dimension2, metric1, metric2) VALUES (1, 'a', 1, 1, 2, 2);
select * from keytest;

drop table keytest;

CREATE TABLE keytest2 (
 date_added int NOT NULL,
 key int NOT NULL,
 dimension1 int NOT NULL,
 dimension2 int NOT NULL,
 metric1 int NOT NULL,
 metric2 int NOT NULL,
 CONSTRAINT key_test2 UNIQUE (key,date_added)
);

insert into keytest2 (date_added, key, dimension1, dimension2, metric1, metric2) VALUES (1, 1, 1, 1, 2, 2);
insert into keytest2 (date_added, key, dimension1, dimension2, metric1, metric2) VALUES (1, 1, 1, 1, 2, 2);
select * from keytest2;

drop table keytest2;
