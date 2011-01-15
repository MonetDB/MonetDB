start transaction;

create table t978043 (id int, val varchar(1024));
insert into t978043 values (1, 'niels'), (2, 'fabian'), (3, 'martin');

create table test978043 (x integer, y varchar(1024));
insert into test978043 (y) select val from t978043 where id <= 2;
