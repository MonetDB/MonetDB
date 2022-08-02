start transaction;

create table testme (b varchar(16));

insert into testme values ('another'), ('testing'), ('all');
select group_concat(b) from testme;

insert into testme values ('lets'), ('get'), ('harder');
select group_concat(b) from testme;

insert into testme values ('even'), ('more'), ('serious');
select group_concat(b) from testme;

insert into testme values (NULL);
select group_concat(b) from testme;

delete from testme where b is null;
select group_concat(b) from testme;

insert into testme values (''), ('stress'), ('');
select group_concat(b) from testme;

rollback;
