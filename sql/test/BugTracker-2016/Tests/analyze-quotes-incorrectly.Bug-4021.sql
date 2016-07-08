start transaction;

create table bug4021(s string);

insert into bug4021 values (''''), ('"');

analyze sys.bug4021;

rollback;
