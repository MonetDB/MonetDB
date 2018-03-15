start transaction;

create table testme (a int, b clob, c int);

insert into testme values (1, 'another', 1), (5, '', 20), (5, 'if', 20), (2, 'two', 2), (4, 'a singleton', 10), (5, 'else', 20);

select a, group_concat(b) from testme where c > 3 group by a;

select group_concat(a) from testme;

select '[' || group_concat(a) || ']' from testme;

select group_concat(c) from testme where c < 3;

insert into testme values (6, '', 12), (7, '', 323), (4, 'not a singleton anymore', 7), (7, NULL, 323);

select a, group_concat(b) from testme where c > 3 group by a;

select group_concat(a) from testme;

select '[' || group_concat(a) || ']' from testme;

select group_concat(c) from testme where c < 3;

create table othertest (a int, b clob);

insert into othertest values (1, '\\t a\t'), (1, '\n\\n,'), (1, ',,,');

select a, group_concat(b) from othertest group by a;

select group_concat(b) from othertest;

insert into othertest values (2, '\n'), (2, '\n'), (1, '');

select a, group_concat(b) from othertest group by a;

select group_concat(b) from othertest;

rollback;
