create table utf8len(a varchar(1));
copy 1 records into utf8len from stdin;
0

copy 1 records into utf8len from stdin;
€

insert into utf8len values ('€');
select a, length(a) AS len from utf8len;
select 'Liever €uro' as "Liever euro";
drop table utf8len;
