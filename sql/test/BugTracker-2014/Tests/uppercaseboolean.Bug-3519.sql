start transaction;
create table bug3519(a boolean);
-- works
insert into bug3519 values (true),(false),(TRUE),(FALSE);
insert into bug3519 values ('true'),('false');
insert into bug3519 values (1),(0),(NULL),('1'),('0');
COPY 5 RECORDS INTO bug3519 FROM stdin NULL as '';
true
false
1
0

select * from bug3519;

-- does not work
insert into bug3519 values ('TRUE'),('FALSE');
COPY 2 RECORDS INTO bug3519 FROM stdin;
TRUE
FALSE

select * from bug3519;
rollback;
