create table table1_bug2771 (x clob, y clob);
insert into table1_bug2771 values ('one','');
insert into table1_bug2771 values ('two','');
create table table2_bug2771 (x clob, y clob);
insert into table2_bug2771 values ('one','een');
insert into table2_bug2771 values ('two','twee');
update table1_bug2771 set y = (select y from table2_bug2771 where table1_bug2771.x = table2_bug2771.x and table1_bug2771.y = '' and table2_bug2771.y <> '');
select * from table1_bug2771;
