create table data (idx int);
insert into data values (13), (17);

create function crash2(i int, j int) returns int begin
  return i + j;
END;
select * from crash2((select idx from data), 2);
select * from crash2(1, (select idx from data));

create function crash3(i int, j int, k int) returns int begin
  return i + j + k;
END;
select * from crash3((select idx from data), 2, 3);

drop function crash3;
drop function crash2;
drop table data;
