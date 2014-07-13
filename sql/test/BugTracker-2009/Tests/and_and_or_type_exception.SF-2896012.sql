create table "simple" (id int,field1 int, field2 int);
select * from "simple" where field1 = 1 and (field1 = 1 or field2 = 1);
drop table "simple";
