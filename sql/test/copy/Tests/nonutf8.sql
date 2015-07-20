-- test resistance against non-UFT8 input

start transaction;
create table nonutf8 ( s string);
insert into nonutf8 values ('zwaar lange golf piek -dal ±10cm vak5');

copy 2 records into nonutf8 from stdin;
zwaar lange golf piek -dal ±10cm vak5
±17 %

select * from nonutf8;

drop table nonutf8;
rollback;
