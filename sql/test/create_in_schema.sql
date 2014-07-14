create schema "bla";

create table bla1(id int);
select * from bla1;

create temporary table bla2(id int);
select * from bla2;

create table bla.bla3(id int);
select * from bla3;	-- should fail
select * from bla.bla3;

create temporary table tmp.bla4(id int);
select * from bla4;

create table tmp.bla5(id int);
select * from bla5;	-- works, because tmp is in search path

create view v_bla1 as select * from bla1;
select * from v_bla1;

create view v_bla2 as select * from bla2;
select * from v_bla2;

create view v_bla3 as select * from bla.bla3;
select * from v_bla3;

create view bla.v_bla4 as select * from bla4;
select * from bla.v_bla4;

create view tmp.v_bla5 as select * from bla5;
select * from v_bla5;

drop view v_bla5;	-- should fail
drop view tmp.v_bla5;
drop view v_bla4;	-- should fail
drop view bla.v_bla4;
drop view v_bla3;
drop view v_bla2;
drop view v_bla1;

drop table bla5;
drop table sys.bla4;	-- should fail
drop table bla4;
drop table bla3;	-- should fail
drop table bla.bla3;
drop table bla1;
drop table bla2;

drop schema "bla";
