/*database as3ap;
*/
create table uniques(k integer  not null, i integer  not null, si integer , f float not null, d double not null, decim integer  not null, date date  not null, code char(10)  not null, name char(20)  not null, address string not null, fill char(6)  not null);
create table hundred(k integer  not null, i integer  not null, si integer , f float  not null, d double  not null, decim integer  not null, date date  not null, code char(10)  not null, name char(20)  not null, address string not null, fill char(6)  not null);
create table tenpct(k integer  not null, i integer  not null, si integer , f float  not null, d double  not null, decim integer  not null, date date  not null, code char(10)  not null, name char(20)  not null, address string not null, fill char(6)  not null);
create table updates(k integer  not null, i integer  not null, si integer , f float  not null, d double  not null, decim integer  not null, date date  not null, code char(10)  not null, name char(20)  not null, address string not null, fill char(6)  not null);
create table tiny(k integer  not null);
/*
copy table uniques(k=c0, i=c0, si=c0, f=c0, d=c0, decim=c0, date=c0, code=c0, name=c0, address=c0, fill=c0)from 'uniques';
copy table hundred(k=c0, i=c0, si=c0, f=c0, d=c0, decim=c0, date=c0, code=c0, name=c0, address=c0, fill=c0)from 'hundred';
copy table tenpct(k=c0, i=c0, si=c0, f=c0, d=c0, decim=c0, date=c0, code=c0, name=c0, address=c0, fill=c0)from 'tenpct';
copy table updates(k=c0, i=c0, si=c0, f=c0, d=c0, decim=c0, date=c0, code=c0, name=c0, address=c0, fill=c0)from 'updates';
insert into tiny(k)values(0);
copy table updates(k=c0, i=c0, si=c0, f=c0, d=c0, decim=c0, date=c0, code=c0, name=c0, address=c0, fill=c0)into 'updates.bu';
*/
create view _AS3AP_o_mode_tiny as 
	select * from tiny;
create view _AS3AP_sel_1_cl as 
	select k, i, si, code, d, name from updates where k=1000;
create view _AS3AP_o_mode_1k as 
	select * from updates where k<=10;
create view _AS3AP_o_mode_10k as 
	select * from hundred where k<=100;
create view _AS3AP_o_mode_100k as 
	select * from hundred where k<=1000;
create table _AS3AP_join_3_cl(us integer , ud date , hs integer , hd date , ts integer , td date );
insert into _AS3AP_join_3_cl 
	select uniques.si, uniques.date, hundred.si, 
	       hundred.date, tenpct.si, tenpct.date 
	from uniques, hundred, tenpct 
	where uniques.k=hundred.k 
	and uniques.k=tenpct.k and uniques.k=1000;
create view _AS3AP_sel_100_ncl as select k, i, si, code, d, name 
	from updates where k<=100;
create view _AS3AP_table_scan as 
	select * from uniques where i=1;
create view _AS3AP_func_agg as 
	select min(k)from hundred group by name;
create view _AS3AP_scal_agg as 
	select min(k)from uniques;
create view _AS3AP_sel_100_cl as 
	select k, i, si, code, d, name from updates where k<=100;
create table _AS3AP_join_3_ncl(us integer , ud date , hs integer , hd date , ts integer , td date );
insert into _AS3AP_join_3_ncl 
	select uniques.si, uniques.date, hundred.si, hundred.date, tenpct.si, tenpct.date from uniques, hundred, tenpct where uniques.code=hundred.code and uniques.code=tenpct.code and uniques.code='BENCHMARKS';
create view _AS3AP_sel_10pct_ncl as select k, i, si, code, d, name from tenpct where name='THE+ASAP+BENCHMARKS+';
create view _AS3AP_sel_10pct_cl as select k, i, si, code, d, name from uniques where k<=100000000;
create view _AS3AP_report as 
	select avg(updates.decim) 
	from updates where updates.decim 
	in( select updates.decim 
		from updates, hundred 
		where hundred.k=updates.k 
		and updates.decim>980000000);
/*
create view _AS3AP_info_retrieval as select count(k)from tenpct where name='THE+ASAP+BENCHMARKS+' and i<=100000000 and si>0 and si<100000000 and(f< -450000000 or f>450000000)and d>600000000 and decim< -600000000;
*/
create view reportview(k, si, date, decim, name, code, i) as 
	select updates.k, updates.si, updates.date, updates.decim, 
	       hundred.name, hundred.code, hundred.i from updates, hundred 
	where updates.k=hundred.k;
create view _AS3AP_subtotal_report as 
	select avg(si), min(si), max(si), max(date), min(date), 
	       count(distinct name), count(name), code, i 
	from reportview 
	where decim>980000000 
	group by code, i;
create view _AS3AP_total_report as select avg(si), min(si), max(si), max(date), min(date), count(distinct name), count(name), count(code), count(i)from reportview where decim>980000000;
create table _AS3AP_join_1_10(uk integer , un char(20) , tn char(20) , ts integer );
/*
insert into _AS3AP_join_1_10 select uniques.k, uniques.name, tenpct.name, tenpct.si from uniques, tenpct where uniques.k=tenpct.si and(uniques.k=500000000 or uniques.k=600000000 or uniques.k=700000000 or uniques.k=800000000 or uniques.k=900000000);
*/
create table _AS3AP_join_2_cl(us integer , un char(20) , hs integer , hn char(20) );
insert into _AS3AP_join_2_cl select uniques.si, uniques.name, hundred.si, hundred.name from uniques, hundred where uniques.k=hundred.k and uniques.k=1000;
create table _AS3AP_join_2(us integer , un char(20) , hs integer , hn char(20) );
insert into _AS3AP_join_2 select uniques.si, uniques.name, hundred.si, hundred.name from uniques, hundred where uniques.address=hundred.address and uniques.address='SILICON VALLEY';
create view _AS3AP_varselectlow as select k, i, si, code, d, name from tenpct where si<40;
create view _AS3AP_varselecthigh as select k, i, si, code, d, name from tenpct where si<40;
create table _AS3AP_join_4_cl(ud integer, hd integer , td integer , upd integer );
insert into _AS3AP_join_4_cl select uniques.date, hundred.date, tenpct.date, updates.date from uniques, hundred, tenpct, updates where uniques.k=hundred.k and uniques.k=tenpct.k and uniques.k=updates.k and uniques.k=1000;
create view _AS3AP_proj_100 as select distinct address, si from hundred;
create table _AS3AP_join_4_ncl(ud date, hd date , td date , upd date );
insert into _AS3AP_join_4_ncl select uniques.date, hundred.date, tenpct.date, updates.date from uniques, hundred, tenpct, updates where uniques.code=hundred.code and uniques.code=tenpct.code and uniques.code=updates.code and uniques.code='BENCHMARKS';
create view _AS3AP_proj_10pct as select distinct si from tenpct;
create view _AS3AP_sel_1_ncl as select k, i, si, code, d, name from updates where code='BENCHMARKS';
create table _AS3AP_join_2_ncl(us integer , un char(20) , hs integer , hn char(20) );
insert into _AS3AP_join_2_ncl 
	select uniques.si, uniques.name, hundred.si, hundred.name 
	from uniques, hundred 
	where uniques.code=hundred.code and uniques.code='BENCHMARKS';
create view tempint as select * from hundred where i=0;
update hundred set si=-5000 where i=0;
update hundred set si=-500000000 where i=0;
delete from hundred where i=0;
insert into hundred select * from tempint;
delete from updates where k=5005;
create view saveupdates as select * from updates where k between 5010 and 6009;
update updates set k=k -100000 where k between 5010 and 6009;
insert into updates values(6000, 0, 60000, 39997.90, 50005.00, 50005.00, '11/10/85', 'CONTROLLER', 'ALICE IN WONDERLAND', 'UNIVERSITY OF ILLINOIS AT CHICAGO ', 'Krabbe');
delete from updates where k=6000 and i=0;
insert into updates values(5005, 5005, 50005, 50005.00, 50005.00, 50005.00, '1/1/88', 'CONTROLLER', 'ALICE IN WONDERLAND', 'UNIVERSITY OF ILLINOIS AT CHICAGO ', 'Krabbe');
update updates set k= -5000 where k=5005;
delete from updates where k= -5000;
insert into updates values(1000000001, 50005, 50005, 50005.00, 50005.00, 50005.00, '1/1/88', 'CONTROLLER', 'ALICE IN WONDERLAND', 'UNIVERSITY OF ILLINOIS AT CHICAGO ', 'Krabbe');
update updates set k= -1000 where k=1000000001;
delete from updates where k= -1000;
insert into updates values(5005, 5005, 50005, 50005.00, 50005.00, 50005.00, '1/1/88', 'CONTROLLER', 'ALICE IN WONDERLAND', 'UNIVERSITY OF ILLINOIS AT CHICAGO ', 'Krabbe');
update updates set code='SQL+GROUPS' where k=5005;
delete from updates where k=5005;
insert into updates values(5005, 5005, 50005, 50005.00, 50005.00, 50005.00, '1/1/88', 'CONTROLLER', 'ALICE IN WONDERLAND', 'UNIVERSITY OF ILLINOIS AT CHICAGO ', 'Krabbe');
update updates set i=500015 where k=5005;
delete from updates where k= -5000;
insert into updates select * from saveupdates;
delete from updates where k<0;

commit;
