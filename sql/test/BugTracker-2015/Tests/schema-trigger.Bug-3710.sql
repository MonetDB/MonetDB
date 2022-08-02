create schema marketdata;

create table marketdata.quotes (i integer);

CREATE TRIGGER marketdata.calc_sdate BEFORE INSERT ON marketdata.quotes
FOR EACH ROW 
BEGIN ATOMIC
-- select * from marketdata.quotes limit 1;
 update marketdata.quotes set i = i +2 where i < 2;
END;

insert into marketdata.quotes (i) values (1);
select * from marketdata.quotes;
insert into marketdata.quotes (i) values (2);
select * from marketdata.quotes;
insert into marketdata.quotes (i) values (1);
select * from marketdata.quotes;
insert into marketdata.quotes (i) values (2);
select * from marketdata.quotes;

drop trigger marketdata.calc_sdate;
drop table marketdata.quotes;
drop schema marketdata;

