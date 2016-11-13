create schema marketdata;
create table marketdata.quotes (i integer);
CREATE TRIGGER marketdata.calc_sdate BEFORE INSERT ON marketdata.quotes
FOR EACH ROW 
BEGIN ATOMIC
-- select * from marketdata.quotes limit 1;
END;
--syntax error, unexpected '.', expecting WHILE in: "create trigger marketdata.calc_sdate before insert on marketdata."
drop trigger marketdata.calc_sdate;
drop table marketdata.quotes;
drop schema marketdata;

