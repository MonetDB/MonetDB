-- test ordinary procedure call to update a stream
create stream table cqinput00 (a int);
insert into cqinput00 values(123);

create table cqresults00 (a int);

create procedure cquery00()
begin 
	insert into cqresults00 select a from sys.cqinput00;
END;

-- a continuous procedure can be called like any other procedure
call cquery00();

select * from cqresults00;

--select * from functions where name = 'cquery00';

drop procedure cquery00;
drop table cqresults00;
drop table cqinput00;
