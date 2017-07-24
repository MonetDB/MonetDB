-- test ordinary procedure call to update a stream
create stream table testing (a int);
insert into testing values(123);

create table results (a int);

create procedure myproc()
begin 
	insert into results select a from sys.testing; 
END;

-- a continuous procedure can be called like any other procedure
call myproc();

select * from results;

select * from functions where name = 'myproc';

drop procedure myproc;
drop table results;
drop table testing;
