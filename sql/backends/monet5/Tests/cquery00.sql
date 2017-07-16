create stream table testing (a int);
insert into TESTING values(123);

create table results (a int);

create continuous procedure myfirstcq() 
begin 
	insert into results select a from sys.testing; 
END;

-- a continuous procedure can be called like any other procedure
call myfirstcq();

select * from results;

select * from functions wherE name = 'myfirstcq';

