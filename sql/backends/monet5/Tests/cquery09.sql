--cannot drop stream table while there are continuous queries referencing it
create stream table result9(i integer);

create procedure cq_c(inputA int)
begin
	insert into sys.result9 values (inputA);
end;

start continuous sys.cq_c(1) as cq_ca;

call cquery.wait(4000);

drop table result9; --error

stop continuous cq_c; --error

stop continuous cq_ca;

drop procedure cq_c;
drop table result9; --no error
