--cannot drop stream table while there are continuous queries referencing it
create stream table result9(i integer);

create procedure cq_c(inputA int)
begin
	insert into sys.result9 values (inputA);
end;

start continuous sys.cq_c(1);

call cquery.wait(4000);

drop table result9; --error

stop continuous sys.cq_c(2); --error

stop continuous sys.cq_c(1);

drop procedure cq_c;
drop table result9; --no error
