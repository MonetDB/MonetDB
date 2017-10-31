-- Test a continuous function returning a table
create table results3 (aa int, bb text);

create function cfunc3(input text) returns table (aa integer, bb text) begin
    declare s int;
    set s = 0;
    while true do
        set s = s + 1;
        yield table (select s, input);
    end while;
end;

start continuous function cfunc3('test') with heartbeat 1000 cycles 3;

pause continuous cfunc3;

create procedure cproc3() begin
    insert into results3 (select aa, bb from tmp.cfunc3);
end;

start continuous procedure cproc3() with cycles 3;

call cquery.wait(4000);

stop continuous cfunc3;

select aa, bb from results3;

drop function cfunc3;
drop procedure cproc3;
drop table results3;
