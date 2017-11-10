-- Test a continuous function returning a table
create table results2 (aa int, bb text);

create function cfunc2(input text) returns table (aa integer, bb text) begin
    declare s int;
    set s = 0;
    while true do
        set s = s + 1;
        yield table (select s, input);
    end while;
end;

start continuous function cfunc2('test') with heartbeat 1000 cycles 3;

pause continuous cfunc2;

create procedure cproc2() begin
    insert into results2 (select aa, bb from tmp.cfunc2);
end;

start continuous procedure cproc2() with cycles 2;

call cquery.wait(4000);

stop continuous cfunc2;

select aa, bb from results2;

drop function cfunc2;
drop procedure cproc2;
drop table results2;
