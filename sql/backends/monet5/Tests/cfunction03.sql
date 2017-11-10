-- Test continuous functions with limited runs
create table results3 (aa time);

create function cfunc3(input time) returns table (aa time) begin
    while true do
        yield table (select input);
    end while;
end;

start continuous function cfunc3(time '15:00:00') with heartbeat 100 cycles 3;

call cquery.wait(2000);

select aa from time;

drop function cfunc3;
drop procedure cproc3;
drop table results3;
