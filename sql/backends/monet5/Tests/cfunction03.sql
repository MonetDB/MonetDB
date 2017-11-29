-- Test continuous functions with limited runs
create table results3 (aa time);

create function cfunc3(input time) returns table (aa time) begin
    while true do
        insert into results3 values (input);
        yield table (select input);
    end while;
end;

start continuous function cfunc3(time '15:00:00') with heartbeat 100 cycles 3;

call cquery.wait(2000);

select count(*) from results3;

drop function cfunc3;
drop table results3;
