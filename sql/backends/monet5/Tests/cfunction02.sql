-- Test a continuous function returning a table
create table results2 (aa int, bb text);

create function cfunc2(input text) returns table (aa integer, bb text) begin
    declare s int;
    set s = 0;
    while true do
        set s = s + 1;
        insert into results2 values (s, input);
        yield table (select s, input);
    end while;
end;

start continuous function cfunc2('test') with heartbeat 1000 cycles 3;

call sleep(4000);

stop continuous cfunc2; --error, cfunc2 is no longer available

select aa, bb from results2;

drop function cfunc2;
drop table results2;
