create table select_into_t (j int);
insert into select_into_t values (5);

create function test5 () returns int
begin
    declare i int;
    select j into i from select_into_t;
    return i;
end;

select test5();

drop function test5;
drop table select_into_t;
