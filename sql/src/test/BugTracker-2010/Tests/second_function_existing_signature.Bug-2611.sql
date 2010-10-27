create table table_a (
    column_a int,
    column_b int
);

create function next_a()
returns integer
begin
    declare col_a integer;
    set col_a = 1;

    return col_a;
end;

create function next_a()
returns integer
begin
    declare col_a integer;
    set col_a = 1;

    return col_a;
end;

drop function next_a;

drop table table_a;
