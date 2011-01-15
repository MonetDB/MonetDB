create table table_b (
    column_a int,
    column_b int
);

create function next_b()
returns integer
begin
    declare col_a integer;
    set col_a = null;

    set col_a = ( select min(column_a)
                     from   table_b
                     where  column_b is null );

    if col_a is not null then 
        update table_b
            set column_b = 1
        where  column_a = col_a;
    end if;

    return col_a;
end;

insert into table_b (
    column_a,
    column_b )
values (
   1,
   null
);

select * from table_b;

select next_b();

select * from table_b;

drop function next_b;
drop table table_b;
