create schema datacell;
create table datacell.basket_X(
    id integer auto_increment,
    tag timestamp default now(),
    payload integer
);
create function datacell.basket_X()
returns table (id integer, tag timestamp, payload integer)
begin
    return select * from datacell.basket_X;
end;
select * from datacell.basket_X;
select * from datacell.basket_X();
drop function datacell.basket_X;
drop table datacell.basket_X;
drop schema datacell cascade;

create schema schema_2817;
create table schema_2817.table_2817(
    id integer auto_increment,
    tag timestamp default now(),
    payload integer
);
create function schema_2817.function_2817()
returns table (id integer, tag timestamp, payload integer)
begin
    return select * from schema_2817.table_2817;
end;
select * from schema_2817.table_2817;
select * from schema_2817.function_2817();
drop function schema_2817.function_2817;
drop table schema_2817.table_2817;
drop schema schema_2817 cascade;
