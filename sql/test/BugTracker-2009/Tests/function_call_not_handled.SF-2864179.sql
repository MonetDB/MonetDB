create table table_a1 (
    table_a1_id integer not null auto_increment,
    column_a1 integer
);

create function insert_table_a1 (value_a1 integer)
returns integer
begin
    declare id_table_a1 integer;

    insert into table_a1 (
        column_a1 )
    values (
        value_a1
    );

    set id_table_a1 = (select max(table_a1_id)
                      from table_a1);

  return id_table_a1;
end;

create function insert_table_error ()
returns integer
begin
  declare id_table_a1 integer;

  set id_table_a1 = insert_table_a1 ( 1 );

  return 0;
end;

create function insert_table_correct ()
returns integer
begin
  declare id_table_a1 integer;

  set id_table_a1 = insert_table_a1 ( 1 );

  return id_table_a1;
end;

select insert_table_error();

select * from table_a1;

select insert_table_correct();

select * from table_a1;

