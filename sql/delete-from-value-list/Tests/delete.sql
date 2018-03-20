create table table1 (id int);

create procedure fill_table1(maximum_size bigint)
begin
    declare val bigint;
    set val = 0;

    while val < maximum_size do
        insert into table1 values (val);

        set val = val + 1;
    end while;
end;

call fill_table1(1000000);

create table subtable1 (id int);

insert into subtable1 select id from table1 where id % 5 = 0;

select count(*) from subtable1;
