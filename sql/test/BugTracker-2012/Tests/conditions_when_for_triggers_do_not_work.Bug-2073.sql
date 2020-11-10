create table t1_2073 (id serial, toggle boolean);
create table t2_2073 (id serial, ref bigint);

insert into t1_2073 (toggle) values (false);
insert into t1_2073 (toggle) values (false);

create trigger updateMe
        after update on t1_2073 referencing new row new_row
        for each row when ( new_row.toggle = true )
                insert into t2_2073 (ref) values (new_row.id);

update t1_2073 set toggle = true where id = 1;

select * from t2_2073;
drop table t1_2073 CASCADE;
drop table t2_2073 CASCADE;

create table t1_2073 (id serial, toggle boolean);
create table t2_2073 (id serial, ref bigint);

insert into t1_2073 (toggle) values (false);
insert into t1_2073 (toggle) values (false);

create trigger updateMe2
        after update on t1_2073 referencing new row new_row
        for each row insert into t2_2073 (ref) values (new_row.id);

update t1_2073 set toggle = true where id = 1;
select * from t2_2073;

drop table t1_2073 CASCADE;
drop table t2_2073 CASCADE;
