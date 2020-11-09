start transaction;
create table a (id int primary key);
insert into a values (1);

create table b (id int primary key);

create table c (a int references a(id), b int references b(id));
insert into c (a, b) values (1, 2); -- 40002!INSERT INTO: FOREIGN KEY constraint 'c.c_b_fkey' violated
rollback;
