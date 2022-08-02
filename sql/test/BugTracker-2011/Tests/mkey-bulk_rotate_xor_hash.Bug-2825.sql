create table table1 (user_id  integer, item_id integer);
create table table2 (user_id  integer, item_id integer, primary key(user_id, item_id));

insert into table1 values (1,1);

INSERT INTO table2(user_id,item_id)
SELECT DISTINCT USER_ID, ITEM_ID 
FROM table1;

delete from table2;
insert into table1 values (1,1);

SELECT DISTINCT USER_ID, ITEM_ID
FROM table1;

INSERT INTO table2(user_id,item_id) 
SELECT  DISTINCT USER_ID, ITEM_ID 
FROM table1;

drop table table1;
drop table table2;
