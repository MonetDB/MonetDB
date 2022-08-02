create function f1() RETURNS TABLE (id int, age int) BEGIN return
table(select 1, 2); end;
select f1();
drop function f1;
