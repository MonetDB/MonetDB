select * from parent;
select * from child;

DELETE FROM parent WHERE key = 1;

select * from parent;
select * from child;

drop table child;
drop table parent;
