-- the common expressions should be removed
explain select count(*) from tables;
explain select count(*) from (select * from tables union select * from tables ) as xxx;
