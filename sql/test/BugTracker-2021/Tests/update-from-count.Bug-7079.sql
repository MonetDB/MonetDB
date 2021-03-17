START TRANSACTION;
CREATE TABLE testme (a int, b varchar(32));
insert into testme values (1, 'a'), (2, 'b'), (3, 'c');
update testme set a = "y".a from testme "y", testme "z";
ROLLBACK;
