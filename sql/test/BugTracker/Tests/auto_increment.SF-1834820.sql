CREATE TABLE testusers (
id INT NOT NULL AUTO_INCREMENT,
name VARCHAR(40),
fullname VARCHAR(100),
PRIMARY KEY (id)
);


INSERT INTO testusers (id, name, fullname) VALUES (1, 'wendy', 'Wendy
Wones');
select * from testusers;

-- This will fail the first time
INSERT INTO testusers (name, fullname) VALUES ('fred', 'Fred Flintstone');
select * from testusers;

-- Now it will succeed
INSERT INTO testusers (name, fullname) VALUES ('fred', 'Fred Flintstone');
select * from testusers;

select count(*) from sequences;

drop table testusers;

select count(*) from sequences;
