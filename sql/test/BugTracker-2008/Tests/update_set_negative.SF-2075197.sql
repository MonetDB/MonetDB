
CREATE TABLE A (
GRADE decimal(4,0) DEFAULT NULL
);

INSERT INTO A VALUES (10);
INSERT INTO A VALUES (12);

select * from A;

UPDATE A SET GRADE = -GRADE;

select * from A;

drop table A;
