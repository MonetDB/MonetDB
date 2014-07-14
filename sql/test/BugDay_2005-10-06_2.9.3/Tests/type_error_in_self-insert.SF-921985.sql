 CREATE TABLE insert_test (
id INT,
txt VARCHAR(32)
);

INSERT INTO insert_test VALUES(1, 'foo');
INSERT INTO insert_test VALUES(2, 'bar');

select * from insert_test;

INSERT INTO insert_test SELECT 3, txt FROM insert_test
WHERE id=2;

select * from insert_test;
