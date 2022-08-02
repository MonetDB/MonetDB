CREATE TABLE null_1 (
id INT NOT NULL,
text1 VARCHAR(32) NOT NULL,
text2 VARCHAR(32) NOT NULL DEFAULT 'foo'
);

INSERT INTO null_1 (id) VALUES(1);
INSERT INTO null_1 (text1) VALUES('test');
INSERT INTO null_1 (id,text1) VALUES(1,'test');

select * from null_1;

drop table null_1;
