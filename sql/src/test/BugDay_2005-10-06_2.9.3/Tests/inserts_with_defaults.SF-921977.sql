CREATE TABLE null_1 (
id INT NOT NULL,
text1 VARCHAR(32) NOT NULL,
text2 VARCHAR(32) NOT NULL DEFAULT 'foo'
);
commit;

INSERT INTO null_1 (id) VALUES(1);
rollback;
INSERT INTO null_1 (text1) VALUES('test');
rollback;
INSERT INTO null_1 (id,text1) VALUES(1,'test');

select * from null_1;
