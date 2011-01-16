CREATE TABLE bugtest (
id INTEGER NOT NULL,
name VARCHAR(255)
);

INSERT INTO bugtest VALUES (1, 'André');
INSERT INTO bugtest VALUES (1, 'test');

SELECT * FROM bugtest;
