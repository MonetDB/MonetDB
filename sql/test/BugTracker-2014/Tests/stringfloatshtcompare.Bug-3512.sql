START TRANSACTION;
CREATE TABLE bug3512 (a VARCHAR(100) ) ;

INSERT INTO bug3512 (a) VALUES ('9999') ; 
INSERT INTO bug3512 (a) VALUES ('32767') ; 
INSERT INTO bug3512 (a) VALUES ('32768') ; 
INSERT INTO bug3512 (a) VALUES ('327670') ; 
-- this fails because values > 32767 cannot be represented as short
-- and MonetDB tries to cast the col to short
SELECT * FROM bug3512 WHERE a > 8888;

INSERT INTO bug3512 (a) VALUES ('0.0');
INSERT INTO bug3512 (a) VALUES ('0.10');
INSERT INTO bug3512 (a) VALUES ('9999.00') ; 

-- this fails because decimal numbers cannot be cast to short
SELECT * FROM bug3512 WHERE a = 9999;
ROLLBACK;
