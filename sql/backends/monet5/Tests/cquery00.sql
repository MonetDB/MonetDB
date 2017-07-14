CREATE stream TABLE testing (a int);
INSERT INTO testing VALUES(123);

CREATE TABLE results (b int);

CREATE CONTINUOUS PROCEDURE myfirstcq() 
BEGIN 
	INSERT INTO results SELECT a FROM testing; 
END;

-- a continuous procedure can be called like any other procedure
CALL myfirstcq();

SELECT * FROM results;

select * FROM functions WHERE name = 'myfirstcq';

-- START_CONTINUOUS_PROCEDURE and STOP_CONTINOUS_PROCEDURE are defined for continuous queries.
