CREATE stream TABLE testing (a int);

CREATE TABLE results (b int);

CREATE PROCEDURE stressing() BEGIN INSERT INTO results SELECT a FROM testing; END;

START CONTINUOUS stressing();
STOP CONTINUOUS stressing();
