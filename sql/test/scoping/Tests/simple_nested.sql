-- This is a simple global variables and a prepared statement test
-- BEWARE, life is much easier for the test writer if he can label a prepared statement.
START TRANSACTION;

CREATE SCHEMA A;
SET SCHEMA A;
DECLARE Avar string;
SET Avar='Avar';

PREPARE mytst AS SELECT Avar;
CALL mytst;  -- should show it

PREPARE mysts2 AS SELECT A.avar;
CALL mytst2; -- should show it

SET SCHEMA tmp;
PREPARE mysts3 AS SELECT A.avar;
CALL mytst3; -- should produce an error

ROLLBACK;
