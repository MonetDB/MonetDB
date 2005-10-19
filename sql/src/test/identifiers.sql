SELECT 1 AS bla; -- gives bla
SELECT 1 AS BLA; -- gives bla
SELECT 1 AS "bla"; -- gives bla
SELECT 1 AS "Bla"; -- gives Bla
SELECT 1 AS "BLA"; -- gives BLA
CREATE TABLE "B\"la\"" (id int); -- succeeds
SELECT 1 AS "B\"la\""; -- should succeed, gives B"la"
SELECT 1 AS "\"Bla\""; -- should fail
