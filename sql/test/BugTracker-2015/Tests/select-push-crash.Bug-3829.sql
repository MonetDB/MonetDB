START TRANSACTION;

CREATE TABLE kb (value INT);
SELECT 1 FROM kb WHERE value=1 AND (value=2 OR value=3 AND (value=4 OR value=5));
SELECT 1 FROM kb WHERE value AND (value OR value AND (value OR value));

ROLLBACK;
