-- appeared in parallel processing experiment
-- Terminal 1 (Mapiclient)
START TRANSACTION;
CREATE TABLE aap (id int);
INSERT INTO aap VALUES (1);
INSERT INTO aap VALUES (6);
INSERT INTO aap VALUES (8);
INSERT INTO aap VALUES (10);
COMMIT;

-- Step 1:

SELECT * FROM aap;

-- Step 3:

UPDATE aap SET id=2 WHERE id=1;

-- Step 5:

-- Terminal 2 (Mapiclient):

-- Step 2:

SELECT * FROM aap;

-- Step 4:

UPDATE aap SET id=7 WHERE id=8;
SELECT * FROM aap;
DROP TABLE aap;
