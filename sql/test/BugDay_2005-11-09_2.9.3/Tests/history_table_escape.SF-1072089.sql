START TRANSACTION;

DELETE FROM history;
record SELECT 1;
record SELECT 'test';
SELECT query FROM history;
