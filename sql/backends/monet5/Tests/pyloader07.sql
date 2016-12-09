START TRANSACTION;
CREATE LOADER pyloader07() LANGUAGE PYTHON {
    _emit.emit({'s': 33, 't': 42});
 
};
CREATE TABLE pyloader07table FROM LOADER pyloader07();

SELECT * FROM pyloader07table;
DROP TABLE pyloader07table;
DROP LOADER pyloader07;
ROLLBACK;
