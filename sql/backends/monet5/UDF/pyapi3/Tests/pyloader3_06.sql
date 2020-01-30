# test incorrect loaders

START TRANSACTION;


# loader emitting 0 elements
CREATE LOADER pyloader06() LANGUAGE PYTHON {
	return
};
CREATE TABLE pyloader06table FROM LOADER pyloader06();
ROLLBACK;

START TRANSACTION;
# loader with correct return value
CREATE LOADER pyloader06() LANGUAGE PYTHON {
	return {'a': 1, 'b': 2, 'c': 3}
};
CREATE TABLE pyloader06table FROM LOADER pyloader06();
SELECT * FROM pyloader06table;

ROLLBACK;

START TRANSACTION;
# loader with incorrect return value
CREATE LOADER pyloader06() LANGUAGE PYTHON {
	return 3
};
CREATE TABLE pyloader06table FROM LOADER pyloader06();

ROLLBACK;
