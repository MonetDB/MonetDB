
CREATE TABLE parent2(a int, b int, PRIMARY KEY(a,b));
-- normal correct FK definition:
CREATE TABLE child1(x int, y int, FOREIGN KEY(x,y) REFERENCES parent2);

-- FK definition with more columns than the PK:
CREATE TABLE child3(x int,y int,z int, FOREIGN KEY(x,y,z) REFERENCES parent2);
-- it correctly returns an error

-- FK definition with less columns than the PK:
CREATE TABLE child2(x int REFERENCES parent2);
-- it is accepted but I prefer to get a warning

-- show PK columns
SELECT table_name, column_name, key_name, key_col_nr, key_type, depend_type FROM dependency_columns_on_keys WHERE table_name LIKE 'parent%' AND key_name LIKE 'parent%';

-- show FK columns
SELECT key_name, fk_name, key_type, depend_type FROM dependency_keys_on_foreignkeys WHERE fk_name LIKE 'child%';

drop table child1;
drop table parent2;
