CREATE  STREAM TABLE strt  (id int primary key, nm varchar(123) NOT NULL);
CREATE merge TABLE strt  (id int primary key, nm varchar(123) NOT NULL);
\d
SELECT table_id, * FROM sys._columns WHERE (table_id) NOT IN (SELECT id FROM sys._tables);
-- no rows

DROP TABLE strt;
\d
SELECT table_id, * FROM sys._columns WHERE (table_id) NOT IN (SELECT id FROM sys._tables);
-- shows 2 columns which reference a table_id which does not exist in sys._tables

