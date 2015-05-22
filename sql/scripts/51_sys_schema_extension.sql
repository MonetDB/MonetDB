-- This Source Code Form is subject to the terms of the Mozilla Public
-- License, v. 2.0.  If a copy of the MPL was not distributed with this
-- file, You can obtain one at http://mozilla.org/MPL/2.0/.
--
-- Copyright 2008-2015 MonetDB B.V.

-- create additional tables in "sys" schema

CREATE TABLE sys.keywords (
    keyword VARCHAR(40) NOT NULL PRIMARY KEY);

INSERT INTO sys.keywords (keyword) VALUES
('ADMIN'), ('AFTER'), ('AGGREGATE'), ('ALWAYS'), ('ASYMMETRIC'), ('ATOMIC'), ('AUTO_INCREMENT'),
('BEFORE'), ('BIGINT'), ('BIGSERIAL'), ('BINARY'), ('BLOB'),
('CALL'), ('CHAIN'), ('CLOB'), ('COMMITTED'), ('COPY'), ('CORR'), ('CUME_DIST'), ('CURRENT_ROLE'), ('CYCLE'),
('DATABASE'), ('DELIMITERS'), ('DENSE_RANK'), ('DO'),
('EACH'), ('ELSEIF'), ('ENCRYPTED'), ('EVERY'), ('EXCLUDE'),
('FOLLOWING'), ('FUNCTION'),
('GENERATED'),
('IF'), ('ILIKE'), ('INCREMENT'),
('LAG'), ('LEAD'), ('LIMIT'), ('LOCALTIME'), ('LOCALTIMESTAMP'), ('LOCKED'),
('MAXVALUE'), ('MEDIAN'), ('MEDIUMINT'), ('MERGE'), ('MINVALUE'),
('NEW'), ('NOCYCLE'), ('NOMAXVALUE'), ('NOMINVALUE'), ('NOW'),
('OFFSET'), ('OLD'), ('OTHERS'), ('OVER'),
('PARTITION'), ('PERCENT_RANK'), ('PLAN'), ('PRECEDING'), ('PROD'),
('QUANTILE'),
('RANGE'), ('RANK'), ('RECORDS'), ('REFERENCING'), ('REMOTE'), ('RENAME'), ('REPEATABLE'), ('REPLICA'),
('RESTART'), ('RETURN'), ('RETURNS'), ('ROWS'), ('ROW_NUMBER'),
('SAMPLE'), ('SAVEPOINT'), ('SCHEMA'), ('SEQUENCE'), ('SERIAL'), ('SERIALIZABLE'), ('SIMPLE'),
('START'), ('STATEMENT'), ('STDIN'), ('STDOUT'), ('STREAM'), ('STRING'), ('SYMMETRIC'),
('TIES'), ('TINYINT'), ('TRIGGER'),
('UNBOUNDED'), ('UNCOMMITTED'), ('UNENCRYPTED'),
('WHILE'),
('XMLAGG'), ('XMLATTRIBUTES'), ('XMLCOMMENT'), ('XMLCONCAT'), ('XMLDOCUMENT'), ('XMLELEMENT'), ('XMLFOREST'),
('XMLNAMESPACES'), ('XMLPARSE'), ('XMLPI'), ('XMLQUERY'), ('XMLSCHEMA'), ('XMLTEXT'), ('XMLVALIDATE');


CREATE TABLE sys.table_types (
    table_type_id   SMALLINT NOT NULL PRIMARY KEY,
    table_type_name VARCHAR(25) NOT NULL UNIQUE);

INSERT INTO sys.table_types (table_type_id, table_type_name) VALUES
-- values from sys._tables.type:  0=Table, 1=View, 2=Generated, 3=Merge, etc.
  (0, 'TABLE'), (1, 'VIEW'), /* (2, 'GENERATED'), */ (3, 'MERGE TABLE'), (4, 'STREAM TABLE'), (5, 'REMOTE TABLE'), (6, 'REPLICA TABLE'),
-- synthetically constructed system obj variants (added 10 to sys._tables.type value when sys._tables.system is true).
  (10, 'SYSTEM TABLE'), (11, 'SYSTEM VIEW'),
-- synthetically constructed temporary variants (added 20 or 30 to sys._tables.type value depending on values of temporary and commit_action).
  (20, 'GLOBAL TEMPORARY TABLE'),
  (30, 'LOCAL TEMPORARY TABLE');


CREATE TABLE sys.dependency_types (
    dependency_type_id   SMALLINT NOT NULL PRIMARY KEY,
    dependency_type_name VARCHAR(15) NOT NULL UNIQUE);

INSERT INTO sys.dependency_types (dependency_type_id, dependency_type_name) VALUES
-- values taken from sql_catalog.h
  (1, 'SCHEMA'), (2, 'TABLE'), (3, 'COLUMN'), (4, 'KEY'), (5, 'VIEW'), (6, 'USER'), (7, 'FUNCTION'), (8, 'TRIGGER'),
  (9, 'OWNER'), (10, 'INDEX'), (11, 'FKEY'), (12, 'SEQUENCE'), (13, 'PROCEDURE'), (14, 'BE_DROPPED');
