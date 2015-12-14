-- This Source Code Form is subject to the terms of the Mozilla Public
-- License, v. 2.0.  If a copy of the MPL was not distributed with this
-- file, You can obtain one at http://mozilla.org/MPL/2.0/.
--
-- Copyright 2008-2015 MonetDB B.V.

-- create additional tables in "sys" schema

CREATE TABLE sys.keywords (
    keyword VARCHAR(40) NOT NULL PRIMARY KEY);

INSERT INTO sys.keywords (keyword) VALUES
('ADD'), ('ADMIN'), ('AFTER'), ('AGGREGATE'), ('ALL'), ('ALTER'), ('ALWAYS'), ('AND'), ('ANY'), ('ASC'), ('ASYMMETRIC'), ('ATOMIC'), ('AUTO_INCREMENT'),
('BEFORE'), ('BEGIN'), ('BEST'), ('BETWEEN'), ('BIGINT'), ('BIGSERIAL'), ('BINARY'), ('BLOB'), ('BY'),
('CALL'), ('CASCADE'), ('CASE'), ('CAST'), ('CHAIN'), ('CHAR'), ('CHARACTER'), ('CHECK'), ('CLOB'), ('COALESCE'), ('COMMIT'), ('COMMITTED'), ('CONSTRAINT'), ('CONVERT'), ('COPY'), ('CORRESPONDING'), ('CREATE'), ('CROSS'), ('CURRENT'), ('CURRENT_DATE'), ('CURRENT_ROLE'), ('CURRENT_TIME'), ('CURRENT_TIMESTAMP'), ('CURRENT_USER'),
('DAY'), ('DEC'), ('DECIMAL'), ('DECLARE'), ('DEFAULT'), ('DELETE'), ('DELIMITERS'), ('DESC'), ('DO'), ('DOUBLE'), ('DROP'),
('EACH'), ('EFFORT'), ('ELSE'), ('ELSEIF'), ('ENCRYPTED'), ('END'), ('ESCAPE'), ('EVERY'), ('EXCEPT'), ('EXCLUDE'), ('EXISTS'), ('EXTERNAL'), ('EXTRACT'),
('FALSE'), ('FLOAT'), ('FOLLOWING'), ('FOR'), ('FOREIGN'), ('FROM'), ('FULL'), ('FUNCTION'),
('GENERATED'), ('GLOBAL'), ('GRANT'), ('GROUP'),
('HAVING'), ('HOUR'), ('HUGEINT'),
('IDENTITY'), ('IF'), ('ILIKE'), ('IN'), ('INDEX'), ('INNER'), ('INSERT'), ('INT'), ('INTEGER'), ('INTERSECT'), ('INTO'), ('IS'), ('ISOLATION'),
('JOIN'),
('LEFT'), ('LIKE'), ('LIMIT'), ('LOCAL'), ('LOCALTIME'), ('LOCALTIMESTAMP'), ('LOCKED'),
('MEDIUMINT'), ('MERGE'), ('MINUTE'), ('MONTH'),
('NATURAL'), ('NEW'), ('NEXT'), ('NOCYCLE'), ('NOMAXVALUE'), ('NOMINVALUE'), ('NOT'), ('NOW'), ('NULL'), ('NULLIF'), ('NUMERIC'),
('OF'), ('OFFSET'), ('OLD'), ('ON'), ('ONLY'), ('OPTION'), ('OR'), ('ORDER'), ('OTHERS'), ('OUTER'), ('OVER'),
('PARTIAL'), ('PARTITION'), ('POSITION'), ('PRECEDING'), ('PRESERVE'), ('PRIMARY'), ('PRIVILEGES'), ('PROCEDURE'), ('PUBLIC'),
('RANGE'), ('READ'), ('REAL'), ('RECORDS'), ('REFERENCES'), ('REFERENCING'), ('REMOTE'), ('RENAME'), ('REPEATABLE'), ('REPLICA'), ('RESTART'), ('RESTRICT'), ('RETURN'), ('RETURNS'), ('REVOKE'), ('RIGHT'), ('ROLLBACK'), ('ROWS'),
('SAMPLE'), ('SAVEPOINT'), ('SECOND'), ('SELECT'), ('SEQUENCE'), ('SERIAL'), ('SERIALIZABLE'), ('SESSION_USER'), ('SET'), ('SIMPLE'), ('SMALLINT'), ('SOME'), ('SPLIT_PART'), ('STDIN'), ('STDOUT'), ('STORAGE'), ('STREAM'), ('STRING'), ('SUBSTRING'), ('SYMMETRIC'),
('THEN'), ('TIES'), ('TINYINT'), ('TO'), ('TRANSACTION'), ('TRIGGER'), ('TRUE'),
('UNBOUNDED'), ('UNCOMMITTED'), ('UNENCRYPTED'), ('UNION'), ('UNIQUE'), ('UPDATE'), ('USER'), ('USING'),
('VALUES'), ('VARCHAR'), ('VARYING'), ('VIEW'),
('WHEN'), ('WHERE'), ('WHILE'), ('WITH'), ('WORK'), ('WRITE'),
('XMLAGG'), ('XMLATTRIBUTES'), ('XMLCOMMENT'), ('XMLCONCAT'), ('XMLDOCUMENT'), ('XMLELEMENT'), ('XMLFOREST'), ('XMLNAMESPACES'), ('XMLPARSE'), ('XMLPI'), ('XMLQUERY'), ('XMLSCHEMA'), ('XMLTEXT'), ('XMLVALIDATE');

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
