# Test IF (NOT) EXISTS functionality for tables

START TRANSACTION;
CREATE TABLE hello(i INTEGER);
INSERT INTO hello VALUES (2), (3), (4);
CREATE TABLE IF NOT EXISTS hello(i INTEGER);

# (2,3,4)
SELECT * FROM hello;
ROLLBACK;

# verify that the table is gone
# ()
SELECT name FROM tables WHERE name='hello';

START TRANSACTION;
CREATE TABLE hello(i INTEGER);
# !CREATE TABLE: name 'hello' already in use
CREATE TABLE hello(i INTEGER);
ROLLBACK;

START TRANSACTION;
# test DROP TABLE IF EXISTS
CREATE TABLE hello(i INTEGER);
# verify that the table exists
# ('hello')
SELECT name FROM tables WHERE name='hello';
DROP TABLE IF EXISTS hello;
DROP TABLE IF EXISTS hello;
# (1)
SELECT 1;
ROLLBACK;

START TRANSACTION;
# this should still fail
CREATE TABLE hello(i INTEGER);
DROP TABLE hello;
# !DROP TABLE: no such table 'hello'
DROP TABLE hello;
ROLLBACK;

# Test IF (NOT) EXISTS functionality for schemas

START TRANSACTION;
# create the initial schema
CREATE SCHEMA IF NOT EXISTS hello;
# verify that the schema exists
# ('hello')
SELECT name FROM schemas WHERE name='hello';
CREATE SCHEMA IF NOT EXISTS hello;

# use the schema to make sure it's there
CREATE TABLE hello.tbl(i INTEGER);
INSERT INTO hello.tbl VALUES (3);
# (3)
SELECT * FROM hello.tbl;

ROLLBACK;

# verify that the schema is gone
# ()
SELECT name FROM schemas WHERE name='hello';

START TRANSACTION;
CREATE SCHEMA hello;
# CREATE SCHEMA: name 'hello' already in use
CREATE SCHEMA hello;
ROLLBACK;

START TRANSACTION;
# test DROP SCHEMA IF EXISTS
CREATE SCHEMA hello;
# verify that the schema exists
# ('hello')
SELECT name FROM schemas WHERE name='hello';
DROP SCHEMA IF EXISTS hello;
DROP SCHEMA IF EXISTS hello;
# (1)
SELECT 1;

# verify that the schema is gone
# ()
SELECT name FROM schemas WHERE name='hello';
ROLLBACK;


START TRANSACTION;
# this should still fail
CREATE SCHEMA hello;
DROP SCHEMA hello;
# !DROP SCHEMA: name hello does not exist
DROP SCHEMA hello;
ROLLBACK;
