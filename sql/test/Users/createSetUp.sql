

CREATE SCHEMA library;
CREATE ROLE libraryWorker;
CREATE TABLE library.orders(price int, name VARCHAR(100));


CREATE ROLE bankAdmin;
CREATE SCHEMA bank AUTHORIZATION bankAdmin;
CREATE TABLE bank.accounts(nr int, name VARCHAR(100));
CREATE TABLE bank.loans(nr int, amount int);

CREATE USER alice WITH PASSWORD 'alice' name 'alice' schema library;
CREATE USER april WITH PASSWORD 'april' name 'april' schema bank;

GRANT ALL ON bank.accounts to april;
GRANT bankAdmin to april;

