CREATE TABLE DOUBLE_TBL(x double);
INSERT INTO DOUBLE_TBL(x) VALUES ('NaN');
SELECT x, cast(x as varchar(30)) as x_str FROM DOUBLE_TBL;

INSERT INTO DOUBLE_TBL(x) VALUES ('-NaN');
SELECT x, cast(x as varchar(30)) as x_str FROM DOUBLE_TBL;

INSERT INTO DOUBLE_TBL(x) VALUES ('Inf');
INSERT INTO DOUBLE_TBL(x) VALUES ('Infinity');
INSERT INTO DOUBLE_TBL(x) VALUES ('inf');
INSERT INTO DOUBLE_TBL(x) VALUES ('infinity');
SELECT x, cast(x as varchar(30)) as x_str FROM DOUBLE_TBL;

INSERT INTO DOUBLE_TBL(x) VALUES ('-Inf');
INSERT INTO DOUBLE_TBL(x) VALUES ('-Infinity');
INSERT INTO DOUBLE_TBL(x) VALUES ('-inf');
INSERT INTO DOUBLE_TBL(x) VALUES ('-infinity');
SELECT x, cast(x as varchar(30)) as x_str FROM DOUBLE_TBL;



CREATE TABLE REAL_TBL(x real);
INSERT INTO REAL_TBL(x) VALUES ('NaN');
SELECT x, cast(x as varchar(30)) as x_str FROM REAL_TBL;

INSERT INTO REAL_TBL(x) VALUES ('-NaN');
SELECT x, cast(x as varchar(30)) as x_str FROM REAL_TBL;

INSERT INTO REAL_TBL(x) VALUES ('Inf');
INSERT INTO REAL_TBL(x) VALUES ('Infinity');
INSERT INTO REAL_TBL(x) VALUES ('inf');
INSERT INTO REAL_TBL(x) VALUES ('infinity');
SELECT x, cast(x as varchar(30)) as x_str FROM REAL_TBL;

INSERT INTO REAL_TBL(x) VALUES ('-Inf');
INSERT INTO REAL_TBL(x) VALUES ('-Infinity');
INSERT INTO REAL_TBL(x) VALUES ('-inf');
INSERT INTO REAL_TBL(x) VALUES ('-infinity');
SELECT x, cast(x as varchar(30)) as x_str FROM REAL_TBL;

INSERT INTO REAL_TBL(x) VALUES ('1e+39');
INSERT INTO REAL_TBL(x) VALUES ('-1e+39');
SELECT x, cast(x as varchar(30)) as x_str FROM REAL_TBL;



CREATE TABLE FLOAT_TBL(x float);
INSERT INTO FLOAT_TBL(x) VALUES ('NaN');
SELECT x, cast(x as varchar(30)) as x_str FROM FLOAT_TBL;

INSERT INTO FLOAT_TBL(x) VALUES ('-NaN');
SELECT x, cast(x as varchar(30)) as x_str FROM FLOAT_TBL;

INSERT INTO FLOAT_TBL(x) VALUES ('Inf');
INSERT INTO FLOAT_TBL(x) VALUES ('Infinity');
INSERT INTO FLOAT_TBL(x) VALUES ('inf');
INSERT INTO FLOAT_TBL(x) VALUES ('infinity');
SELECT x, cast(x as varchar(30)) as x_str FROM FLOAT_TBL;

INSERT INTO FLOAT_TBL(x) VALUES ('-Inf');
INSERT INTO FLOAT_TBL(x) VALUES ('-Infinity');
INSERT INTO FLOAT_TBL(x) VALUES ('-inf');
INSERT INTO FLOAT_TBL(x) VALUES ('-infinity');
SELECT x, cast(x as varchar(30)) as x_str FROM FLOAT_TBL;



DROP TABLE DOUBLE_TBL;
DROP TABLE REAL_TBL;
DROP TABLE FLOAT_TBL;
