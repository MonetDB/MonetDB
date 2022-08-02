CREATE TABLE a (S varchar(255) PRIMARY KEY);
CREATE TABLE b (S varchar(255) PRIMARY KEY);

INSERT INTO a VALUES ('hallo');
INSERT INTO b SELECT S FROM a GROUP BY S;
select * from a;
select * from b;

drop table a;
drop table b;

CREATE TABLE a (S varchar(255) PRIMARY KEY);
CREATE TABLE b (S varchar(255) PRIMARY KEY);

INSERT INTO a VALUES ('hallo');
INSERT INTO b SELECT S FROM a;

drop table a;
drop table b;
