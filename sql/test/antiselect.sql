CREATE TABLE antitest01 (
id    INTEGER      NOT NULL,
col1  VARCHAR(10)  NOT NULL,
col2  INTEGER      NOT NULL
) ;

CREATE TABLE antitest02 (
id    INTEGER      NOT NULL,
col1  VARCHAR(10)  NOT NULL,
col2  INTEGER      NOT NULL
) ;

INSERT INTO antitest01 VALUES (10,'ABC',21) ;
INSERT INTO antitest01 VALUES (11,'ABD',22) ;
INSERT INTO antitest01 VALUES (12,'ABE',23) ;
INSERT INTO antitest01 VALUES (13,'ABF',24) ;
INSERT INTO antitest01 VALUES (14,'ABC',25) ;
INSERT INTO antitest01 VALUES (15,'ABD',26) ;
INSERT INTO antitest01 VALUES (16,'ABE',27) ;
INSERT INTO antitest01 VALUES (17,'ABD',28) ;
INSERT INTO antitest01 VALUES (18,'ABE',29) ;
INSERT INTO antitest01 VALUES (19,'ABF',30) ;

insert into antitest02 select  * from antitest01 order by id, col2;

select * from antitest01;
select * from antitest01
where id <> 14;

select * from antitest01
where id between 14 and 16;

select * from antitest01
where id not between 14 and 16;

select * from antitest02;
select * from antitest02
where id <> 14;

select * from antitest02
where id between 14 and 16;

select * from antitest02
where id not between 14 and 16;

drop table antitest01;
drop table antitest02;
