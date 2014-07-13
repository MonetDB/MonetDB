CREATE TABLE mmtest10 (  
	a1  varchar(4)   NOT NULL,
   	a2  bigint       NOT NULL,
	a3  bigint       NOT NULL,
	a4  bigint       NOT NULL );

CREATE TABLE mmtest20 (  
	a1  varchar(9)   NOT NULL, 
	a2  bigint       NOT NULL,
	a3  bigint       NOT NULL,
	a4  bigint       NOT NULL );

ALTER TABLE mmtest20 ADD CONSTRAINT mmtest20_uk1 UNIQUE (a1,a2,a3,a4);
SELECT p.a1, p.a2, p.a3, v.a3  
  FROM mmtest10 v JOIN mmtest20 p 
    ON (p.a1 = v.a1 AND p.a2 = v.a2 AND p.a3 = v.a3 AND p.a4 = v.a4);

drop table mmtest20;
drop table mmtest10;
