CREATE TABLE bulk ( 
	id NUMERIC(9, 0) NOT NULL,
   	fax NUMERIC(10, 0),
        phone NUMERIC(10, 0),
        acctnum NUMERIC(7, 0)
);

COPY 3 RECORDS INTO bulk from stdin USING DELIMITERS '|', '\n';
1001|NULL|231231234|1001
1001|NULL|231231234|1001
1002|1234|1231231235|1002


select * from bulk;
drop table bulk;

CREATE TABLE bulk ( 
	id BIGINT NOT NULL,
   	fax BIGINT,
        phone BIGINT,
        acctnum BIGINT
);

COPY 3 RECORDS INTO bulk from stdin USING DELIMITERS '|', '\n';
1001|NULL|231231234|1001
1001|NULL|231231234|1001
1002|1234|1231231235|1002

select * from bulk;
drop table bulk;
