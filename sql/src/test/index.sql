create table people
(
        first_name      varchar,
        last_name       varchar,
	pc		varchar,
	street_nr	int,
	bank_nr		int
);
create index names on people ( last_name, first_name );

create table address
(
	place		varchar,
	street_nr	int,
	pc		varchar
);
create table account
(
	bank_nr		int,
	account_nr	varchar,
	saldo		int
);

INSERT INTO people ( last_name, first_name, pc, street_nr, bank_nr ) VALUES ( 'nes', 'niels', '1628VT', 162, 0 );
INSERT INTO people VALUES ( 'lilian', 'nes', '1628VT', 162, 0 );
INSERT INTO people VALUES ( 'lilian', 'nes', '1628VT', 162, 0 );
INSERT INTO account VALUES ( 0, '12344', 2000 ); 
INSERT INTO account VALUES ( 0, '12345', 4000 ); 
INSERT INTO account VALUES ( 0, '12346', 10000 ); 

SELECT * 
FROM people 
WHERE first_name = 'niels'
;

