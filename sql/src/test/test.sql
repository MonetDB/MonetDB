/*
CREATE SCHEMA schema1 AUTHORIZATION sqlusers  
	CREATE TABLE test (n INTEGER);
CREATE SCHEMA niels AUTHORIZATION sqlusers  
	CREATE TABLE image (n INTEGER NOT NULL);
DROP SCHEMA niels CASCADE;

create table synch
(
    val1 int,
    val2 int,
    val3 int
);
create table comment
(
        timestamp       datetime,
        comment         varchar(30)
);
create table master_sync
(

        function     char(22),
        ramp_up      int,
        steady_state int,
        ramp_down    int,
        scale        int,
        history      int
);
create table run_id ( val        int);
create table stat_table
(
        tran_count      int,
        tot_res         float,
        min_res         int,
        max_res         int,
        not_done        int,
        tran_sqr        float,
        tran_2sec       int, 
	test_OK		int
);
create table histogram
(
        interval        int,
        tran_count      int
);
create table resp_hist
(
        bucket          int,
        value           int
);
*/
/*
SELECT * INTO :ka FROM test;
SELECT * INTO :ka FROM test, test2 WHERE a = 10 AND b = 20;
SELECT * INTO :ka FROM test, test2 WHERE a BETWEEN 10 AND 20;
SELECT * INTO :ka FROM test, test2 WHERE a LIKE 20;
SELECT * INTO :ka FROM test WHERE a IS NOT NULL;
*/
create table people
(
        first_name      varchar(30),
        last_name       varchar(30),
	pc		varchar(8),
	street_nr	int,
	bank_nr		int
);
create table address
(
	place		varchar(30),
	street_nr	int,
	pc		varchar(8)
);
create table account
(
	bank_nr		int,
	account_nr	varchar(30),
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

SELECT first_name, last_name, account_nr, saldo - 10000 
FROM people, account  
WHERE last_name = 'nes' 
AND saldo > 10000  
AND people.bank_nr = account.bank_nr
;

SELECT first_name, last_name, account_nr, saldo - 10000 
FROM people, account  
WHERE last_name = 'nes' 
AND people.bank_nr = account.bank_nr
AND saldo > 10000
;

SELECT first_name, last_name, account_nr, saldo - 10000 
FROM people, account, address  
WHERE address.place = 'HOORN' 
AND people.bank_nr = account.bank_nr
AND people.street_nr = address.street_nr
AND people.pc = address.pc
AND saldo > 10000
ORDER BY last_name DESC, first_name
;

SELECT first_name, last_name, account_nr, saldo - 10000 
FROM people, account  
WHERE last_name = 'nes' 
AND people.bank_nr = account.bank_nr
AND (saldo > 10000  OR first_name = 'niels')
;

SELECT max(saldo)
FROM account
WHERE saldo > 10000
;

commit;
