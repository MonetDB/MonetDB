
connect to 'localhost' port 40000 database 'demo' as 'test_db' user 'monetdb' password 'monetdb' language 'sql';

select * from connections;

disconnect 'test_db';

select * from connections;

connect to 'localhost' port 40000 database 'demo' as 'test_db' user 'monetdb' password 'monetdb' language 'sql';

connect to 'localhost' port 40000 database 'demo' as 'test_db' user 'monetdb' password 'monetdb' language 'sql';

connect to 'localhost' port 40000 database 'demo' as 'test' user 'monetdb' password 'monetdb' language 'sql';

connect to 'localhost' port 4000 database 'dmo' as 'test_db' user 'monetdb' password 'monetdb' language 'sql';

disconnect 'test_db';

disconnect 'test_db';

connect to 'localhost' port 40000 database 'demo' as 'test_db' user 'monetdb' password 'monetdb' language 'sql';

select * from connections;

disconnect ALL;

select * from connections;

disconnect 'test_tb';

