
connect to 'localhost' database 'demo';

select * from connections;

disconnect 'localhost_demo';

connect to 'localhost' port 40000 database 'demo' as 'test_db' user 'monetdb' password 'monetdb' language 'sql';

connect to 'localhost' port 50000 database 'demo' as 'test_db' user 'monetdb' password 'monetdb' language 'sql';

connect to 'localhost' port 40000 database 'demo' as 'localhost:40000_demo' user 'monetdb' password 'monetdb' language 'sql';

connect to 'localhost' port 60000 database 'demo';

select * from connections;

disconnect ALL;

