
--connect

connect to 'localhost' port 40000 database 'demo' as 'test_db_1' user 'monetdb' password 'monetdb' language 'sql';

connect to 'localhost' database 'demo' as 'test_db_1' user 'monetdb' password 'monetdb' language 'sql';

connect to 'localhost' port 40000 database 'demo' user 'monetdb' password 'monetdb' language 'sql';

connect to 'localhost' port 40000 database 'demo' as 'test_db_1' language 'sql';

connect to 'localhost' port 40000 database 'demo' as 'test_db_1' user 'monetdb' password 'monetdb';

connect to 'localhost' database 'demo';


--syntax errors

connect 'localhost' port 40000 database 'demo' as 'test_db_1' user 'monetdb' password 'monetdb' language 'sql';

connect to 'localhost' port 40000 as 'test_db_1' user 'monetdb' password 'monetdb' language 'sql';

--disconnect

disconnect ALL;

connect to 'localhost' port 40000 database 'demo' as 'test_db' user 'monetdb' password 'monetdb' language 'sql';

disconnect 'test_db';

