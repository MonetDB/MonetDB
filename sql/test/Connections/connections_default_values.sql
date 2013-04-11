connect to 'localhost' port port_num database 'mTests_sql_src_test_Connections_test1' user 'monetdb' password 'monetdb';
select server, db, db_alias, language from connections;
disconnect 'localhost_mTests_sql_src_test_Connections_test1_monetdb';
connect to 'localhost' port port_num database 'mTests_sql_src_test_Connections_test1' as 'test_db' user 'monetdb' password 'monetdb' language 'sql';
disconnect ALL;
connect to 'localhost' port port_num database 'mTests_sql_src_test_Connections_test1' as 'localhost_mTests_sql_src_test_Connections_test1_monetdb' user 'monetdb' password 'monetdb' language 'sql';
disconnect 'localhost_mTests_sql_src_test_Connections_test1_monetdb';
select server, db, db_alias, language from connections;
disconnect ALL;
