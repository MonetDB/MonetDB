connect to 'localhost' database 'mTests_src_test_Connections_test1';
select * from connections;
disconnect 'localhost:port_num_mTests_src_test_Connections_test1';
connect to 'localhost' port port_num database 'mTests_src_test_Connections_test1' as 'test_db' user 'monetdb' password 'monetdb' language 'sql';
connect to 'localhost' port port_num5 database 'mTests_src_test_Connections_test1' as 'test_db1' user 'monetdb' password 'monetdb' language 'sql';
connect to 'localhost' port port_num database 'mTests_src_test_Connections_test1' as 'localhost:port_num_mTests_src_test_Connections_test1' user 'monetdb' password 'monetdb' language 'sql';
connect to 'localhost' port port_num6 database 'mTests_src_test_Connections_test1';
select * from connections;
disconnect ALL;
