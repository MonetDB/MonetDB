connect to default;

disconnect all;

-- next should mostly fail

connect to 'whatever' port 50001 database 'nonexisting' USER 'monetdb' PASSWORD 'monetdb' LANGUAGE 'mal';
disconnect 'whatever';
