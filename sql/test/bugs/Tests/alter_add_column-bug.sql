create schema myschema;
create table myschema.collection_info( test_col varchar(100));

ALTER TABLE "myschema"."COLLECTION_INFO" ADD COLUMN test_col1  varchar(256);

ALTER TABLE myschema.COLLECTION_INFO ADD COLUMN test_col2 varchar(256);

ALTER TABLE "myschema"."collection_info" ADD COLUMN test_col3  varchar(256);

ALTER TABLE myschema.collection_info ADD COLUMN test_col4  varchar(256);

drop table myschema.collection_info;
