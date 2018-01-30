START TRANSACTION;
CREATE TABLE sys.objcts (
	"id"   INTEGER,
	"name" VARCHAR(1024),
	"nr"   INTEGER
);
COPY 54 RECORDS INTO sys.objcts FROM stdin USING DELIMITERS '\t','\n','"';
6965	"srid"	0
6964	"srid"	0
7676	"keyword"	0
7675	"keyword"	0
7681	"table_type_id"	0
7684	"table_type_name"	0
7680	"table_type_id"	0
7683	"table_type_name"	0
7689	"dependency_type_id"	0
7692	"dependency_type_name"	0
7688	"dependency_type_id"	0
7691	"dependency_type_name"	0
7697	"function_type_id"	0
7700	"function_type_name"	0
7696	"function_type_id"	0
7699	"function_type_name"	0
7709	"language_id"	0
7712	"language_name"	0
7708	"language_id"	0
7711	"language_name"	0
7717	"key_type_id"	0
7720	"key_type_name"	0
7716	"key_type_id"	0
7719	"key_type_name"	0
7725	"index_type_id"	0
7728	"index_type_name"	0
7724	"index_type_id"	0
7727	"index_type_name"	0
7733	"privilege_code_id"	0
7736	"privilege_code_name"	0
7732	"privilege_code_id"	0
7735	"privilege_code_name"	0
8159	"file_id"	0
8158	"file_id"	0
8170	"sn"	0
8170	"file_id"	1
8172	"file_id"	0
8169	"sn"	0
8169	"file_id"	1
8171	"file_id"	0
8189	"id"	0
8189	"file_id"	1
8191	"file_id"	0
8188	"id"	0
8188	"file_id"	1
8190	"file_id"	0
8201	"id"	0
8201	"file_id"	1
8203	"file_id"	0
8200	"id"	0
8200	"file_id"	1
8202	"file_id"	0
8300	"id"	0
8299	"id"	0

select  
  sample_2.name as c0, 
  sample_2.id as c1, 
  sample_2.name as c2, 
  cast(coalesce(sample_2.id,
    sample_2.id) as int) as c3, 
  sample_2.id as c4, 
  11 as c5, 
  sample_2.id as c6
from 
  sys.objcts as sample_2
where sample_2.name is not NULL
limit 61;

ROLLBACK;
