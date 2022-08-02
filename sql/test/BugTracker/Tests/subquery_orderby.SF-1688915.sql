create table foo (
id int,
name varchar(100)
);
copy 56 records into foo from stdin;
2001|schemas
2007|types
2016|functions
2027|args
2036|sequences
2046|dependencies
2050|connections
2059|_tables
2068|_columns
2079|keys
2086|idxs
2091|triggers
2102|objects
2107|_tables
2116|_columns
2127|keys
2134|idxs
2139|triggers
2150|objects
5956|tables
5966|columns
5982|db_user_info
5988|users
5992|user_role
5995|auths
5999|privileges
6221|querylog_catalog
6232|querylog_calls
6250|querylog_history
6289|tracelog
6432|sessions
6499|optimizers
6507|environment
6557|queue
6587|rejects
6905|spatial_ref_sys
6914|geometry_columns
7622|keywords
7630|table_types
7638|dependency_types
7655|netcdf_files
7661|netcdf_dims
7669|netcdf_vars
7675|netcdf_vardim
7683|netcdf_attrs
7749|storage
7827|storagemodelinput
7881|storagemodel
7891|tablestoragemodel
7905|statistics
8008|files
8021|sq
8040|rg
8052|pg
8065|export
8147|systemfunctions

select name from (select name, id from foo order by id) as x;

drop table foo;
