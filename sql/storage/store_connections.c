/*
 * The contents of this file are subject to the MonetDB Public License
 * Version 1.1 (the "License"); you may not use this file except in
 * compliance with the License. You may obtain a copy of the License at
 * http://www.monetdb.org/Legal/MonetDBLicense
 *
 * Software distributed under the License is distributed on an "AS IS"
 * basis, WITHOUT WARRANTY OF ANY KIND, either express or implied. See the
 * License for the specific language governing rights and limitations
 * under the License.
 *
 * The Original Code is the MonetDB Database System.
 *
 * The Initial Developer of the Original Code is CWI.
 * Portions created by CWI are Copyright (C) 1997-July 2008 CWI.
 * Copyright August 2008-2011 MonetDB B.V.
 * All Rights Reserved.
 */


#include "monetdb_config.h"
#include "store_connections.h"


/*Function to create a connection*/
int
sql_trans_connect_catalog(sql_trans* tr, char *server, int port, char *db, char *db_alias, char *user, char *passwd, char *lang)
{
	int id = store_next_oid(), port_l = port;
	sql_schema * s = find_sql_schema(tr, "sys");
	sql_table *t = find_sql_table(s, "connections");
	sql_column *c_server = find_sql_column(t, "server");
	sql_column *c_db = find_sql_column(t, "db");
	sql_column *c_db_alias = find_sql_column(t, "db_alias");

	if ((table_funcs.column_find_row(tr, c_server, server, c_db, db, NULL) == oid_nil) && (table_funcs.column_find_row(tr, c_db_alias, db_alias, NULL) == oid_nil)) {
		table_funcs.table_insert(tr, t, &id, server, &port_l, db, db_alias, user, passwd, lang);
		return id;
	}
	
	return 0;
}

/*Function to drop the connection*/
int
sql_trans_disconnect_catalog(sql_trans* tr, char * db_alias)
{
	oid rid = oid_nil;
	int id = 0;
	sql_schema * s = find_sql_schema(tr, "sys");
	sql_table* t = find_sql_table(s, "connections");

	sql_column * col_db_alias = find_sql_column(t, "db_alias");
	sql_column * col_id = find_sql_column(t, "id");

	rid = table_funcs.column_find_row(tr, col_db_alias, db_alias, NULL);
	if (rid != oid_nil) {
		id = *(int *) table_funcs.column_find_value(tr, col_id, rid);
		table_funcs.table_delete(tr, t, rid);
	} else {
		id = 0;
	}
	return id;
}

int
sql_trans_disconnect_catalog_ALL(sql_trans* tr)
{
	sql_schema * s = find_sql_schema(tr, "sys");
	sql_table* t = find_sql_table(s, "connections");

	sql_trans_clear_table(tr, t);
	return 1;
}
