/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0.  If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 *
 * Copyright 1997 - July 2008 CWI, August 2008 - 2016 MonetDB B.V.
 */

#include "monetdb_config.h"
#include "store_connections.h"


/*Function to create a connection*/
int
sql_trans_connect_catalog(sql_trans *tr, const char *server, int port, const char *db, const char *db_alias, const char *user, const char *passwd, const char *lang)
{
	int id = store_next_oid(), port_l = port;
	sql_schema *s = find_sql_schema(tr, "sys");
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
sql_trans_disconnect_catalog(sql_trans *tr, const char *db_alias)
{
	oid rid = oid_nil;
	int id = 0;
	sql_schema *s = find_sql_schema(tr, "sys");
	sql_table *t = find_sql_table(s, "connections");

	sql_column *col_db_alias = find_sql_column(t, "db_alias");
	sql_column *col_id = find_sql_column(t, "id");

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
sql_trans_disconnect_catalog_ALL(sql_trans *tr)
{
	sql_schema *s = find_sql_schema(tr, "sys");
	sql_table *t = find_sql_table(s, "connections");

	sql_trans_clear_table(tr, t);
	return 1;
}
