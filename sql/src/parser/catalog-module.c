
#include "mem.h"
#include "catalog.h"

#include <gdk.h>

/*
schema *catalog_schema_create( catalog *cat, char *name, char *user, char *auth);
*/

#define BUNfind(t,v) 	BUNtail(t,BUNfnd(t,v))

ptr BUNfind_safe(BAT *b, oid *tid){
	BUN p = BUNfnd(b,tid);
	if (p) return BUNtail(b,p);
	return NULL;
}

catalog *catalog_create(){
	BUN p,q;
	catalog *c = default_catalog_create();
	int i, tcnt;

	/* schema, name */
	/*
	BAT *schema_info_name; 
	BAT *schema_info_user;
	BAT *schema_info_auth;
	*/

	BAT *table_name; /* table, name */
	BAT *table_temp;
	BAT *table_schema;
	BAT *table_query;

	BAT *column_name; /* column, name */
	BAT *column_type;
	BAT *column_table; /* column, table */
	BAT *column_default;
	BAT *column_null;
	BAT *column_number;

	BAT *sql_type_name;
	BAT *sql_type_monet;
	BAT *sql_type_cast;

	BAT *sql_aggr_name;
	BAT *sql_func_name;

	/*
	schema_info = BATdescriptor(BBPindex("schema_info"));
	schema_info_name = BATdescriptor(BBPindex("schema_info_name"));
	schema_info_user = BATdescriptor(BBPindex("schema_info_user"));
	schema_info_auth = BATdescriptor(BBPindex("schema_info_auth"));
	*/

	table_name = BATdescriptor(BBPindex("table_name"));
	table_temp = BATdescriptor(BBPindex("table_temp"));
	table_schema = BATdescriptor(BBPindex("table_schema"));
	table_query = BATdescriptor(BBPindex("table_query"));

	column_name = BATdescriptor(BBPindex("column_name"));
	column_type = BATdescriptor(BBPindex("column_type"));
	column_table = BATdescriptor(BBPindex("column_table"));
	column_default = BATdescriptor(BBPindex("column_default"));
	column_null = BATdescriptor(BBPindex("column_null"));
	column_number = BATdescriptor(BBPindex("column_number"));

	sql_type_name = BATdescriptor(BBPindex("sql_type_name" ));
	sql_type_monet = BATdescriptor(BBPindex("sql_type_monet" ));
	sql_type_cast = BATdescriptor(BBPindex("sql_type_cast" ));

	sql_aggr_name = BATdescriptor(BBPindex("sql_aggr_name"));
	sql_func_name = BATdescriptor(BBPindex("sql_func_name"));
	 
	tcnt = BATcount(sql_type_name);
	c->types = list_create();
	BATloop(sql_type_name, p, q){
	    int tnr = *(int*)BUNhead(sql_type_name, p );
	    char *sqlname = (char*)BUNtail(sql_type_name, p);
	    char *mntname = (char*)BUNfind(sql_type_monet, &tnr);
	    char *cast = (char*)BUNfind(sql_type_cast, &tnr);
	    c->create_type( c, sqlname, mntname, cast, tnr );
	}
	/* TODO load proper type cast table */

	tcnt = BATcount(sql_aggr_name);
	c->aggrs = list_create();
	BATloop(sql_aggr_name, p, q){
	    int tnr = *(int*)BUNhead(sql_aggr_name, p );
	    char *tname = (char*)BUNtail(sql_aggr_name, p);
	    c->create_aggr( c, tname, tnr );
	}

	tcnt = BATcount(sql_func_name);
	c->funcs = list_create();
	BATloop(sql_func_name, p, q){
	    int tnr = *(int*)BUNhead(sql_func_name, p );
	    char *tname = (char*)BUNtail(sql_func_name, p);
	    c->create_func( c, tname, tnr );
	}

	c->tables = list_create();
	/* bats are void-aligned */
	BATloop(table_name, p, q){
	    BUN v,w;
	    oid *tid = (oid*)BUNhead(table_name, p );
	    char *tname = (char*)BUNtail(table_name, p);
	    bit temp = *(bit*)BUNfind(table_temp, tid);
	    char *query = (char*)BUNfind_safe(table_query, tid);
	    BAT *columns = BATselect( column_table, tid, tid );

	    if (query){
		sql_execute( query, stdout, 0, c );
	    } else { 
	      table *t = c->create_table( c, tname, temp );
	      BATloop( columns, v, w ){
		oid *cid = (oid*)BUNhead(columns, v );
		char *cname = (char*)BUNfind(column_name, cid);
		char *ctype = (char*)BUNfind(column_type, cid);
		char *def = (char*)BUNfind(column_default, cid);
		int nll = *(int*)BUNfind(column_null, cid);
		int seqnr = *(int*)BUNfind(column_number, cid);
		c->create_column( c, t, cname, ctype, def, nll, seqnr );
	      }
	    }
	}
	/* loop over all schema's call create_schema */
	/* loop over all table's call create_table */
	/* loop over all column's call create_column */

	return c;
}
