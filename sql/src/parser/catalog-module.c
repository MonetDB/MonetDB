
#include <gdk.h>
#include "catalog.h"
#include "context.h"
#include "mem.h"
#include "sqlexecute.h"


/*
schema *catalog_schema_create( catalog *cat, char *name, char *user, char *auth);
*/

#define BUNfind(t,v) 	BUNtail(t,BUNfnd(t,v))

ptr BUNfind_safe(BAT *b, oid *tid){
	BUN p = BUNfnd(b,tid);
	if (p) return BUNtail(b,p);
	return NULL;
}

catalog *catalog_create( context *lc ){
	catalog *c = lc->cat;
	int i, tcnt;
	BUN p,q;

	/* schema, name */
	/*
	BAT *schema_id; 
	BAT *schema_user;
	BAT *schema_auth;
	*/

	BAT *table_id; /* table, id */
	BAT *table_name; /* table, name */
	BAT *table_temp;
	BAT *table_schema;
	BAT *table_query;

	BAT *column_id; /* column, id */
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
	schema_id = BATdescriptor(BBPindex("schema_id"));
	schema_name = BATdescriptor(BBPindex("schema_name"));
	schema_auth = BATdescriptor(BBPindex("schema_auth"));
	*/

	table_id = BATdescriptor(BBPindex("table_id"));
	table_name = BATdescriptor(BBPindex("table_name"));
	table_temp = BATdescriptor(BBPindex("table_temp"));
	table_schema = BATdescriptor(BBPindex("table_schema"));
	table_query = BATdescriptor(BBPindex("table_query"));

	column_id = BATdescriptor(BBPindex("column_id"));
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

	c->schemas = list_create();
	c->cur_schema = c->create_schema( c, 0, "default-schema", "default-user" );
	list_append_string( c->schemas, (char*) c->cur_schema );

	/* bats are void-aligned */
	BATloop(table_name, p, q){
	    BUN v,w;
	    oid *lid = (oid*)BUNhead(table_name, p );
	    char *tname = (char*)BUNtail(table_name, p);
	    oid *tid = (oid*)BUNfind_safe(table_id, lid );
	    bit temp = *(bit*)BUNfind(table_temp, lid);
	    char *query = (char*)BUNfind_safe(table_query, lid);
	    BAT *columns = BATselect( column_table, tid, tid );

	    if (query){
		sqlexecute( lc, query );
	    } else { 
	      table *t = c->create_table( c, *tid, c->cur_schema, tname, temp, NULL );
	      BATloop( columns, v, w ){
		oid *lid = (oid*)BUNhead(columns, v );
		oid cid = (char*)BUNfind(column_id, lid);
		char *cname = (char*)BUNfind(column_name, lid);
		char *ctype = (char*)BUNfind(column_type, lid);
		char *def = (char*)BUNfind(column_default, lid);
		int nll = *(int*)BUNfind(column_null, lid);
		int seqnr = *(int*)BUNfind(column_number, lid);
		c->create_column( c, cid, t, cname, ctype, def, nll, seqnr );
	      }
	    }
	}
	/* loop over all schema's call create_schema */
	/* loop over all table's call create_table */
	/* loop over all column's call create_column */

	return c;
}
