
#include <gdk.h>
#include "catalog.h"
#include "context.h"
#include "mem.h"
#include "sqlexecute.h"


#define BUNfind(t,v) 	BUNtail(t,BUNfnd(t,v))

typedef struct cc {
	context *lc;
} cc;

extern statement *sqlexecute( context *lc, char *buf );

static long oidrange( catalog *c, int nr ){
	return OIDnew(nr);
}

ptr BUNfind_safe(BAT *b, oid *tid){
	BUN p = BUNfnd(b,tid);
	if (p) return BUNtail(b,p);
	return NULL;
}

static void getfunctions( catalog *c ){
	BUN p,q;

	BAT *sql_type_name = BATdescriptor(BBPindex("type_sql" ));
	BAT *sql_type_monet = BATdescriptor(BBPindex("type_db" ));
	BAT *sql_type_cast = BATdescriptor(BBPindex("type_cast" ));

	BAT *sql_aggr_name = BATdescriptor(BBPindex("sql_aggr_name"));
	BAT *sql_aggr_imp = BATdescriptor(BBPindex("sql_aggr_imp"));
	BAT *sql_aggr_type = BATdescriptor(BBPindex("sql_aggr_type"));
	BAT *sql_aggr_result = BATdescriptor(BBPindex("sql_aggr_result"));

	BAT *sql_func_name = BATdescriptor(BBPindex("sql_func_name"));
	BAT *sql_func_imp = BATdescriptor(BBPindex("sql_func_imp"));
	BAT *sql_func_tpe1 = BATdescriptor(BBPindex("sql_func_type1"));
	BAT *sql_func_tpe2 = BATdescriptor(BBPindex("sql_func_type2"));
	BAT *sql_func_tpe3 = BATdescriptor(BBPindex("sql_func_type3"));
	BAT *sql_func_result = BATdescriptor(BBPindex("sql_func_result"));
	 
	c->types = list_create();
	BATloop(sql_type_name, p, q){
	    int tnr = *(int*)BUNhead(sql_type_name, p );
	    char *sqlname = (char*)BUNtail(sql_type_name, p);
	    char *mntname = (char*)BUNfind(sql_type_monet, &tnr);
	    char *cast = (char*)BUNfind(sql_type_cast, &tnr);
	    cat_create_type( c, sqlname, mntname, cast, tnr );
	}
	/* TODO load proper type cast table */

	c->aggrs = list_create();
	BATloop(sql_aggr_name, p, q){
	    int tnr = *(int*)BUNhead(sql_aggr_name, p );
	    char *tname = (char*)BUNtail(sql_aggr_name, p);
	    char *imp = (char*)BUNfind(sql_aggr_imp, &tnr);
	    char *tpe = (char*)BUNfind(sql_aggr_type, &tnr);
	    char *res = (char*)BUNfind(sql_aggr_result, &tnr);
	    cat_create_aggr( c, tname, imp, tpe, res, tnr );
	}

	c->funcs = list_create();
	BATloop(sql_func_name, p, q){
	    int tnr = *(int*)BUNhead(sql_func_name, p );
	    char *tname = (char*)BUNtail(sql_func_name, p);
	    char *imp = (char*)BUNfind(sql_func_imp, &tnr);
	    char *tpe1 = (char*)BUNfind(sql_func_tpe1, &tnr);
	    char *tpe2 = (char*)BUNfind(sql_func_tpe2, &tnr);
	    char *tpe3 = (char*)BUNfind(sql_func_tpe3, &tnr);
	    char *res = (char*)BUNfind(sql_func_result, &tnr);
	    cat_create_func( c, tname, imp, tpe1, tpe2, tpe3, res, tnr );
	}
}

static void getschema( catalog *c, char *schema, char *user ){
	oid *lid, id;
	BUN p,q;
	context *lc = ((cc*)c->cc)->lc;
	BAT *tableids;

	/* schema, name */
	BAT *schema_id; 
	BAT *schema_name;
	/*BAT *schema_auth;*/

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

	schema_id = BATdescriptor(BBPindex("schema_id"));
	schema_name = BATdescriptor(BBPindex("schema_name"));
	/*schema_auth = BATdescriptor(BBPindex("schema_auth"));*/

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

	c->schemas = list_create();
	lid = (oid*)BUNfind(BATmirror(schema_name), schema );
	id = *(oid*)BUNfind(schema_id, (ptr*)lid );
	c->cur_schema = cat_create_schema( c, id, schema, user );
	list_append_string( c->schemas, (char*) c->cur_schema );

	/* bats are void-aligned */
	tableids = BATselect( table_schema, (ptr)&id, (ptr)&id );
	BATloop(tableids, p, q){
	    BUN v,w;
	    oid *lid = (oid*)BUNhead(tableids, p );
	    char *tname = (char*)BUNfind_safe(table_name, lid);
	    oid *tid = (oid*)BUNfind_safe(table_id, lid );
	    bit temp = *(bit*)BUNfind(table_temp, lid);
	    char *query = (char*)BUNfind_safe(table_query, lid);
	    BAT *columns = BATselect( column_table, tid, tid );

	    if (query){
		sqlexecute( lc, query );
	    } else { 
	      table *t = cat_create_table( c, *tid, c->cur_schema, tname, temp, NULL );
	      BATloop( columns, v, w ){
		oid *lid = (oid*)BUNhead(columns, v );
		oid cid = *(oid*)BUNfind(column_id, lid);
		char *cname = (char*)BUNfind(column_name, lid);
		char *ctype = (char*)BUNfind(column_type, lid);
		char *def = (char*)BUNfind(column_default, lid);
		int nll = *(int*)BUNfind(column_null, lid);
		int seqnr = *(int*)BUNfind(column_number, lid);
		cat_create_column( c, cid, t, cname, ctype, def, nll, seqnr );
	      }
	    }
	    BBPreclaim(columns);
	}
	BBPreclaim(tableids);
	/* loop over all schema's call create_schema */
	/* loop over all table's call create_table */
	/* loop over all column's call create_column */
}

static void cc_destroy( catalog *c ){
	_DELETE(c->cc);
	c->cc = NULL;
}

catalog *catalog_create( context *lc ){
	cc *CC = NEW(cc);
	catalog *c = lc->cat;
	
	CC->lc = lc;
	c->cc = (char*)CC;
	c->cc_oidrange = &oidrange;
	c->cc_getschema = &getschema;
	c->cc_destroy = &cc_destroy;

	catalog_initoid( c );
	getfunctions( c );
	return c;
}


