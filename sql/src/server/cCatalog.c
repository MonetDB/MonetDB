/* multi version catalog */

#include "mem.h"
#include "cCatalog.h"

static 
ptr *ADTfromStr( int type, char *s){
        int l = 0;
        ptr *res = NULL;
        if (type == TYPE_str) {
		if (*s == '"') {
			int len = strlen(s);
			char *r = GDKstrdup( s+1 );
			r[len-2] = '\0';
			res = (ptr*)r;
		} else {
                	res = (ptr*)GDKstrdup(s);
		}
	} else
                BATatoms[type].atomFromStr(s, &l, (ptr)&res);
        return res;
}

void bat_incref(BAT *b){
	BBPfix(b->batCacheid);
}

mvc *mvc_create(int debug){
	mvc *c = NEW(mvc);

	if (debug) printf("mvc_create\n");
	c->debug = debug;

	c->type_sql = BATdescriptor(BBPindex("type_sql"));
	bat_incref(c->type_sql);
	c->type_db = BATdescriptor(BBPindex("type_db"));
	bat_incref(c->type_db);

	c->schema_id = BATdescriptor(BBPindex("schema_id"));
	bat_incref(c->schema_id);
	c->schema_name = BATdescriptor(BBPindex("schema_name"));
	bat_incref(c->schema_name);
	c->schema_auth = BATdescriptor(BBPindex("schema_auth"));
	bat_incref(c->schema_auth);

	c->table_id = BATdescriptor(BBPindex("table_id"));
	bat_incref(c->table_id);
	c->table_name = BATdescriptor(BBPindex("table_name"));
	bat_incref(c->table_name);
	c->table_temp = BATdescriptor(BBPindex("table_temp"));
	bat_incref(c->table_temp);
	c->table_schema = BATdescriptor(BBPindex("table_schema"));
	bat_incref(c->table_schema);
	c->table_query = BATdescriptor(BBPindex("table_query"));
	bat_incref(c->table_query);

	c->column_id = BATdescriptor(BBPindex("column_id"));
	bat_incref(c->column_id);
	c->column_name = BATdescriptor(BBPindex("column_name"));
	bat_incref(c->column_name);
	c->column_type = BATdescriptor(BBPindex("column_type"));
	bat_incref(c->column_type);
	c->column_table = BATdescriptor(BBPindex("column_table"));
	bat_incref(c->column_table);
	c->column_bat = BATdescriptor(BBPindex("column_bat"));
	bat_incref(c->column_bat);
	c->column_default = BATdescriptor(BBPindex("column_default"));
	bat_incref(c->column_default);
	c->column_null = BATdescriptor(BBPindex("column_null"));
	bat_incref(c->column_null);
	c->column_number = BATdescriptor(BBPindex("column_number"));
	bat_incref(c->column_number);

	c->size = BATcount(c->column_id) + 1024;
	c->batptrs = NEW_ARRAY( BAT*, c->size); 
	memset(c->batptrs, 0, c->size*sizeof(BAT*) );
	return c;
}

BAT *mvc_bind( mvc *c, oid colid ){
	BAT *m = BATmirror(c->column_id);
	oid cid = *(oid*)BUNtail(m,BUNfnd(m, (ptr)&colid));

	if (c->debug) 
		printf("mvc_bind %d, %d\n", colid, cid);

	if (cid > c->size){
		c->size = cid + 1024;
		c->batptrs = RENEW_ARRAY(BAT*,c->batptrs,c->size);
	}
	if (!c->batptrs[cid]){
		BAT *b = BATdescriptor(*(bat*)BUNtail(c->column_bat,
					BUNfnd(c->column_bat, (ptr)&cid)));
		c->batptrs[cid] = b;
		bat_incref(b);
	}
	return c->batptrs[cid];
}

oid mvc_create_schema( mvc *c, oid sid, char *name, char *auth){
	oid s = BATcount( c->schema_name );	

	if (c->debug) 
		printf("mvc_crate_schema %d %s %s\n", sid, name, auth);

	BUNins(c->schema_id, 	(ptr)&s, (ptr)&sid);
	BUNins(c->schema_name, 	(ptr)&s, (ptr)name );
	BUNins(c->schema_auth, 	(ptr)&s, (ptr)auth );
	return s;
}

static
void drop_column( mvc *c, oid id ){
	BAT *b = BATdescriptor(*(bat*)BUNtail(c->column_bat,
				BUNfnd(c->column_bat, (ptr)&id)));

	BBPtransient(b->batCacheid);
	BUNdelHead(c->column_id, 	(ptr)&id );
	BUNdelHead(c->column_table, 	(ptr)&id );
	BUNdelHead(c->column_name, 	(ptr)&id );
	BUNdelHead(c->column_type, 	(ptr)&id );
	BUNdelHead(c->column_default, 	(ptr)&id );
	BUNdelHead(c->column_null, 	(ptr)&id );
	BUNdelHead(c->column_number, 	(ptr)&id );
	BUNdelHead(c->column_bat, 	(ptr)&id );
}

static
void drop_table( mvc *c, oid id, oid tid ){
	BAT *columns = BATselect(c->column_table, (ptr)&tid, (ptr)&tid);
	ptr p,q;

	BATloop(columns, p, q ){
		drop_column( c, *(oid*)BUNhead(columns,p));
	}	
	BBPreclaim(columns);

	BUNdelHead(c->table_id, (ptr)&id );
	BUNdelHead(c->table_schema, (ptr)&id );
	BUNdelHead(c->table_name, (ptr)&id );
	BUNdelHead(c->table_temp, (ptr)&id );
}

void mvc_drop_schema( mvc *c, oid sid ){
	BAT *m = BATmirror(c->schema_id);
	oid id = *(oid*)BUNtail(m, BUNfnd(m, (ptr)&sid));
	BAT *tables = BATselect(c->table_schema, (ptr)&sid, (ptr)&sid);
	ptr p,q;
	
	BATloop(tables, p, q){
		drop_table( c, 
			*(oid*)BUNhead(tables,p), 
			*(oid*)BUNhead(tables,q));
	}	
	BBPreclaim(tables);

	BUNdelHead( c->schema_id, (ptr)&id );
	BUNdelHead( c->schema_name, (ptr)&id );
	BUNdelHead( c->schema_auth, (ptr)&id );
}

oid mvc_create_table( mvc *c, oid tid, oid sid, char *name, bit temp){
	oid t = BATcount( c->table_name );	

	if (c->debug) 
		printf("mvc_crate_table %d %d %s %d\n", tid, sid, name, temp);

	BUNins(c->table_id, 	(ptr)&t, (ptr)&tid);
	BUNins(c->table_schema, 	(ptr)&t, (ptr)&sid );
	BUNins(c->table_name, 	(ptr)&t, (ptr)name );
	BUNins(c->table_temp, 	(ptr)&t, (ptr)&temp );
	return t;
}

void mvc_drop_table( mvc *c, oid tid, bit cascade ){
	BAT *m = BATmirror(c->table_id);
	oid id = *(oid*)BUNtail(m, BUNfnd(m, (ptr)&tid));
	drop_table(c, id, tid);
	/* TODO cascade, ie. remove al references to this table */
}

oid mvc_create_view( mvc *c, oid tid, oid sid, char *name, char *sql){
	oid v = mvc_create_table( c, tid, sid, name, 0 );
	BUNins(c->table_query, 	(ptr)&v, (ptr)sql );
	return v;
}

oid mvc_create_column( mvc *c, oid cid, oid tid, 
					char *name, char *sqltype, int seqnr ){
	BAT *type_sqlr = BATmirror(c->type_sql);
	bit one = 1;
	oid ci = BATcount( c->column_name );	
	char *typename = BUNtail(c->type_db,BUNfnd(c->type_db, 
			BUNtail(type_sqlr, BUNfnd(type_sqlr, sqltype))));
	int type = ATOMindex(typename);
	BAT *b = BATnew( TYPE_oid, type, 1000 );

	if (c->debug) 
		printf("mvc_crate_column %d %d %s %s  %d\n", 
					cid, tid, name, sqltype, seqnr);

	BBPpersistent(b->batCacheid);
	BUNins(c->column_id, 	(ptr)&ci, (ptr)&cid );
	BUNins(c->column_table, 	(ptr)&ci, (ptr)&tid );
	BUNins(c->column_name, 	(ptr)&ci, (ptr)name );
	BUNins(c->column_type, 	(ptr)&ci, (ptr)sqltype );
	BUNins(c->column_default, 	(ptr)&ci, (ptr)"NULL" );
	BUNins(c->column_null, 	(ptr)&ci, (ptr)&one );
	BUNins(c->column_number, 	(ptr)&ci, (ptr)&seqnr );
	BUNins(c->column_bat, 	(ptr)&ci, (ptr)&b->batCacheid );
	mvc_bind( c, cid );

	return cid;
}
void mvc_drop_column( mvc *c, oid cid ){
	BAT *m = BATmirror(c->column_id);
	oid id = *(oid*)BUNtail(m, BUNfnd(m, (ptr)&cid));
	drop_column(c, id );
}
oid mvc_not_null( mvc *c, oid colid ){
	BAT *m = BATmirror(c->column_id);
	oid cid = *(oid*)BUNtail(m,BUNfnd(m, (ptr)&colid));
	bit F = 0;

	if (c->debug)
		printf("mvc_not_null %d %d\n", colid, cid );

	BUNreplace( c->column_null, (ptr)&cid, (ptr)&F );
	return colid;
}
oid mvc_default( mvc *c, oid colid, char *val ){
	BAT *m = BATmirror(c->column_id);
	oid cid = *(oid*)BUNtail(m,BUNfnd(m, (ptr)&colid));

	if (c->debug)
		printf("mvc_default %d %d %s\n", colid, cid, val );

	BUNreplace( c->column_default, (ptr)&cid, val );
	return colid;
}

char *next_single_quotes( char *s ){
	int escaped;

	s++; /* skip ' */
        escaped	= (*s == '\\');
	while(*s){
	       	if (*s == '\'' && !escaped)
		       return s+1;
		else
			escaped = (*s == '\\' && !escaped);
		s++;
	}
	return s;
}
char *next_double_quotes( char *s ){
	int escaped;

	s++; /* skip ' */
        escaped	= (*s == '\\');
	while(*s){
	       	if (*s == '\"' && !escaped){
		       	return s+1;
		} else {
			escaped = (*s == '\\' && !escaped);
		}
		s++;
	}
	return s;
}

char *next_comma( char *s ){
	int escaped = (*s == '\\');

	if (!escaped && (*s == '"')) return next_double_quotes (s);
	if (!escaped && (*s == '\'')) return next_single_quotes (s);

	while(*s != ',') s++;
	return s;
}


void mvc_insert( mvc *c, char *insert_string ){
	char *next = insert_string + 2;
	int nr = strtol( next, &next, 10);
	int cnt = 0;

	if (nr){
		oid id = strtol( next+1, &next, 10);
		BAT *b = mvc_bind(c, id);
		ptr *p;
		char *e = next_comma(next+1); 
		*e = '\0';
		cnt = BATcount(b);
	        p = ADTfromStr( b->dims.tailtype, next+1 );
		BUNins( b, (ptr)&cnt, p );
		GDKfree( p );
		nr--;
		next = e;

	    	while(nr > 0){
			oid id = strtol( next+1, &next, 10);
			BAT  *b = mvc_bind(c, id);
			char *e = next_comma(next+1); 
			*e = '\0';
	        	p = ADTfromStr( b->dims.tailtype, next+1 );
			BUNins( b, (ptr)&cnt, p );
			GDKfree( p );
			nr--;
			next = e;
	    	}
	}
}
