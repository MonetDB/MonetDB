/* multi version catalog */

#include "mem.h"
#include "cCatalog.h"


#define SQL_READ 0
#define SQL_WRITE 1

static int stamp = 0;

static int timestamp(){
	return stamp++;
}

static 
ptr *ADTfromStr( int type, char *s){
        int l = 0;
        ptr *res = NULL;
        if (type == TYPE_str) {
		if (*s == '\1') {
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
	c->bats = NEW_ARRAY( batinfo, c->size); 
	memset(c->bats, 0, c->size*sizeof(batinfo) );
	return c;
}

void mvc_begin( mvc *c ){
	/* if started already commit the changes */
	printf( "TODO: time to implement BEGIN Transaction\n");
}
void mvc_commit( mvc *c ){
	printf( "TODO: time to implement COMMIT Transaction\n");
}
void mvc_rollback( mvc *c ){
	printf( "TODO: time to implement ROLLBACK Transaction\n");
}

void mvc_dump( mvc *c ){
	int i;
	for (i=0; i < c->size; i++){
		if (c->bats[i].b){
			printf("%s %d %d\n", 
				BUNtail(c->column_name,
				  BUNfnd(c->column_name, (ptr)&i)), 
				c->bats[i].rtime, c->bats[i].wtime);
		}
	}
}

BAT *mvc_bind_intern( mvc *c, oid colid, int kind ){
	BAT *m = BATmirror(c->column_id);
	oid cid = *(oid*)BUNtail(m,BUNfnd(m, (ptr)&colid));

	if (c->debug) 
		printf("mvc_bind_intern %d, %d\n", colid, cid);

	if (cid > c->size){
		int oldsz = c->size;
		c->size = cid + 1024;
		c->bats = RENEW_ARRAY(batinfo,c->bats,c->size);
		memset(c->bats+oldsz, 0, (c->size-oldsz)*sizeof(batinfo) );
	}
	/*
	if (!c->bats[cid].b){
		BAT *b = BATdescriptor(*(bat*)BUNtail(c->column_bat,
					BUNfnd(c->column_bat, (ptr)&cid)));
		c->bats[cid].b = b;
		bat_incref(b);
	}
	return c->bats[cid].b;
	*/
	c->bats[cid].b = (BAT*)1; 

	if (kind == SQL_READ){
		c->bats[cid].rtime = timestamp();
	} else {
		c->bats[cid].wtime = timestamp();
	}

	return BATdescriptor(*(bat*)BUNtail(c->column_bat,
				BUNfnd(c->column_bat, (ptr)&cid)));
}

BAT *mvc_bind( mvc *c, oid colid ){
	return mvc_bind_intern(c, colid, SQL_READ);
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

	BUNdelHead(c->column_id, 	(ptr)&id );
	BUNdelHead(c->column_table, 	(ptr)&id );
	BUNdelHead(c->column_name, 	(ptr)&id );
	BUNdelHead(c->column_type, 	(ptr)&id );
	BUNdelHead(c->column_default, 	(ptr)&id );
	BUNdelHead(c->column_null, 	(ptr)&id );
	BUNdelHead(c->column_number, 	(ptr)&id );
	BUNdelHead(c->column_bat, 	(ptr)&id );
	BATmode(b,TRANSIENT);
	if (id <= c->size && c->bats[id].b){
		BBPunfix(b->batCacheid);
		c->bats[id].b = 0;
	}
}

static
void drop_table( mvc *c, oid id, oid tid ){
	BAT *columns = BATselect(c->column_table, (ptr)&tid, (ptr)&tid);
	BUN p,q;

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
	BUN p,q;
	
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
	mvc_create_column( c, OIDnew(1), tid, "id", "OID", -1); 
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
	char buf[BUFSIZ];
	BAT *type_sqlr = BATmirror(c->type_sql);
	bit one = 1;
	oid ci = BATcount( c->column_name );	
	char *typename = BUNtail(c->type_db,BUNfnd(c->type_db, 
			BUNtail(type_sqlr, BUNfnd(type_sqlr, sqltype))));
	int type = ATOMindex(typename);
	BAT *b = BATnew( TYPE_void, type, 1000 );
	BAT *r = BATmirror(c->table_id);
	oid ltid = *(oid*)BUNtail(r, BUNfnd(r, (ptr)&tid));
	char *tname = BUNtail(c->table_name, BUNfnd(c->table_name, (ptr)&ltid));

	BATseqbase(b,0);
	if (c->debug) 
		printf("mvc_create_column %d %d %s %s  %d\n", 
					cid, tid, name, sqltype, seqnr);

	BATmode(b, PERSISTENT);

	/*snprintf(buf, BUFSIZ, "sql_%ld_%s_%ld", tid, name, cid );*/
	snprintf(buf, BUFSIZ, "sql_%s_%s", tname, name );
	BATrename(b, buf);

	BUNins(c->column_id, 	(ptr)&ci, (ptr)&cid );
	BUNins(c->column_table, 	(ptr)&ci, (ptr)&tid );
	BUNins(c->column_name, 	(ptr)&ci, (ptr)name );
	BUNins(c->column_type, 	(ptr)&ci, (ptr)sqltype );
	BUNins(c->column_default, 	(ptr)&ci, (ptr)"NULL" );
	BUNins(c->column_null, 	(ptr)&ci, (ptr)&one );
	BUNins(c->column_number, 	(ptr)&ci, (ptr)&seqnr );
	BUNins(c->column_bat, 	(ptr)&ci, (ptr)&b->batCacheid );
	mvc_bind_intern( c, cid, SQL_WRITE );

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

char *next_single_sep( char *s, char sep ){
	int escaped;

	s++; /* skip sep */
        escaped	= (*s == '\\');
	while(*s){
	       	if (*s == sep && !escaped)
		       return s+1;
		else
			escaped = (*s == '\\' && !escaped);
		s++;
	}
	return s;
}

char *next_comma( char *s ){
	int escaped = (*s == '\\');

	if (!escaped && (*s == '"')) return next_single_sep (s, '\"');
	if (!escaped && (*s == '\'')) return next_single_sep (s, '\'');
	if (!escaped && (*s == '\1')) return next_single_sep (s, '\1');

	while(*s != ',') s++;
	return s;
}


void mvc_fast_insert( mvc *c, char *insert_string ){
	char *next = insert_string + 2; /* skip 0, */
	int tid = strtol( next, &next, 10);
	BAT *columns = BATselect(c->column_table, (ptr)&tid, (ptr)&tid);
	BUN p,q;
	oid nil = oid_nil;

	q = BUNlast(columns);
	p = BUNfirst(columns);
	if ( p < q ){ 
		oid newid = OIDnew(1);
		oid lcid = *(oid*)BUNhead(columns,p);
		oid thiscid = *(oid*)BUNtail(c->column_id,
			BUNfnd(c->column_id, (ptr)&lcid));
		BAT *b = mvc_bind_intern( c, thiscid, SQL_WRITE );
		BUNins(b,  (ptr)&nil, (ptr)&newid );

		p = BUNnext(columns, p);
	}

	next++; /* skip comma */
	for(; p < q; p = BUNnext(columns, p) ){
		oid lcid = *(oid*)BUNhead(columns,p);
		oid thiscid = *(oid*)BUNtail(c->column_id,
			BUNfnd(c->column_id, (ptr)&lcid));
		BAT *b = mvc_bind_intern( c, thiscid, SQL_WRITE );

		char *e = next_comma(next); 
		ptr *a;
		*e = '\0';

        	a = ADTfromStr( b->dims.tailtype, next );
		BUNins(b,  (ptr)&nil, a );
		GDKfree( a );
		next = e+1;
	}
	BBPunfix(columns->batCacheid);
}

void mvc_delete( mvc *c, oid tid, BAT *rids ){
	BAT *columns = BATselect(c->column_table, (ptr)&tid, (ptr)&tid);
	BUN p,q;
	oid cid = *(oid*)BUNhead(columns,BUNfirst(columns));
	BAT *b = BATdescriptor(*(bat*)BUNtail(c->column_bat,
				BUNfnd(c->column_bat, (ptr)&cid)));
	int first =  BUNindex(b,BUNfirst(b));
	signed long base = b->hseqbase-first;

	if (!BATcount(rids))
		return;

	rids = BATsort(rids); /* we need sorted oids */
	
	assert(BAThdense(b));

	/* bats should be void, ie. first translate the rids to 
	 * positions then use deletes of positions 
	 */
	/*
	printf("mvc_delete_bat %ld \n", tid);
	BATprint(rids);
	*/


	BATloop(columns, p, q ){
		BUN r,s;
		int sz;
		oid lcid = *(oid*)BUNhead(columns,p);
		oid thiscid = *(oid*)BUNtail(c->column_id,
				BUNfnd(c->column_id, (ptr)&lcid));

		b = mvc_bind_intern( c, thiscid, SQL_WRITE );

		/* reverse order scan, since the deleted slots are
		 * filled with stuff from the top, else the oid is
		 * already gone
		 */
        	sz = BUNsize(rids);
        	for(r=BUNlast(rids)-sz, s=BUNfirst(rids); r >= s; r -=sz) {
			oid rid = *(oid*)BUNhead(rids,r);
			BUNdelete(b,  BUNptr(b, rid-base));
		}
	}	
	BBPunfix(rids->batCacheid);
	BBPunfix(columns->batCacheid);
}

void mvc_update( mvc *c, oid tid, oid cid, BAT *v ){
	BAT *columns = BATselect(c->column_table, (ptr)&tid, (ptr)&tid);
	BUN p,q;
	
	oid fcid = *(oid*)BUNhead(columns,BUNfirst(columns));
	BAT *b = BATdescriptor(*(bat*)BUNtail(c->column_bat,
				BUNfnd(c->column_bat, (ptr)&fcid)));
	int first =  BUNindex(b,BUNfirst(b));
	signed long base = b->hseqbase-first;

	if (!BATcount(v))
		return;

	v = BATsort(v); /* we need sorted oids */

	assert(BAThdense(b));
	
	BATloop(columns, p, q ){
		BUN r,s;
		int sz;

		oid nil = oid_nil;
		BAT *u = v; 
		oid lcid = *(oid*)BUNhead(columns,p);

		oid thiscid = *(oid*)BUNtail(c->column_id,
				BUNfnd(c->column_id, (ptr)&lcid));
		b = mvc_bind_intern( c, thiscid, SQL_WRITE );

		if (thiscid != cid){
			/* may need own BATsemijoin or use one which
			 * keeps the order, aka fetchjoin
			 */
			u = BATsemijoin(b,v);
		}
		/* reverse order scan, since the deleted slots are
		 * filled with stuff from the top, else the oid is
		 * already gone
		 */
        	sz = BUNsize(u);
        	for(r=BUNlast(u)-sz, s=BUNfirst(u); r >= s; r -=sz) {
			oid rid = *(oid*)BUNhead(u,r);
			BUNdelete(b,  BUNptr(b, rid-base));
		}
		BATloop(u, r, s){
			oid rid = *(oid*)BUNhead(u,r);
			BUNins(b,  (ptr)&nil, BUNtail(u,r));
		}
	}	
	BBPunfix(v->batCacheid);
	BBPunfix(columns->batCacheid);
}
