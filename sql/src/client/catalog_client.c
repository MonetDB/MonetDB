
#include "catalog.h"
#include "statement.h"
#include "mem.h"
#include "sql.h"

#include "comm.h"
#include <string.h>

typedef struct cc {
	stream *in;
	stream *out;
	context *lc;
} cc;

extern statement *sqlexecute( context *lc, char *buf );

static char *readblock( stream *s ){
	int len = 0;
	int size = BUFSIZ + 1;
	char *buf = NEW_ARRAY(char, size ), *start = buf;

	while ((len = s->read(s, start, 1, BUFSIZ)) == BUFSIZ){
		size += BUFSIZ;
		buf = RENEW_ARRAY(char, buf, size); 
		start+= BUFSIZ;
		*start = '\0';
	}
	start += len;
	*start = '\0';
	return buf;
}

static long oidrange( struct catalog *cat, int nr ){
	cc *i = (cc*)cat->cc;
	char buf[BUFSIZ], *e = NULL;
	long res;
	sprintf(buf, "oidrange(Output, %d);\n", nr ); 
	i->out->write(i->out, buf, strlen(buf), 1);
	i->out->flush(i->out);
	e = readblock( i->in );
	res  = strtol(e, (char **)NULL, 10);
	_DELETE(e);
	return res;
}

static void send_getfunctions( catalog *cat ){
	char buf[BUFSIZ];
	cc *i = (cc*)cat->cc;

	sprintf(buf, "ascii_export_functions(Output);\n" ); 
	i->out->write(i->out, buf, strlen(buf), 1);
	i->out->flush(i->out);
}
static void send_getschema( catalog *cat, char *schema ){
	char buf[BUFSIZ];
	cc *i = (cc*)cat->cc;

	sprintf(buf, "ascii_export_schema(Output, \"%s\");\n", schema ); 
	i->out->write(i->out, buf, strlen(buf), 1);
	i->out->flush(i->out);
}


void getfunctions( catalog *c ){
	stream *s = ((cc*)c->cc)->in;
	int i, tcnt;
	char *buf, *start, *n;

	send_getfunctions( c );

	buf = readblock(s);
	n = start = buf;

	tcnt = strtol(n,&n,10); 
	printf("types %d\n", tcnt );
	c->types = list_create();
	for(i=0;i<tcnt;i++){
	    char *sqlname, *name, *cast;

	    n = strchr(start = n+1, '\t'); *n = '\0';
	    sqlname = removeQuotes(start, '"');

	    n = strchr(start = n+1, '\t'); *n = '\0';
	    name = removeQuotes(start, '"');

	    n = strchr(start = n+1, '\n'); *n = '\0';
	    cast = removeQuotes(start, '"');

	    cat_create_type( c, sqlname, name, cast, i );
	}
	/* TODO load proper type cast table */

	tcnt = strtol(n+1,&n,10); 
	printf("aggr %d\n", tcnt );
	c->aggrs = list_create();
	for(i=0;i<tcnt;i++){
	    char *tname, *imp, *tpe, *res;

	    n = strchr(start = n+1, '\t'); *n = '\0';
	    tname = removeQuotes(start, '"');

	    n = strchr(start = n+1, '\t'); *n = '\0';
	    imp = removeQuotes(start, '"');

	    n = strchr(start = n+1, '\t'); *n = '\0';
	    tpe = removeQuotes(start, '"');

	    n = strchr(start = n+1, '\n'); *n = '\0';
	    res = removeQuotes(start, '"');

	    cat_create_aggr( c, tname, imp, tpe, res, i );
	}

	tcnt = strtol(n+1,&n,10); 
	c->funcs = list_create();
	for(i=0;i<tcnt;i++){
	    char *tname, *imp;

	    n = strchr(start = n+1, '\t'); *n = '\0';
	    tname = removeQuotes(start, '"');

	    n = strchr(start = n+1, '\n'); *n = '\0';
	    imp = removeQuotes(start, '"');

	    cat_create_func( c, tname, imp, i );
	}
	_DELETE(buf);
}

void getschema( catalog *c, char *schema, char *user ){
	stream *s = ((cc*)c->cc)->in;
	context *lc = ((cc*)c->cc)->lc;
	int i, tcnt, schema_id;
	char *buf, *start, *n;

	if (c->cur_schema) cat_destroy_schema( c );

	send_getschema(c, schema);

	buf = readblock(s);
	n = start = buf;

	/* bats are void-aligned */
	schema_id = strtol(n,&n,10);
	c->schemas = list_create();
	c->cur_schema = cat_create_schema( c, schema_id, schema, user );
	list_append_string( c->schemas, (char*) c->cur_schema );

	tcnt = strtol(n+1,&n,10); 
	printf("tables %d\n", tcnt );
	for(i=0;i<tcnt;i++){
	    long id;
	    char *tname;
	    int temp;

	    n = strchr(start = n+1, '\t'); *n = '\0';
	    id = strtol(start, (char**)NULL, 10);

	    n = strchr(start = n+1, '\t'); *n = '\0';
	    tname = removeQuotes(start, '"');

	    n = strchr(start = n+1, '\n'); *n = '\0';
	    temp = atoi(start);

	    (void)cat_create_table( c, id, c->cur_schema, tname, temp, NULL );
	}

	tcnt = strtol(n+1,&n,10); 
	printf("columns %d\n", tcnt );
	for(i=0;i<tcnt;i++){
            long id = 0;
	    char *tname, *cname, *ctype, *def;
	    int nll, seqnr;
	    table *t;

	    n = strchr(start = n+1, '\t'); *n = '\0';
	    id = strtol(start, (char**)NULL, 10);

	    n = strchr(start = n+1, '\t'); *n = '\0';
	    tname = removeQuotes(start, '"');

	    n = strchr(start = n+1, '\t'); *n = '\0';
	    cname = removeQuotes(start, '"');

	    n = strchr(start = n+1, '\t'); *n = '\0';
	    ctype = removeQuotes(start, '"');

	    n = strchr(start = n+1, '\t'); *n = '\0';
	    def = removeQuotes(start, '"');

	    n = strchr(start = n+1, '\t'); *n = '\0';
	    nll = atoi(start);

	    n = strchr(start = n+1, '\n'); *n = '\0';
	    seqnr = atoi(start);

	    t = cat_bind_table(c, c->cur_schema, tname);
	    cat_create_column( c, id, t, cname, ctype, def, nll, seqnr );
	}
	/* loop over all schema's call create_schema */
	/* loop over all table's call create_table */
	/* loop over all column's call create_column */

	/* bats are void-aligned */
	/* read views */
	tcnt = strtol(n+1,&n,10); 
	printf("views %d\n", tcnt );
	for(i=0;i<tcnt;i++){
	    table *t;
            long id = 0;
	    char *tname, *query;

	    n = strchr(start = n+1, '\t'); *n = '\0';
	    id = strtol(start, (char**)NULL, 10);

	    n = strchr(start = n+1, '\t'); *n = '\0';
	    tname = removeQuotes(start, '"');

	    n = strchr(start = n+1, '\n'); *n = '\0';
	    query = removeQuotes(start, '"');

	    sqlexecute(lc, query );
	    /* fix id */
	    t = cat_bind_table( c, c->cur_schema, tname );
	    t->id = id;
	}
	_DELETE(buf);
}

static void cc_destroy( catalog *c ){
	_DELETE(c->cc);
	c->cc = NULL;
}

catalog *catalog_create_stream( stream *in, context *lc ){
	cc *CC = NEW(cc);
	catalog *c = lc->cat;
	
	CC->in = in;
	CC->out = lc->out;
	CC->lc = lc;
	c->cc = (char*)CC;
	c->cc_oidrange = &oidrange;
	c->cc_getschema = &getschema;
	c->cc_destroy = &cc_destroy;

	catalog_initoid( c );
	getfunctions( c );
	return c;
}


