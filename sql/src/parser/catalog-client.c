
#include "catalog.h"
#include "statement.h"
#include "mem.h"

#include <comm.h>
#include <string.h>

typedef struct cc {
	stream *in;
	stream *out;
	context *lc;
} cc;

extern statement *sqlexecute( context *lc, char *buf );

static char *readline( stream *rs, char *buf ){ 
	char *start = buf;
	while(rs->read(rs, start, 1, 1)){
		if (*start == '\n' ){
			break;
		}
		start ++;
	}
	*start = '\0';
	return start;
}

static int readnr( stream *rs ){
	char buf[BUFSIZ];

	readline( rs, buf );
	return atoi(buf);
}

static long oidrange( struct catalog *cat, int nr ){
	cc *i = (cc*)cat->cc;
	char buf[BUFSIZ], *e = NULL;
	sprintf(buf, "oidrange(Output, %d);\n\001", nr ); 
	i->out->write(i->out, buf, strlen(buf), 1);
	i->out->flush(i->out);
	e = readline( i->in, buf );
	if (e) *e = '\0';
	return strtol(buf, (char **)NULL, 10);
}

static void send_getfunctions( catalog *cat ){
	char buf[BUFSIZ];
	cc *i = (cc*)cat->cc;

	sprintf(buf, "ascii_export_functions(Output);\n\001" ); 
	i->out->write(i->out, buf, strlen(buf), 1);
	i->out->flush(i->out);
}
static void send_getschema( catalog *cat, char *schema ){
	char buf[BUFSIZ];
	cc *i = (cc*)cat->cc;

	sprintf(buf, "ascii_export_schema(Output, \"%s\");\n\001", schema ); 
	i->out->write(i->out, buf, strlen(buf), 1);
	i->out->flush(i->out);
}


void getfunctions( catalog *c ){
	stream *s = ((cc*)c->cc)->in;
	int i, tcnt;

	send_getfunctions( c );

	tcnt = readnr(s);
	printf("types %d\n", tcnt);
	c->types = list_create();
	for(i=0;i<tcnt;i++){
	    char buf[BUFSIZ+1];
	    char *start = buf, *n = readline(s, buf);
	    char *sqlname, *name, *cast;

	    n = strchr(start, '\t'); *n = '\0';
	    sqlname = removeQuotes(start, '"');

	    n = strchr(start = n+1, '\t'); *n = '\0';
	    name = removeQuotes(start, '"');

	    n = strchr(start = n+1, '\0'); *n = '\0';
	    cast = removeQuotes(start, '"');

	    printf("%s\n", sqlname );
	    c->create_type( c, sqlname, name, cast, i );
	}
	/* TODO load proper type cast table */

	tcnt = readnr(s);
	c->aggrs = list_create();
	for(i=0;i<tcnt;i++){
	    char *tname;
	    char buf[BUFSIZ+1];
	    char *start = buf, *n = readline(s, buf);

	    n = strchr(start, '\0'); *n = '\0';
	    tname = removeQuotes(start, '"');

	    c->create_aggr( c, tname, i );
	}

	tcnt = readnr(s);
	c->funcs = list_create();
	for(i=0;i<tcnt;i++){
	    char *tname;
	    char buf[BUFSIZ+1];
	    char *start = buf, *n = readline(s, buf);

	    n = strchr(start, '\0'); *n = '\0';
	    tname = removeQuotes(start, '"');

	    c->create_func( c, tname, i );
	}
}

void getschema( catalog *c, char *schema, char *user ){
	stream *s = ((cc*)c->cc)->in;
	context *lc = ((cc*)c->cc)->lc;
	int i, tcnt, schema_id;

	if (c->cur_schema) c->destroy_schema( c );

	send_getschema(c, schema);
	/* bats are void-aligned */
	schema_id = readnr(s);
	c->schemas = list_create();
	c->cur_schema = c->create_schema( c, schema_id, schema, user );
	list_append_string( c->schemas, (char*) c->cur_schema );

	tcnt = readnr(s);
	printf("tables %d\n", tcnt );
	for(i=0;i<tcnt;i++){
	    long id;
	    char *tname;
	    int temp;
	    char buf[BUFSIZ+1];
	    char *start = buf, *n = readline(s, buf);

	    n = strchr(start, '\t'); *n = '\0';
	    id = strtol(start, (char**)NULL, 10);

	    n = strchr(start = n+1, '\t'); *n = '\0';
	    tname = removeQuotes(start, '"');

	    n = strchr(start = n+1, '\0'); *n = '\0';
	    temp = atoi(start);

	    (void)c->create_table( c, id, c->cur_schema, tname, temp, NULL );
	}

	tcnt = readnr(s);
	printf("columns %d\n", tcnt );
	for(i=0;i<tcnt;i++){
            long id = 0;
	    char buf[BUFSIZ+1];
	    char *start = buf, *n = readline(s, buf);
	    char *tname, *cname, *ctype, *def;
	    int nll, seqnr;
	    table *t;

	    n = strchr(start, '\t'); *n = '\0';
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

	    n = strchr(start = n+1, '\0'); *n = '\0';
	    seqnr = atoi(start);

	    t = c->bind_table(c, c->cur_schema, tname);
	    c->create_column( c, id, t, cname, ctype, def, nll, seqnr );
	}
	/* loop over all schema's call create_schema */
	/* loop over all table's call create_table */
	/* loop over all column's call create_column */

	/* bats are void-aligned */
	/* read views */
	tcnt = readnr(s);
	printf("views %d\n", tcnt );
	for(i=0;i<tcnt;i++){
            long id = 0;
	    char *tname, *query;
	    char buf[BUFSIZ+1];
	    char *start = buf, *n = readline(s, buf);

	    n = strchr(start, '\t'); *n = '\0';
	    id = strtol(start, (char**)NULL, 10);

	    n = strchr(start = n+1, '\t'); *n = '\0';
	    tname = removeQuotes(start, '"');

	    n = strchr(start = n+1, '\0'); *n = '\0';
	    query = removeQuotes(start, '"');

	    sqlexecute(lc, query );
	}
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


