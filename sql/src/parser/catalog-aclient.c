
#include "mem.h"
#include "catalog.h"
#include "statement.h"
#include "sql.h"

#include <stream.h>
#include <string.h>

extern statement *sqlexecute( context *lc, char *buf );

/*
schema *catalog_schema_create( catalog *cat, char *name, char *user, char *auth);
*/

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

catalog *catalog_create_stream( stream *s, context *lc ){
	catalog *c = lc->cat;
	int i, tcnt;

	tcnt = readnr(s);
	c->types = list_create();
	for(i=0;i<tcnt;i++){
	    char buf[BUFSIZ+1];
	    char *start = buf, *n = readline(s, buf);
	    char *sqlname, *monetname, *cast;

	    n = strchr(start, '\t'); *n = '\0';
	    sqlname = removeQuotes(start, '"');

	    n = strchr(start = n+1, '\t'); *n = '\0';
	    monetname = removeQuotes(start, '"');

	    n = strchr(start = n+1, '\0'); *n = '\0';
	    cast = removeQuotes(start, '"');

	    c->create_type( c, sqlname, monetname, cast, i );
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

	/* bats are void-aligned */
	tcnt = readnr(s);
	c->tables = list_create();
	for(i=0;i<tcnt;i++){
	    char *tname, *query;
	    int temp;
	    char buf[BUFSIZ+1];
	    char *start = buf, *n = readline(s, buf);

	    n = strchr(start, '\t'); *n = '\0';
	    tname = removeQuotes(start, '"');

	    n = strchr(start = n+1, '\t'); *n = '\0';
	    temp = atoi(start);

	    n = strchr(start = n+1, '\0'); *n = '\0';
	    query = removeQuotes(start, '"');

	    if (strlen(query)){
	      sqlexecute(lc, query );
	    } else {
	      (void)c->create_table( c, tname, temp );
	    }
	}

	tcnt = readnr(s);
	for(i=0;i<tcnt;i++){
	    char buf[BUFSIZ+1];
	    char *start = buf, *n = readline(s, buf);
	    char *tname, *cname, *ctype, *def;
	    int nll, seqnr;
	    table *t;

	    n = strchr(start, '\t'); *n = '\0';
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

	    t = c->bind_table(c, tname);
	    c->create_column( c, t, cname, ctype, def, nll, seqnr );
	}
	/* loop over all schema's call create_schema */
	/* loop over all table's call create_table */
	/* loop over all column's call create_column */

	return c;
}


