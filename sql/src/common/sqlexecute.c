
#include <mem.h>
#include <ctype.h>
#include <stdlib.h>

#include "symbol.h"
#include "sqlexecute.h"
#include "sqlscan.h"
#include <string.h>
#include <stream.h>
#include <statement.h>

extern int sqlparse (void *);

void sql_init_context( context *lc , stream *out, int debug, catalog *cat ){
	init_keywords();

	memset(lc,0,sizeof(context));

        lc->cur = ' ';
        lc->filename = NULL;
        lc->buf = NULL;
        lc->in = NULL;
        lc->out = out;
        lc->debug = debug;
        lc->optimize = SQL_FAST_INSERT;
        lc->lineno = 1;
        lc->l = NULL;
        lc->cat = cat;
        lc->errstr[0] = '\0';

	lc->yyval = 0;
	lc->yytext = NEW_ARRAY(char, BUFSIZ);
	lc->yylen = 0;
	lc->yysize = BUFSIZ;

	if (lc->sql)
		_DELETE(lc->sql);
	lc->sql = NEW_ARRAY(char, BUFSIZ);
	lc->sqlsize = BUFSIZ;

        sql_statement_init(lc);
}

void sql_exit_context( context *lc ){
	lc->out->close( lc->out );
	lc->out->destroy( lc->out );
	catalog_destroy( lc->cat );
	exit_keywords();

	if (lc->sql != NULL)
		_DELETE(lc->sql);
	lc->sql = NULL;
	_DELETE(lc->yytext);
}


int sql_execute( context *lc, stream *in ){

	lc->filename = _strdup(in->filename);
	lc->in = in;

	if(sqlparse(lc)){
		fprintf(stderr, "%s\n", lc->errstr );
		fprintf(stderr, "Embedded SQL parse failed\n");
		return -1;
        } else {
                list *sl = semantic(lc, lc->l);
		dlist_destroy(lc->l); lc->l = NULL;
                if (sl){
                        node *n = sl->h;
                        while(n){
                        	int nr = 1;
                                statement_dump( n->data.stval, &nr, lc );
                                n = n -> next;
                        }
			lc->out->flush( lc->out );
			list_destroy(sl);
			fprintf(stderr, "Embedded SQL parse worked\n");
                } else {
			fprintf(stderr, "Embedded SQL parse failed\n");
			fflush(stderr);
			return -1;
                }
        }
	return 0;
} 

statement *sqlexecute( context *lc, char *buf ){
        statement *res = NULL;

        lc->filename = "<stdin>";
        lc->buf = buf;
        lc->cur = ' ';

	sql_statement_init(lc);

	if (lc->l){
		dlist_destroy(lc->l);
		lc->l = NULL; 
	}

        if(!sqlparse(lc )){
                list *l = semantic(lc, lc->l);
		dlist_destroy(lc->l); lc->l = NULL;
                if (l){ 
			if (list_length(l)){
				res = l->h->data.stval; res->refcnt++;
			}
			list_destroy(l);
		}
        } else {
		fprintf(stderr, "%s\n", lc->errstr );
	}

        return res;
}

void sql_statement_init(context *lc){
	if (lc->sql && lc->debug)
		fprintf(stderr, "%s\n", lc->sql );
	lc->sql[0] = '\0';
	lc->sqllen = 1;
}


