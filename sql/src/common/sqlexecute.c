
#include <mem.h>
#include <ctype.h>
#include <stdlib.h>

#include "symbol.h"
#include "sqlexecute.h"
#include "sqlscan.h"
#include "symbol.h"
#include "optimize.h"
#include <string.h>
#include <stream.h>
#include <statement.h>

extern int sqlparse(void *);

void sql_init_context(context * lc, stream * out, int debug, catalog * cat)
{
	memset(lc, 0, sizeof(context));
	lc->cur = ' ';
	lc->filename = NULL;
	lc->buf = NULL;
	lc->in = NULL;
	lc->out = out;
	lc->debug = debug;
	lc->lineno = 1;
	lc->sym = NULL;
	lc->cat = cat;
	lc->status = 0;
	lc->errstr[0] = '\0';

	lc->yyval = 0;
	lc->yytext = NEW_ARRAY(char, BUFSIZ);
	lc->yytext[0] = 0;
	lc->yylen = 0;
	lc->yysize = BUFSIZ;

	if (lc->sql)
		_DELETE(lc->sql);
	lc->sql = NEW_ARRAY(char, BUFSIZ);
	lc->sqlsize = BUFSIZ;
	lc->sql[0] = '\0';

	sql_statement_init(lc);
}

void sql_exit_context(context * lc)
{
	lc->out->close(lc->out);
	lc->out->destroy(lc->out);
	catalog_destroy(lc->cat);
	lc->cat = NULL;

	if (lc->sql != NULL)
		_DELETE(lc->sql);
	lc->sql = NULL;
	_DELETE(lc->yytext);
}

stmt *sqlnext(context * lc, stream * in, int *err)
{
	stmt *res = NULL;

	lc->filename = in->name;
	lc->in = in;

	sql_statement_init(lc);

	if (lc->cur != EOF && !(*err = sqlparse(lc))) {
		res = semantic(lc, lc->sym);
		if (res){
			stmt *opt = optimize(lc, res);
			stmt_destroy(res);
			res = opt;
		}
		if (!res && lc->status){
			*err = 1;
		}
	}
	if (lc->sym) {
		symbol_destroy(lc->sym);
		lc->sym = NULL;
	}
	return res;
}

stmt *sqlexecute(context * lc, char *buf)
{
	stmt *res = NULL;

	lc->filename = "<stdin>";
	lc->buf = buf;
	lc->cur = ' ';

	sql_statement_init(lc);

	if (!sqlparse(lc)) {
		res = semantic(lc, lc->sym);
		if (res)
			res = optimize(lc, res);
	} else {
		/* errors should be handled in upper layer
		 * not directly printed
		fprintf(stderr, "%s\n", lc->errstr);
		*/
	}
	if (lc->sym) {
		symbol_destroy(lc->sym);
		lc->sym = NULL;
	}
	return res;
}

void sql_statement_init(context * lc)
{
	if (lc->sql && lc->debug & D__SQL)
		fprintf(stderr, "%s\n", lc->sql);
	lc->sql[0] = '\0';
	lc->sqllen = 1;

	lc->errstr[0] = '\0';
}
