#ifndef _CONTEXT_H_
#define _CONTEXT_H_

#include "catalog.h"
#include <stream.h>

#define ERRSIZE 1024

#define SQL_FAST_INSERT 1

typedef struct context {
	int cur;

	int yyval;
	char *yytext;
	int yylen;
	int yysize;
	int debug;
	int optimize;
	
	char *sql;
	int sqllen;
	int sqlsize;

	int lineno;
	char *filename;
	char *buf;
	stream *in;
	stream *out;

	char errstr[ERRSIZE];
	struct catalog *cat;

	struct symbol *sym;
} context;

#endif /* _CONTEXT_H_ */
