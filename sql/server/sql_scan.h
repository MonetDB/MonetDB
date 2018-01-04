/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0.  If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 *
 * Copyright 1997 - July 2008 CWI, August 2008 - 2018 MonetDB B.V.
 */

#ifndef _SQL_SCAN_H_
#define _SQL_SCAN_H_

#include "sql_mem.h"
#include "sql_list.h"
#include "stream.h"
#include <stdio.h>

typedef enum { LINE_1, LINE_N } prot;

struct scanner {
	bstream *rs;
	stream *ws;
	stream *log;

	int yynext;		/* next token, lr(1) isn't powerful enough for sql */
	int yylast;		/* previous token, to detect superfluous semi-colons */
	int yysval;		/* start of current token */
	int yyval;		/* current token */
	int yycur;		/* next char in the queue */
	char yybak;		/* sometimes it's needed to write an EOS marker */
	int as;			/* start of query part of view's etc */
	int key;		/* query hash */
	int started;		/* found at least one token */
	prot mode;		/* which mode (line (1,N), blocked) */
	char *schema;		/* Keep schema name of create statement, 
				   needed AUTO_INCREMENT, SERIAL */
	char *errstr;		/* error message from the bowels of
				 * the scanner */
};

#define QUERY(scanner) (scanner.rs->buf+scanner.rs->pos)

extern char *query_cleaned(const char *query);
extern void scanner_init(struct scanner *s, bstream *rs, stream *ws);
extern void scanner_reset_key(struct scanner *s);
extern void scanner_query_processed(struct scanner *s);

extern int scanner_init_keywords(void);
#endif /* _SQL_SCAN_H_ */

