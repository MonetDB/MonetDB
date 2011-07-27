/*
 * The contents of this file are subject to the MonetDB Public License
 * Version 1.1 (the "License"); you may not use this file except in
 * compliance with the License. You may obtain a copy of the License at
 * http://www.monetdb.org/Legal/MonetDBLicense
 *
 * Software distributed under the License is distributed on an "AS IS"
 * basis, WITHOUT WARRANTY OF ANY KIND, either express or implied. See the
 * License for the specific language governing rights and limitations
 * under the License.
 *
 * The Original Code is the MonetDB Database System.
 *
 * The Initial Developer of the Original Code is CWI.
 * Portions created by CWI are Copyright (C) 1997-July 2008 CWI.
 * Copyright August 2008-2011 MonetDB B.V.
 * All Rights Reserved.
 */


#ifndef _SQL_SCAN_H_
#define _SQL_SCAN_H_

#include "sql_mem.h"
#include "sql_list.h"
#include <stream.h>
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
	char yybak;		/* sometimes its needed to write an EOS marker */
	int as;			/* start of query part of view's etc */
	int key;		/* query hash */
	int started;		/* found at least one token */
	prot mode;		/* which mode (line (1,N), blocked) */
	char *schema;		/* Keep schema name of create statement, 
				   needed AUTO_INCREMENT, SERIAL */
};

#define QUERY(scanner) (scanner.rs->buf+scanner.rs->pos)

extern void scanner_init(struct scanner *s, bstream *rs, stream *ws);
extern void scanner_query_processed(struct scanner *s);

extern void scanner_init_keywords(void);
#endif /* _SQL_SCAN_H_ */

