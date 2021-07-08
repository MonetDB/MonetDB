/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0.  If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 *
 * Copyright 1997 - July 2008 CWI, August 2008 - 2021 MonetDB B.V.
 */

#ifndef _SQL_SCAN_H_
#define _SQL_SCAN_H_

#include "sql_mem.h"
#include "sql_list.h"
#include "stream.h"

typedef enum { LINE_1, LINE_N } prot;

struct scanner {
	bstream *rs;
	stream *ws;
	stream *log;

	int yynext;		/* next token, lr(1) isn't powerful enough for sql */
	int yylast;		/* previous token, to detect superfluous semi-colons */
	int yyval;		/* current token */
	size_t yysval;		/* start of current token */
	size_t yycur;		/* next char in the queue */
	size_t as;		/* start of query part of view's etc */
	char yybak;		/* sometimes it's needed to write an EOS marker */
	int started;	/* found at least one token */
	prot mode;		/* which mode (line (1,N), blocked) */
	char *schema;	/* Keep schema name of create statement, needed AUTO_INCREMENT, SERIAL */
	char *errstr;	/* error message from the bowels of the scanner */


	/* Currently, MonetDB interprets \ specially in strings.  This is contrary
	 * to the SQL standard. The default behavior of the reader is controlled by
	 * ``raw_strings`` mserver5 option (``--set raw_strings=<true|false>``) and
	 * the database property of the same name of merovingian (``monetdb set
	 * raw_strings=yes <database>``). If it is ``true`` by default the reader
	 * interprets strings as specified in the SQL standard, i.e. no
	 * interpretation of the \ characters. If it is ``false`` \ characters are
	 * used for escaping.
	 *
	 * The default value of this setting is false as of the Jul2021 release,
	 * i.e. MonetDB by default interprets \ in strings.
	 *
	 * In case that we are interpreting \ in strings, we need state in the
	 * scanner so that we Do The Right Thing (TM) when we get a unicode string
	 * split up in multiple parts (i.e. U&'string1' 'string2') where the second
	 * and subsequent string MUST NOT undergo \ interpretation.
	 *
	 * Because we need to be backward compatible (i.e. we need have ``select
	 * char_length('a\nb')`` return 3) the next 2 members will probably stay for
	 * the forseeable future.
	 */
	bool next_string_is_raw;
	bool raw_string_mode;
};

#define QUERY(scanner) (scanner.rs->buf+scanner.rs->pos)

extern char *query_cleaned(sql_allocator *sa, const char *query);
extern void scanner_init(struct scanner *s, bstream *rs, stream *ws);
sql_export void scanner_query_processed(struct scanner *s);

extern int scanner_init_keywords(void);
#endif /* _SQL_SCAN_H_ */
