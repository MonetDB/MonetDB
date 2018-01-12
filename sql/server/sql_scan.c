/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0.  If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 *
 * Copyright 1997 - July 2008 CWI, August 2008 - 2018 MonetDB B.V.
 */

#include "monetdb_config.h"
#include <wctype.h>
#include "sql_mem.h"
#include "sql_scan.h"
#include "sql_types.h"
#include "sql_symbol.h"
#include "sql_mvc.h"
#include "sql_parser.tab.h"
#include "sql_semantic.h"
#include "sql_parser.h"		/* for sql_error() */

#include "stream.h"
#include <unistd.h>
#include <string.h>
#include <ctype.h>
#include "sql_keyword.h"
#ifdef HAVE_HGE
#include "mal.h"		/* for have_hge */
#endif

char *
query_cleaned(const char *query)
{
	char *q, *r;
	int quote = 0;		/* inside quotes ('..', "..", {..}) */
	int bs = 0;		/* seen a backslash in a quoted string */
	int incomment1 = 0;	/* inside traditional C style comment */
	int incomment2 = 0;	/* inside comment starting with --  */
	r = GDKmalloc(strlen(query) + 1);
	if(!r)
		return NULL;

	for (q = r; *query; query++) {
		if (incomment1) {
			if (*query == '/' && query[-1] == '*') {
				incomment1 = 0;
			}
		} else if (incomment2) {
			if (*query == '\n') {
				incomment2 = 0;
				/* add newline only if comment doesn't
				 * occupy whole line */
				if (q > r && q[-1] != '\n')
					*q++ = '\n';
			}
		} else if (quote) {
			if (bs) {
				bs = 0;
			} else if (*query == '\\') {
				bs = 1;
			} else if (*query == quote) {
				quote = 0;
			}
			*q++ = *query;
		} else if (*query == '"' || *query == '\'') {
			quote = *query;
			*q++ = *query;
		} else if (*query == '{') {
			quote = '}';
			*q++ = *query;
		} else if (*query == '-' && query[1] == '-') {
			incomment2 = 1;
		} else if (*query == '/' && query[1] == '*') {
			incomment1 = 1;
		} else if (*query == '\n') {
			/* collapse newlines */
			if (q > r && q[-1] != '\n')
				*q++ = '\n';
		} else if (*query == ' ' || *query == '\t') {
			/* collapse white space */
			if (q > r && q[-1] != ' ')
				*q++ = ' ';
		} else {
			*q++ = *query;
		}
	}
	*q = 0;
	return r;
}

int
scanner_init_keywords(void)
{
	int failed = 0;

	failed += keywords_insert("false", BOOL_FALSE);
	failed += keywords_insert("true", BOOL_TRUE);

	failed += keywords_insert("ALTER", ALTER);
	failed += keywords_insert("ADD", ADD);
	failed += keywords_insert("AND", AND);
	failed += keywords_insert("MEDIAN", AGGR);
	failed += keywords_insert("CORR", AGGR2);
	failed += keywords_insert("QUANTILE", AGGR2);
	failed += keywords_insert("AVG", AGGR);
	failed += keywords_insert("MIN", AGGR);
	failed += keywords_insert("MAX", AGGR);
	failed += keywords_insert("SUM", AGGR);
	failed += keywords_insert("PROD", AGGR);
	failed += keywords_insert("COUNT", AGGR);

	failed += keywords_insert("LAG", AGGR);
	failed += keywords_insert("LEAD", AGGR);
	failed += keywords_insert("LAG", AGGR2);
	failed += keywords_insert("LEAD", AGGR2);

	failed += keywords_insert("RANK", RANK);
	failed += keywords_insert("DENSE_RANK", RANK);
	failed += keywords_insert("PERCENT_RANK", RANK);
	failed += keywords_insert("CUME_DIST", RANK);
	failed += keywords_insert("ROW_NUMBER", RANK);
	failed += keywords_insert("BEST", BEST);
	failed += keywords_insert("EFFORT", EFFORT);

	failed += keywords_insert("AS", AS);
	failed += keywords_insert("ASC", ASC);
	failed += keywords_insert("AUTHORIZATION", AUTHORIZATION);
	failed += keywords_insert("BETWEEN", BETWEEN);
	failed += keywords_insert("SYMMETRIC", SYMMETRIC);
	failed += keywords_insert("ASYMMETRIC", ASYMMETRIC);
	failed += keywords_insert("BY", BY);
	failed += keywords_insert("CAST", CAST);
	failed += keywords_insert("CONVERT", CONVERT);
	failed += keywords_insert("CHARACTER", CHARACTER);
	failed += keywords_insert("CHAR", CHARACTER);
	failed += keywords_insert("VARYING", VARYING);
	failed += keywords_insert("VARCHAR", VARCHAR);
	failed += keywords_insert("BINARY", BINARY);
	failed += keywords_insert("LARGE", LARGE);
	failed += keywords_insert("OBJECT", OBJECT);
	failed += keywords_insert("CLOB", CLOB);
	failed += keywords_insert("BLOB", sqlBLOB);
	failed += keywords_insert("TEXT", sqlTEXT);
	failed += keywords_insert("TINYTEXT", sqlTEXT);
	failed += keywords_insert("STRING", CLOB);	/* ? */
	failed += keywords_insert("CHECK", CHECK);
	failed += keywords_insert("COMMENT", COMMENT);
	failed += keywords_insert("CONSTRAINT", CONSTRAINT);
	failed += keywords_insert("CREATE", CREATE);
	failed += keywords_insert("CROSS", CROSS);
	failed += keywords_insert("COPY", COPY);
	failed += keywords_insert("RECORDS", RECORDS);
	failed += keywords_insert("DELIMITERS", DELIMITERS);
	failed += keywords_insert("STDIN", STDIN);
	failed += keywords_insert("STDOUT", STDOUT);

	failed += keywords_insert("TINYINT", TINYINT);
	failed += keywords_insert("SMALLINT", SMALLINT);
	failed += keywords_insert("INTEGER", sqlINTEGER);
	failed += keywords_insert("INT", sqlINTEGER);
	failed += keywords_insert("MEDIUMINT", sqlINTEGER);
	failed += keywords_insert("BIGINT", BIGINT);
#ifdef HAVE_HGE
	if (have_hge)
		failed += keywords_insert("HUGEINT", HUGEINT);
#endif
	failed += keywords_insert("DEC", sqlDECIMAL);
	failed += keywords_insert("DECIMAL", sqlDECIMAL);
	failed += keywords_insert("NUMERIC", sqlDECIMAL);
	failed += keywords_insert("DECLARE", DECLARE);
	failed += keywords_insert("DEFAULT", DEFAULT);
	failed += keywords_insert("DESC", DESC);
	failed += keywords_insert("DISTINCT", DISTINCT);
	failed += keywords_insert("DOUBLE", sqlDOUBLE);
	failed += keywords_insert("REAL", sqlREAL);
	failed += keywords_insert("DROP", DROP);
	failed += keywords_insert("ESCAPE", ESCAPE);
	failed += keywords_insert("EXISTS", EXISTS);
	failed += keywords_insert("EXTRACT", EXTRACT);
	failed += keywords_insert("FLOAT", sqlFLOAT);
	failed += keywords_insert("FOR", FOR);
	failed += keywords_insert("FOREIGN", FOREIGN);
	failed += keywords_insert("FROM", FROM);
	failed += keywords_insert("FWF", FWF);

	failed += keywords_insert("REFERENCES", REFERENCES);

	failed += keywords_insert("MATCH", MATCH);
	failed += keywords_insert("FULL", FULL);
	failed += keywords_insert("PARTIAL", PARTIAL);
	failed += keywords_insert("SIMPLE", SIMPLE);

	failed += keywords_insert("INSERT", INSERT);
	failed += keywords_insert("UPDATE", UPDATE);
	failed += keywords_insert("DELETE", sqlDELETE);
	failed += keywords_insert("TRUNCATE", TRUNCATE);

	failed += keywords_insert("ACTION", ACTION);
	failed += keywords_insert("CASCADE", CASCADE);
	failed += keywords_insert("RESTRICT", RESTRICT);
	failed += keywords_insert("GLOBAL", GLOBAL);
	failed += keywords_insert("GROUP", sqlGROUP);
	failed += keywords_insert("HAVING", HAVING);
	failed += keywords_insert("ILIKE", ILIKE);
	failed += keywords_insert("IMPRINTS", IMPRINTS);
	failed += keywords_insert("IN", sqlIN);
	failed += keywords_insert("INNER", INNER);
	failed += keywords_insert("INTO", INTO);
	failed += keywords_insert("IS", IS);
	failed += keywords_insert("JOIN", JOIN);
	failed += keywords_insert("KEY", KEY);
	failed += keywords_insert("LATERAL", LATERAL);
	failed += keywords_insert("LEFT", LEFT);
	failed += keywords_insert("LIKE", LIKE);
	failed += keywords_insert("LIMIT", LIMIT);
	failed += keywords_insert("SAMPLE", SAMPLE);
	failed += keywords_insert("LOCAL", LOCAL);
	failed += keywords_insert("LOCKED", LOCKED);
	failed += keywords_insert("NATURAL", NATURAL);
	failed += keywords_insert("NOT", NOT);
	failed += keywords_insert("NULL", sqlNULL);
	failed += keywords_insert("OFFSET", OFFSET);
	failed += keywords_insert("ON", ON);
	failed += keywords_insert("OPTIONS", OPTIONS);
	failed += keywords_insert("OPTION", OPTION);
	failed += keywords_insert("OR", OR);
	failed += keywords_insert("ORDER", ORDER);
	failed += keywords_insert("ORDERED", ORDERED);
	failed += keywords_insert("OUTER", OUTER);
	failed += keywords_insert("OVER", OVER);
	failed += keywords_insert("PARTITION", PARTITION);
	failed += keywords_insert("PATH", PATH);
	failed += keywords_insert("PRECISION", PRECISION);
	failed += keywords_insert("PRIMARY", PRIMARY);

	failed += keywords_insert("USER", USER);
	failed += keywords_insert("RENAME", RENAME);
	failed += keywords_insert("UNENCRYPTED", UNENCRYPTED);
	failed += keywords_insert("ENCRYPTED", ENCRYPTED);
	failed += keywords_insert("PASSWORD", PASSWORD);
	failed += keywords_insert("GRANT", GRANT);
	failed += keywords_insert("REVOKE", REVOKE);
	failed += keywords_insert("ROLE", ROLE);
	failed += keywords_insert("ADMIN", ADMIN);
	failed += keywords_insert("PRIVILEGES", PRIVILEGES);
	failed += keywords_insert("PUBLIC", PUBLIC);
	failed += keywords_insert("CURRENT_USER", CURRENT_USER);
	failed += keywords_insert("CURRENT_ROLE", CURRENT_ROLE);
	failed += keywords_insert("SESSION_USER", SESSION_USER);
	failed += keywords_insert("SESSION", sqlSESSION);

	failed += keywords_insert("RIGHT", RIGHT);
	failed += keywords_insert("SCHEMA", SCHEMA);
	failed += keywords_insert("SELECT", SELECT);
	failed += keywords_insert("SET", SET);
	failed += keywords_insert("AUTO_COMMIT", AUTO_COMMIT);

	failed += keywords_insert("ALL", ALL);
	failed += keywords_insert("ANY", ANY);
	failed += keywords_insert("SOME", SOME);
	failed += keywords_insert("EVERY", ANY);
	/*
	   failed += keywords_insert("SQLCODE", SQLCODE );
	 */
	failed += keywords_insert("COLUMN", COLUMN);
	failed += keywords_insert("TABLE", TABLE);
	failed += keywords_insert("TEMPORARY", TEMPORARY);
	failed += keywords_insert("TEMP", TEMP);
	failed += keywords_insert("STREAM", STREAM);
	failed += keywords_insert("REMOTE", REMOTE);
	failed += keywords_insert("MERGE", MERGE);
	failed += keywords_insert("REPLICA", REPLICA);
	failed += keywords_insert("TO", TO);
	failed += keywords_insert("UNION", UNION);
	failed += keywords_insert("EXCEPT", EXCEPT);
	failed += keywords_insert("INTERSECT", INTERSECT);
	failed += keywords_insert("CORRESPONDING", CORRESPONDING);
	failed += keywords_insert("UNIQUE", UNIQUE);
	failed += keywords_insert("USING", USING);
	failed += keywords_insert("VALUES", VALUES);
	failed += keywords_insert("VIEW", VIEW);
	failed += keywords_insert("WHERE", WHERE);
	failed += keywords_insert("WITH", WITH);
	failed += keywords_insert("DATA", DATA);

	failed += keywords_insert("DATE", sqlDATE);
	failed += keywords_insert("TIME", TIME);
	failed += keywords_insert("TIMESTAMP", TIMESTAMP);
	failed += keywords_insert("INTERVAL", INTERVAL);
	failed += keywords_insert("CURRENT_DATE", CURRENT_DATE);
	failed += keywords_insert("CURRENT_TIME", CURRENT_TIME);
	failed += keywords_insert("CURRENT_TIMESTAMP", CURRENT_TIMESTAMP);
	failed += keywords_insert("NOW", CURRENT_TIMESTAMP);
	failed += keywords_insert("LOCALTIME", LOCALTIME);
	failed += keywords_insert("LOCALTIMESTAMP", LOCALTIMESTAMP);
	failed += keywords_insert("ZONE", ZONE);

	failed += keywords_insert("YEAR", YEAR);
	failed += keywords_insert("QUARTER", QUARTER);
	failed += keywords_insert("MONTH", MONTH);
	failed += keywords_insert("WEEK", WEEK);
	failed += keywords_insert("DAY", DAY);
	failed += keywords_insert("HOUR", HOUR);
	failed += keywords_insert("MINUTE", MINUTE);
	failed += keywords_insert("SECOND", SECOND);

	failed += keywords_insert("POSITION", POSITION);
	failed += keywords_insert("SUBSTRING", SUBSTRING);
	failed += keywords_insert("SPLIT_PART", SPLIT_PART);

	failed += keywords_insert("CASE", CASE);
	failed += keywords_insert("WHEN", WHEN);
	failed += keywords_insert("THEN", THEN);
	failed += keywords_insert("ELSE", ELSE);
	failed += keywords_insert("END", END);
	failed += keywords_insert("NULLIF", NULLIF);
	failed += keywords_insert("COALESCE", COALESCE);
	failed += keywords_insert("ELSEIF", ELSEIF);
	failed += keywords_insert("IF", IF);
	failed += keywords_insert("WHILE", WHILE);
	failed += keywords_insert("DO", DO);

	failed += keywords_insert("COMMIT", COMMIT);
	failed += keywords_insert("ROLLBACK", ROLLBACK);
	failed += keywords_insert("SAVEPOINT", SAVEPOINT);
	failed += keywords_insert("RELEASE", RELEASE);
	failed += keywords_insert("WORK", WORK);
	failed += keywords_insert("CHAIN", CHAIN);
	failed += keywords_insert("PRESERVE", PRESERVE);
	failed += keywords_insert("ROWS", ROWS);
	failed += keywords_insert("NO", NO);
	failed += keywords_insert("START", START);
	failed += keywords_insert("TRANSACTION", TRANSACTION);
	failed += keywords_insert("READ", READ);
	failed += keywords_insert("WRITE", WRITE);
	failed += keywords_insert("ONLY", ONLY);
	failed += keywords_insert("ISOLATION", ISOLATION);
	failed += keywords_insert("LEVEL", LEVEL);
	failed += keywords_insert("UNCOMMITTED", UNCOMMITTED);
	failed += keywords_insert("COMMITTED", COMMITTED);
	failed += keywords_insert("REPEATABLE", sqlREPEATABLE);
	failed += keywords_insert("SERIALIZABLE", SERIALIZABLE);
	failed += keywords_insert("DIAGNOSTICS", DIAGNOSTICS);
	failed += keywords_insert("SIZE", sqlSIZE);
	failed += keywords_insert("STORAGE", STORAGE);

	failed += keywords_insert("TYPE", TYPE);
	failed += keywords_insert("PROCEDURE", PROCEDURE);
	failed += keywords_insert("FUNCTION", FUNCTION);
	failed += keywords_insert("LOADER", sqlLOADER);
	failed += keywords_insert("REPLACE", REPLACE);

	failed += keywords_insert("FILTER", FILTER);
	failed += keywords_insert("AGGREGATE", AGGREGATE);
	failed += keywords_insert("RETURNS", RETURNS);
	failed += keywords_insert("EXTERNAL", EXTERNAL);
	failed += keywords_insert("NAME", sqlNAME);
	failed += keywords_insert("RETURN", RETURN);
	failed += keywords_insert("CALL", CALL);
	failed += keywords_insert("LANGUAGE", LANGUAGE);

	failed += keywords_insert("ANALYZE", ANALYZE);
	failed += keywords_insert("MINMAX", MINMAX);
	failed += keywords_insert("EXPLAIN", SQL_EXPLAIN);
	failed += keywords_insert("PLAN", SQL_PLAN);
	failed += keywords_insert("DEBUG", SQL_DEBUG);
	failed += keywords_insert("TRACE", SQL_TRACE);
	failed += keywords_insert("PREPARE", PREPARE);
	failed += keywords_insert("PREP", PREP);
	failed += keywords_insert("EXECUTE", EXECUTE);
	failed += keywords_insert("EXEC", EXEC);

	failed += keywords_insert("INDEX", INDEX);

	failed += keywords_insert("SEQUENCE", SEQUENCE);
	failed += keywords_insert("RESTART", RESTART);
	failed += keywords_insert("INCREMENT", INCREMENT);
	failed += keywords_insert("MAXVALUE", MAXVALUE);
	failed += keywords_insert("MINVALUE", MINVALUE);
	failed += keywords_insert("CYCLE", CYCLE);
	failed += keywords_insert("NOMAXVALUE", NOMAXVALUE);
	failed += keywords_insert("NOMINVALUE", NOMINVALUE);
	failed += keywords_insert("NOCYCLE", NOCYCLE);
	failed += keywords_insert("CACHE", CACHE);
	failed += keywords_insert("NEXT", NEXT);
	failed += keywords_insert("VALUE", VALUE);
	failed += keywords_insert("GENERATED", GENERATED);
	failed += keywords_insert("ALWAYS", ALWAYS);
	failed += keywords_insert("IDENTITY", IDENTITY);
	failed += keywords_insert("SERIAL", SERIAL);
	failed += keywords_insert("BIGSERIAL", BIGSERIAL);
	failed += keywords_insert("AUTO_INCREMENT", AUTO_INCREMENT);
	failed += keywords_insert("CONTINUE", CONTINUE);

	failed += keywords_insert("TRIGGER", TRIGGER);
	failed += keywords_insert("ATOMIC", ATOMIC);
	failed += keywords_insert("BEGIN", BEGIN);
	failed += keywords_insert("OF", OF);
	failed += keywords_insert("BEFORE", BEFORE);
	failed += keywords_insert("AFTER", AFTER);
	failed += keywords_insert("ROW", ROW);
	failed += keywords_insert("STATEMENT", STATEMENT);
	failed += keywords_insert("NEW", sqlNEW);
	failed += keywords_insert("OLD", OLD);
	failed += keywords_insert("EACH", EACH);
	failed += keywords_insert("REFERENCING", REFERENCING);

	failed += keywords_insert("RANGE", RANGE);
	failed += keywords_insert("UNBOUNDED", UNBOUNDED);
	failed += keywords_insert("PRECEDING", PRECEDING);
	failed += keywords_insert("FOLLOWING", FOLLOWING);
	failed += keywords_insert("CURRENT", CURRENT);
	failed += keywords_insert("EXCLUDE", EXCLUDE);
	failed += keywords_insert("OTHERS", OTHERS);
	failed += keywords_insert("TIES", TIES);

	/* special SQL/XML keywords */
	failed += keywords_insert("XMLCOMMENT", XMLCOMMENT);
	failed += keywords_insert("XMLCONCAT", XMLCONCAT);
	failed += keywords_insert("XMLDOCUMENT", XMLDOCUMENT);
	failed += keywords_insert("XMLELEMENT", XMLELEMENT);
	failed += keywords_insert("XMLATTRIBUTES", XMLATTRIBUTES);
	failed += keywords_insert("XMLFOREST", XMLFOREST);
	failed += keywords_insert("XMLPARSE", XMLPARSE);
	failed += keywords_insert("STRIP", STRIP);
	failed += keywords_insert("WHITESPACE", WHITESPACE);
	failed += keywords_insert("XMLPI", XMLPI);
	failed += keywords_insert("XMLQUERY", XMLQUERY);
	failed += keywords_insert("PASSING", PASSING);
	failed += keywords_insert("XMLTEXT", XMLTEXT);
	failed += keywords_insert("NIL", NIL);
	failed += keywords_insert("REF", REF);
	failed += keywords_insert("ABSENT", ABSENT);
	failed += keywords_insert("DOCUMENT", DOCUMENT);
	failed += keywords_insert("ELEMENT", ELEMENT);
	failed += keywords_insert("CONTENT", CONTENT);
	failed += keywords_insert("XMLNAMESPACES", XMLNAMESPACES);
	failed += keywords_insert("NAMESPACE", NAMESPACE);
	failed += keywords_insert("XMLVALIDATE", XMLVALIDATE);
	failed += keywords_insert("RETURNING", RETURNING);
	failed += keywords_insert("LOCATION", LOCATION);
	failed += keywords_insert("ID", ID);
	failed += keywords_insert("ACCORDING", ACCORDING);
	failed += keywords_insert("XMLSCHEMA", XMLSCHEMA);
	failed += keywords_insert("URI", URI);
	failed += keywords_insert("XMLAGG", XMLAGG);

	/* keywords for opengis */
	failed += keywords_insert("GEOMETRY", GEOMETRY);

	failed += keywords_insert("POINT", GEOMETRYSUBTYPE);
	failed += keywords_insert("LINESTRING", GEOMETRYSUBTYPE);
	failed += keywords_insert("POLYGON", GEOMETRYSUBTYPE);
	failed += keywords_insert("MULTIPOINT", GEOMETRYSUBTYPE);
	failed += keywords_insert("MULTILINESTRING", GEOMETRYSUBTYPE);
	failed += keywords_insert("MULTIPOLYGON", GEOMETRYSUBTYPE);
	failed += keywords_insert("GEOMETRYCOLLECTION", GEOMETRYSUBTYPE);

	failed += keywords_insert("POINTZ", GEOMETRYSUBTYPE);
	failed += keywords_insert("LINESTRINGZ", GEOMETRYSUBTYPE);
	failed += keywords_insert("POLYGONZ", GEOMETRYSUBTYPE);
	failed += keywords_insert("MULTIPOINTZ", GEOMETRYSUBTYPE);
	failed += keywords_insert("MULTILINESTRINGZ", GEOMETRYSUBTYPE);
	failed += keywords_insert("MULTIPOLYGONZ", GEOMETRYSUBTYPE);
	failed += keywords_insert("GEOMETRYCOLLECTIONZ", GEOMETRYSUBTYPE);

	failed += keywords_insert("POINTM", GEOMETRYSUBTYPE);
	failed += keywords_insert("LINESTRINGM", GEOMETRYSUBTYPE);
	failed += keywords_insert("POLYGONM", GEOMETRYSUBTYPE);
	failed += keywords_insert("MULTIPOINTM", GEOMETRYSUBTYPE);
	failed += keywords_insert("MULTILINESTRINGM", GEOMETRYSUBTYPE);
	failed += keywords_insert("MULTIPOLYGONM", GEOMETRYSUBTYPE);
	failed += keywords_insert("GEOMETRYCOLLECTIONM", GEOMETRYSUBTYPE);

	failed += keywords_insert("POINTZM", GEOMETRYSUBTYPE);
	failed += keywords_insert("LINESTRINGZM", GEOMETRYSUBTYPE);
	failed += keywords_insert("POLYGONZM", GEOMETRYSUBTYPE);
	failed += keywords_insert("MULTIPOINTZM", GEOMETRYSUBTYPE);
	failed += keywords_insert("MULTILINESTRINGZM", GEOMETRYSUBTYPE);
	failed += keywords_insert("MULTIPOLYGONZM", GEOMETRYSUBTYPE);
	failed += keywords_insert("GEOMETRYCOLLECTIONZM", GEOMETRYSUBTYPE);

	return failed;
}

#define find_keyword_bs(lc, s) find_keyword(lc->rs->buf+lc->rs->pos+s)

void
scanner_init(struct scanner *s, bstream *rs, stream *ws)
{
	s->rs = rs;
	s->ws = ws;
	s->log = NULL;

	s->yynext = 0;
	s->yylast = 0;
	s->yyval = 0;
	s->yybak = 0;		/* keep backup of char replaced by EOS */
	s->yycur = 0;

	s->key = 0;		/* keep a hash key of the query */
	s->started = 0;
	s->as = 0;

	s->mode = LINE_N;
	s->schema = NULL;
}

void
scanner_query_processed(struct scanner *s)
{
	int cur;

	if (s->yybak) {
		s->rs->buf[s->rs->pos + s->yycur] = s->yybak;
		s->yybak = 0;
	}
	if (s->rs) {
		s->rs->pos += s->yycur;
		/* completely eat the query including white space after the ; */
		while (s->rs->pos < s->rs->len &&
			   (cur = s->rs->buf[s->rs->pos], iswspace(cur))) {
			s->rs->pos++;
		}
	}
	/*assert(s->rs->pos <= s->rs->len);*/
	s->yycur = 0;
	s->key = 0;		/* keep a hash key of the query */
	s->started = 0;
	s->as = 0;
	s->schema = NULL;
}

void
scanner_reset_key(struct scanner *s)
{
	s->key = 0;
}

static int
scanner_error(mvc *lc, int cur)
{
	switch (cur) {
	case EOF:
		(void) sql_error(lc, 1, SQLSTATE(42000) "Unexpected end of input");
		return -1;	/* EOF needs -1 result */
	default:
		/* on Windows at least, iswcntrl returns TRUE for
		 * U+FEFF, but we just want consistent error
		 * messages */
		(void) sql_error(lc, 1, SQLSTATE(42000) "Unexpected%s character (U+%04X)", iswcntrl(cur) && cur != 0xFEFF ? " control" : "", cur);
	}
	return LEX_ERROR;
}


/*
   UTF-8 encoding is as follows:
U-00000000 - U-0000007F: 0xxxxxxx
U-00000080 - U-000007FF: 110xxxxx 10xxxxxx
U-00000800 - U-0000FFFF: 1110xxxx 10xxxxxx 10xxxxxx
U-00010000 - U-001FFFFF: 11110xxx 10xxxxxx 10xxxxxx 10xxxxxx
U-00200000 - U-03FFFFFF: 111110xx 10xxxxxx 10xxxxxx 10xxxxxx 10xxxxxx
U-04000000 - U-7FFFFFFF: 1111110x 10xxxxxx 10xxxxxx 10xxxxxx 10xxxxxx 10xxxxxx
*/
/* To be correctly coded UTF-8, the sequence should be the shortest
   possible encoding of the value being encoded.  This means that for
   an encoding of length n+1 (1 <= n <= 5), at least one of the bits in
   utf8chkmsk[n] should be non-zero (else the encoding could be
   shorter).
*/
static int utf8chkmsk[] = {
	0x0000007f,
	0x00000780,
	0x0000f800,
	0x001f0000,
	0x03e00000,
	0x7c000000
};

static void
utf8_putchar(struct scanner *lc, int ch)
{
	if ((ch) < 0x80) {
		lc->yycur--;
	} else if ((ch) < 0x800) {
		lc->yycur -= 2;
	} else if ((ch) < 0x10000) {
		lc->yycur -= 3;
	} else {
		lc->yycur -= 4;
	}
}

static inline int
scanner_read_more(struct scanner *lc, int n)
{
	bstream *b = lc->rs;
	int more = 0;


	while (b->len < b->pos + lc->yycur + n) {

		if (lc->mode == LINE_1 || !lc->started)
			return EOF;

		/* query is not finished ask for more */
		if (b->eof || !isa_block_stream(b->s)) {
			if (mnstr_write(lc->ws, PROMPT2, sizeof(PROMPT2) - 1, 1) == 1)
				mnstr_flush(lc->ws);
			b->eof = 0;
			more = 1;
		}
		/* we need more query text */
		if (bstream_next(b) < 0 ||
		    /* we asked for more data but didn't get any */
		    (more && b->eof && b->len < b->pos + lc->yycur + n))
			return EOF;
	}
	return 1;
}

static inline int
scanner_getc(struct scanner *lc)
{
	bstream *b = lc->rs;
	unsigned char *s = NULL;
	int c;
	int n, m, mask;

	if (scanner_read_more(lc, 1) == EOF) {
		lc->errstr = "end of input stream";
		return EOF;
	}
	lc->errstr = NULL;

	s = (unsigned char *) b->buf + b->pos + lc->yycur++;
	if (((c = *s) & 0x80) == 0) {
		/* 7-bit char */
		return c;
	}
	for (n = 0, m = 0x40; c & m; n++, m >>= 1)
		;
	/* n now is number of 10xxxxxx bytes that should follow */
	if (n == 0 || n >= 6 || (b->pos + n) > b->len) {
		/* incorrect UTF-8 sequence */
		/* n==0: c == 10xxxxxx */
		/* n>=6: c == 1111111x */
		lc->errstr = SQLSTATE(42000) "invalid start of UTF-8 sequence";
		goto error;
	}

	if (scanner_read_more(lc, n) == EOF)
		return EOF;
	s = (unsigned char *) b->buf + b->pos + lc->yycur;

	mask = utf8chkmsk[n];
	c &= ~(0xFFC0 >> n);	/* remove non-x bits */
	while (--n >= 0) {
		c <<= 6;
		lc->yycur++;
		if (((m = *s++) & 0xC0) != 0x80) {
			/* incorrect UTF-8 sequence: byte is not 10xxxxxx */
			/* this includes end-of-string (m == 0) */
			lc->errstr = SQLSTATE(42000) "invalid continuation in UTF-8 sequence";
			goto error;
		}
		c |= m & 0x3F;
	}
	if ((c & mask) == 0) {
		/* incorrect UTF-8 sequence: not shortest possible */
		lc->errstr = "!not shortest possible UTF-8 sequence";
		goto error;
	}

	return c;

error:
	if (b->pos + lc->yycur < b->len)	/* skip bogus char */
		lc->yycur++;
	return EOF;
}

static int
scanner_token(struct scanner *lc, int token)
{
	lc->yybak = lc->rs->buf[lc->rs->pos + lc->yycur];
	lc->rs->buf[lc->rs->pos + lc->yycur] = 0;
	lc->yyval = token;
	return lc->yyval;
}

static int
scanner_string(mvc *c, int quote)
{
	struct scanner *lc = &c->scanner;
	bstream *rs = lc->rs;
	int cur = quote;
	int escape = 0;

	lc->started = 1;
	while (cur != EOF) {
		unsigned int pos = (int)rs->pos + lc->yycur;

		while ((((cur = rs->buf[pos++]) & 0x80) == 0) && cur && (cur != quote || escape)) {
			if (cur != '\\')
				escape = 0;
			else
				escape = !escape;
		}
		lc->yycur = pos - (int)rs->pos;
		/* check for quote escaped quote: Obscure SQL Rule */
		/* TODO also handle double "" */
		if (cur == quote && rs->buf[pos] == quote) {
			rs->buf[pos - 1] = '\\';
			lc->yycur++;
			continue;
		}
		assert(pos <= rs->len + 1);
		if (cur == quote && !escape) {
			return scanner_token(lc, STRING);
		}
		lc->yycur--;	/* go back to current (possibly invalid) char */
		/* long utf8, if correct isn't the quote */
		if (!cur) {
			if (lc->rs->len >= lc->rs->pos + lc->yycur + 1) {
				(void) sql_error(c, 2, SQLSTATE(42000) "NULL byte in string");
				return LEX_ERROR;
			}
			cur = scanner_read_more(lc, 1);
		} else {
			cur = scanner_getc(lc);
		}
	}
	(void) sql_error(c, 2, SQLSTATE(42000) "%s", lc->errstr ? lc->errstr : "unexpected end of input");
	return LEX_ERROR;
}

/* scan a structure {blah} into a string. We only count the matching {}
 * unless escaped. We do not consider embeddings in string literals yet
 */

static int
scanner_body(mvc *c)
{
	struct scanner *lc = &c->scanner;
	bstream *rs = lc->rs;
	int cur = (int) 'x';
	int blk = 1;
	int escape = 0;

	lc->started = 1;
	assert(rs->buf[(int)rs->pos + lc->yycur-1] == '{');
	while (cur != EOF) {
		unsigned int pos = (int)rs->pos + lc->yycur;

		while ((((cur = rs->buf[pos++]) & 0x80) == 0) && cur && (blk || escape)) {
			if (cur != '\\')
				escape = 0;
			else
				escape = !escape;
			blk += cur =='{';
			blk -= cur =='}';
		}
		lc->yycur = pos - (int)rs->pos;
		assert(pos <= rs->len + 1);
		if (blk == 0 && !escape){
			lc->yycur--;	/* go back to current (possibly invalid) char */
			return scanner_token(lc, X_BODY);
		}
		lc->yycur--;	/* go back to current (possibly invalid) char */
		if (!cur) {
			if (lc->rs->len >= lc->rs->pos + lc->yycur + 1) {
				(void) sql_error(c, 2, SQLSTATE(42000) "NULL byte in string");
				return LEX_ERROR;
			}
			cur = scanner_read_more(lc, 1);
		} else {
			cur = scanner_getc(lc);
		}
	}
	(void) sql_error(c, 2, SQLSTATE(42000) "Unexpected end of input");
	return LEX_ERROR;
}

static int 
keyword_or_ident(mvc * c, int cur)
{
	struct scanner *lc = &c->scanner;
	keyword *k = NULL;
	int s;

	lc->started = 1;
	utf8_putchar(lc, cur);
	s = lc->yycur;
	lc->yyval = IDENT;
	while ((cur = scanner_getc(lc)) != EOF) {
		if (!iswalnum(cur) && cur != '_') {
			utf8_putchar(lc, cur);
			(void)scanner_token(lc, IDENT);
			k = find_keyword_bs(lc,s);
			if (k) 
				lc->yyval = k->token;
			/* find keyword in SELECT/JOIN/UNION FUNCTIONS */
			else if (sql_find_func(c->sa, cur_schema(c), lc->rs->buf+lc->rs->pos+s, -1, F_FILT, NULL)) 
				lc->yyval = FILTER_FUNC;
			return lc->yyval;
		}
	}
	(void)scanner_token(lc, IDENT);
	k = find_keyword_bs(lc,s);
	if (k) 
		lc->yyval = k->token;
	/* find keyword in SELECT/JOIN/UNION FUNCTIONS */
	else if (sql_find_func(c->sa, cur_schema(c), lc->rs->buf+lc->rs->pos+s, -1, F_FILT, NULL)) 
		lc->yyval = FILTER_FUNC;
	return lc->yyval;
}

static int 
skip_white_space(struct scanner * lc)
{
	int cur;

	lc->yysval = lc->yycur;
	while ((cur = scanner_getc(lc)) != EOF && iswspace(cur))
		lc->yysval = lc->yycur;
	return cur;
}

static int 
skip_c_comment(struct scanner * lc)
{
	int cur;
	int prev = 0;
	int started = lc->started;
	int depth = 1;

	lc->started = 1;
	while (depth > 0 && (cur = scanner_getc(lc)) != EOF) {
		if (prev == '*' && cur == '/')
			depth--;
		else if (prev == '/' && cur == '*') {
			/* block comments can nest */
			cur = 0; /* prevent slash-star-slash from matching */
			depth++;
		}
		prev = cur;
	}
	lc->yysval = lc->yycur;
	lc->started = started;
	/* a comment is equivalent to a newline */
	return cur == EOF ? cur : '\n';
}

static int 
skip_sql_comment(struct scanner * lc)
{
	int cur;
	int started = lc->started;

	lc->started = 1;
	while ((cur = scanner_getc(lc)) != EOF && (cur != '\n'))
		;
	lc->yysval = lc->yycur;
	lc->started = started;
	/* a comment is equivalent to a newline */
	return cur;
}

static int tokenize(mvc * lc, int cur);

static int 
number(mvc * c, int cur)
{
	struct scanner *lc = &c->scanner;
	int token = sqlINT;
	int before_cur = EOF;

	lc->started = 1;
	if (cur == '0' && (cur = scanner_getc(lc)) == 'x') {
		while ((cur = scanner_getc(lc)) != EOF && 
		       (iswdigit(cur) || 
				 (cur >= 'A' && cur <= 'F') || 
				 (cur >= 'a' && cur <= 'f')))
			token = HEXADECIMAL; 
		if (token == sqlINT)
			before_cur = 'x';
	} else {
		if (iswdigit(cur))
			while ((cur = scanner_getc(lc)) != EOF && iswdigit(cur)) 
				;
		if (cur == '@') {
			token = OIDNUM;
			cur = scanner_getc(lc);
			if (cur == '0')
				cur = scanner_getc(lc);
		}

		if (cur == '.') {
			token = INTNUM;
	
			while ((cur = scanner_getc(lc)) != EOF && iswdigit(cur)) 
				;
		}
		if (cur == 'e' || cur == 'E') {
			token = APPROXNUM;
			cur = scanner_getc(lc);
			if (cur == '-' || cur == '+') 
				token = 0;
			while ((cur = scanner_getc(lc)) != EOF && iswdigit(cur)) 
				token = APPROXNUM;
		}
	}

	if (cur == EOF && lc->rs->buf == NULL) /* malloc failure */
		return EOF;

	if (token) {
		if (cur != EOF)
			utf8_putchar(lc, cur);
		if (before_cur != EOF)
			utf8_putchar(lc, before_cur);
		return scanner_token(lc, token);
	} else {
		(void)sql_error( c, 2, SQLSTATE(42000) "Unexpected symbol %lc", (wint_t) cur);
		return LEX_ERROR;
	}
}

static
int scanner_symbol(mvc * c, int cur)
{
	struct scanner *lc = &c->scanner;
	int next = 0;
	int started = lc->started;

	switch (cur) {
	case '/':
		lc->started = 1;
		next = scanner_getc(lc);
		if (next == '*') {
			lc->started = started;
			cur = skip_c_comment(lc);
			if (cur < 0)
				return EOF;
			return tokenize(c, cur);
		} else {
			utf8_putchar(lc, next); 
			return scanner_token(lc, cur);
		}
	case '0':
	case '1':
	case '2':
	case '3':
	case '4':
	case '5':
	case '6':
	case '7':
	case '8':
	case '9':
		return number(c, cur);
	case '#':
		if ((cur = skip_sql_comment(lc)) == EOF)
			return cur;
		return tokenize(c, cur);
	case '\'':
	case '"':
		return scanner_string(c, cur);
	case '{':
		return scanner_body(c);
	case '-':
		lc->started = 1;
		next = scanner_getc(lc);
		if (next == '-') {
			lc->started = started;
			if ((cur = skip_sql_comment(lc)) == EOF)
				return cur;
			return tokenize(c, cur);
		}
		lc->started = 1;
		utf8_putchar(lc, next); 
		return scanner_token(lc, cur);
	case '~': /* binary not */
		lc->started = 1;
		next = scanner_getc(lc);
		if (next == '=') 
			return scanner_token(lc, GEOM_MBR_EQUAL);
		utf8_putchar(lc, next); 
		return scanner_token(lc, cur);
	case '^': /* binary xor */
	case '*':
	case '?':
	case '%':
	case '+':
	case '(':
	case ')':
	case ',':
	case '=':
	case '[':
	case ']':
		lc->started = 1;
		return scanner_token(lc, cur);
	case '&':
		lc->started = 1;
		cur = scanner_getc(lc);
		if(cur == '<') {
			next = scanner_getc(lc);
			if(next == '|') {
				return scanner_token(lc, GEOM_OVERLAP_OR_BELOW);
			} else {
				utf8_putchar(lc, next); //put the char back
				return scanner_token(lc, GEOM_OVERLAP_OR_LEFT);
			}
		} else if(cur == '>')
			return scanner_token(lc, GEOM_OVERLAP_OR_RIGHT);
		else if(cur == '&')
			return scanner_token(lc, GEOM_OVERLAP);
		else {/* binary and */
			utf8_putchar(lc, cur); //put the char back
			return scanner_token(lc, '&');
		}
	case '@':
		lc->started = 1;
		return scanner_token(lc, AT);
	case ';':
		lc->started = 0;
		return scanner_token(lc, SCOLON);
	case '<':
		lc->started = 1;
		cur = scanner_getc(lc);
		if (cur == '=') {
			return scanner_token( lc, COMPARISON);
		} else if (cur == '>') {
			return scanner_token( lc, COMPARISON);
		} else if (cur == '<') {
			next = scanner_getc(lc);
			if (next == '=') {
				return scanner_token( lc, LEFT_SHIFT_ASSIGN);
			} else if (next == '|') {
				return scanner_token(lc, GEOM_BELOW);
			} else {
				utf8_putchar(lc, next); //put the char back
				return scanner_token( lc, LEFT_SHIFT);
			}
		} else if(cur == '-') {
			next = scanner_getc(lc);
			if(next == '>') {
				return scanner_token(lc, GEOM_DIST);
			} else {
				//put the characters back and fall in the next possible case
				utf8_putchar(lc, next);
				utf8_putchar(lc, cur);
				return scanner_token( lc, COMPARISON);
			}
		} else {
			utf8_putchar(lc, cur); 
			return scanner_token( lc, COMPARISON);
		}
	case '>':
		lc->started = 1;
		cur = scanner_getc(lc);
		if (cur == '>') {
			cur = scanner_getc(lc);
			if (cur == '=')
				return scanner_token( lc, RIGHT_SHIFT_ASSIGN);
			utf8_putchar(lc, cur); 
			return scanner_token( lc, RIGHT_SHIFT);
		} else if (cur != '=') {
			utf8_putchar(lc, cur); 
			return scanner_token( lc, COMPARISON);
		} else {
			return scanner_token( lc, COMPARISON);
		}
	case '.':
		lc->started = 1;
		cur = scanner_getc(lc);
		if (!iswdigit(cur)) {
			utf8_putchar(lc, cur); 
			return scanner_token( lc, '.');
		} else {
			utf8_putchar(lc, cur); 
			cur = '.';
			return number(c, cur);
		}
	case '|': /* binary or or string concat */
		lc->started = 1;
		cur = scanner_getc(lc);
		if (cur == '|') {
			return scanner_token(lc, CONCATSTRING);
		} else if (cur == '&') {
			next = scanner_getc(lc);
			if(next == '>') {
				return scanner_token(lc, GEOM_OVERLAP_OR_ABOVE);
			} else {
				utf8_putchar(lc, next); //put the char back
				utf8_putchar(lc, cur); //put the char back
				return scanner_token(lc, '|');
			}
		} else if (cur == '>') {
			next = scanner_getc(lc);
			if(next == '>') {
				return scanner_token(lc, GEOM_ABOVE);
			} else {
				utf8_putchar(lc, next); //put the char back
				utf8_putchar(lc, cur); //put the char back
				return scanner_token(lc, '|');
			}
		} else {
			utf8_putchar(lc, cur); 
			return scanner_token(lc, '|');
		}
	}
	(void)sql_error( c, 3, SQLSTATE(42000) "Unexpected symbol (%lc)", (wint_t) cur);
	return LEX_ERROR;
}

static int 
tokenize(mvc * c, int cur)
{
	struct scanner *lc = &c->scanner;
	while (1) {
		if (cur == 0xFEFF) {
			/* on Linux at least, iswpunct returns TRUE
			 * for U+FEFF, but we don't want that, we just
			 * want to go to the scanner_error case
			 * below */
			;
		} else if (iswspace(cur)) {
			if ((cur = skip_white_space(lc)) == EOF)
				return cur;
			continue;  /* try again */
		} else if (iswdigit(cur)) {
			return number(c, cur);
		} else if (iswalpha(cur) || cur == '_') {
			return keyword_or_ident(c, cur);
		} else if (iswpunct(cur)) {
			return scanner_symbol(c, cur);
		}
		if (cur == EOF) {
			if (lc->mode == LINE_1 || !lc->started )
				return cur;
			return scanner_error(c, cur);
		}
		/* none of the above: error */
		return scanner_error(c, cur);
	}
}

/* SQL 'quoted' idents consist of a set of any character of 
 * the source language character set other than a 'quote' 
 *
 * MonetDB has 2 restrictions:
 * 	1 we disallow '%' as the first character.
 * 	2 the length is reduced to 1024 characters 
 */
static int
valid_ident(char *s, char *dst)
{
	int escaped = 0;
	int p = 0;
	
	if (*s == '%')
		return 0;

	while (*s && (*s != '"' || escaped)) {
		if (*s == '\\' && s[1] == '"') {
			escaped = !escaped;
			if (escaped) 
				dst[p++] = *s;
		} else if (*s == '"' && escaped) {
			escaped = 0;
			dst[p++] = *s;
		} else {
			escaped = 0;
			dst[p++] = *s;
		}
		s++;
		if (p >= 1024)
			return 0;
	}
	if (*s)
		return 0;
	dst[p] = '\0';
	return 1;
}

static inline int
sql_get_next_token(YYSTYPE *yylval, void *parm) {
	mvc *c = (mvc*)parm;
	struct scanner *lc = &c->scanner;
	int token = 0, cur = 0;

	if (lc->rs->buf == NULL) /* malloc failure */
		return EOF;

	if (lc->yynext) {
		int next = lc->yynext;

		lc->yynext = 0;
		return(next);
	}

	if (lc->yybak) {
		lc->rs->buf[lc->rs->pos + lc->yycur] = lc->yybak;
		lc->yybak = 0;
	}
	
	lc->yysval = lc->yycur;
	lc->yylast = lc->yyval;
	cur = scanner_getc(lc);
	if (cur < 0)
		return EOF;
	token = tokenize(c, cur);

	yylval->sval = (lc->rs->buf + (int)lc->rs->pos + lc->yysval);

	/* This is needed as ALIAS and aTYPE get defined too late, see
	   sql_keyword.h */
	if (token == KW_ALIAS)
		token = ALIAS;

	if (token == KW_TYPE)
		token = aTYPE;

	if (token == IDENT || token == COMPARISON || token == FILTER_FUNC || token == AGGR || token == AGGR2 || token == RANK || token == aTYPE || token == ALIAS)
		yylval->sval = sa_strndup(c->sa, yylval->sval, lc->yycur-lc->yysval);
	else if (token == STRING) {
		char quote = *yylval->sval;
		char *str = sa_alloc( c->sa, (lc->yycur-lc->yysval-2)*2 + 1 );
		assert(quote == '"' || quote == '\'');

		lc->rs->buf[lc->rs->pos+lc->yycur- 1] = 0; 
		if (quote == '"') {
			if (valid_ident(yylval->sval+1,str)) {
				token = IDENT;
			} else {
				sql_error(c, 1, SQLSTATE(42000) "Invalid identifier '%s'", yylval->sval+1);
				return LEX_ERROR;
			}
		} else {
			memcpy(str, yylval->sval+1, lc->yycur-lc->yysval - 1);
		}
		yylval->sval = str;

		/* reset original */
		lc->rs->buf[lc->rs->pos+lc->yycur- 1] = quote; 
	}

	return(token);
}

/* also see sql_parser.y */
extern int sqllex( YYSTYPE *yylval, void *m );

int
sqllex(YYSTYPE * yylval, void *parm)
{
	int token;
	mvc *c = (mvc *) parm;
	struct scanner *lc = &c->scanner;
	int pos;

	/* store position for when view's query ends */
	pos = (int)lc->rs->pos + lc->yycur;

	token = sql_get_next_token(yylval, parm);
	
	if (token == NOT) {
		int next = sqllex(yylval, parm);

		if (next == NOT) {
			return sqllex(yylval, parm);
		} else if (next == BETWEEN) {
			token = NOT_BETWEEN;
		} else if (next == sqlIN) {
			token = NOT_IN;
		} else if (next == LIKE) {
			token = NOT_LIKE;
		} else if (next == ILIKE) {
			token = NOT_ILIKE;
		} else {
			lc->yynext = next;
		}
	} else if (token == UNION) {
		int next = sqllex(yylval, parm);

		if (next == JOIN) {
			token = UNIONJOIN;
		} else {
			lc->yynext = next;
		}
	} else if (token == NO) {
		int next = sqllex(yylval, parm);

		switch (next) {
			case MAXVALUE:
				token = NOMAXVALUE;
			break;
			case MINVALUE:
				token = NOMINVALUE;
			break;
			case CYCLE:
				token = NOCYCLE;
			break;
			default:
				lc->yynext = next;
			break;
		}
	} else if (token == SCOLON) {
		/* ignore semi-colon(s) following a semi-colon */
		if (lc->yylast == SCOLON) {
			int prev = lc->yycur;
			while ((token = sql_get_next_token(yylval, parm)) == SCOLON)
				prev = lc->yycur;

			/* skip the skipped stuff also in the buffer */
			lc->rs->pos += prev;
			lc->yycur -= prev;
			assert(lc->yycur >= 0);
		}
	}

	if (lc->log) 
		mnstr_write(lc->log, lc->rs->buf+pos, lc->rs->pos + lc->yycur - pos, 1);

	/* Don't include literals in the calculation of the key */
	if (token != STRING && token != sqlINT && token != OIDNUM && token != INTNUM && token != APPROXNUM && token != sqlNULL)
		lc->key ^= token;
	lc->started += (token != EOF);
	return token;
}
