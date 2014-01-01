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
 * Copyright August 2008-2014 MonetDB B.V.
 * All Rights Reserved.
 */

%{
/* copied from Windows' monetdb_config.h because we cannot include it
 * here, but need it on windows for stream.h */
#if defined(_MSC_VER) || defined(__CYGWIN__) || defined(__MINGW32__)
/* Define to `int' if <sys/types.h> does not define. */
#ifdef _WIN64
#define ssize_t __int64
#else
#define ssize_t int
#endif
#endif

#include <stdio.h>
#include <stream.h>
#include "jaqltree.h"
#include "jaql.tab.h"

/* prompt: more data needed (stolen from mapi.h to avoid conflicts) */
#define PROMPT2  "\001\002\n"

#ifdef _MSC_VER
#define snprintf _snprintf
#define fileno _fileno
__declspec(dllimport)
#else
extern
#endif
char *GDKstrdup(const char *);
%}

%option reentrant
%option noyywrap
%option bison-bridge
%option case-insensitive
%option batch
%option nostdinit
%option nodefault
%option fast
%option extra-type="struct _jc *"

%{
YYSTYPE yylval;
extern void jaqlerror(jc* j, char const *msg);

/* set line numbers each time a token is recognised */
#define YY_USER_ACTION yyextra->tokstart = yytext;

#define YY_INPUT(buf, res, max_size) res = readinput(yyextra, buf, res, max_size)
#define YY_NO_INPUT

/* ugly as hell, but needed to prevent flex from assigning stdin to yyin */
#ifdef stdin
# undef stdin
#endif
#define stdin (FILE *)0
#ifdef stdout
# undef stdout
#endif
#define stdout (FILE *)0

static size_t readinput(jc *j, char *buf, size_t res, size_t max_size) {
	if (j->buf != NULL && j->buf[j->pos] != '\0') {
		res = (int)strlen(j->buf + j->pos);  /* FIXME: cache this */
		if (res > max_size)
			res = max_size;
		memcpy(buf, j->buf + j->pos, res);
		j->start = j->pos;
		j->pos += res;
		j->scanbuf = buf;
	} else {
		if (j->expect_json != ';' && j->scanstreamin != NULL) {
			bstream *bs = (bstream *)j->scanstreamin;

			/* try and read some more data */
			if (bstream_next(bs) < 0) {
				/* read failed, force shutdown next iteration */
				j->scanstreameof = 1;
				res = YY_NULL;
				return res;
			}

			/* request more if we appear to be at the end of current
			 * block */
			if (j->buf != NULL && bs->eof == 1) {
				stream *s = (stream *)j->scanstreamout;
				if (mnstr_write(s, PROMPT2, sizeof(PROMPT2) - 1, 1) == 1) {
					mnstr_flush(s);
					bs->eof = 0;
				}
				if (bstream_next(bs) < 0) {
					/* read failed, force shutdown next iteration */
					j->scanstreameof = 1;
					res = YY_NULL;
					return res;
				}
			}

			/* did we get some query text */
			if (bs->eof == 0) {
				j->buf = bs->buf;
				res = readinput(j, buf, res, max_size);
				return res;
			}
		}
		res = YY_NULL;
	}
	return res;
}
%}

 /* allow to disregard JSON bits and deal with trailing garbage after ; */
%x ARR OBJ SCOLON

%%
	/* if the parser expects a JSON bit, give it */
	if (yyextra->expect_json == '[') {
		yyextra->esc_depth = 0;
		yyextra->expect_json = 0;
		unput('[');
		BEGIN(ARR);
	} else if (yyextra->expect_json == '{') {
		yyextra->esc_depth = 0;
		yyextra->expect_json = 0;
		unput('{');
		BEGIN(OBJ);
	}

 /* core language */
each       return EACH;
filter     return FILTER;
transform  return TRANSFORM;
expand     return EXPAND;
unroll     return UNROLL;
group      return GROUP;
into       return INTO;
by         return BY;
as         return AS;
join       return JOIN;
preserve   return PRESERVE;
where      return WHERE;
in         return IN;
sort       return SORT;
top        return TOP;
desc       return DESC;
asc        return ASC;

 /* debugging aids */
explain    return EXPLAIN;
plan       return PLAN;
planf      return PLANF;
debug      return DEBUG;
trace      return TRACE;
time       return TIME;

 /* language constructs */
"->"       return ARROW;
-?[0-9]+               {yylval->j_number = atol(yytext); return NUMBER;}
-?[0-9]+\.[0-9e+-]+    {yylval->j_double = atof(yytext); return DOUBLE;}
\"[^\"]*\"             {yytext[yyleng - 1] = '\0';
                        yylval->j_string = GDKstrdup(yytext + 1);
                        return STRING;}
"$"        return '$';
"="        return ASSIGN;
"=="       return EQUALS;
"!="       return NEQUAL;
">"        return GREATER;
">="       return GEQUAL;
"<"        return LESS;
"<="       return LEQUAL;
not        return NOT;
and        return AND;
or         return OR;
true       return TRUE;
false      return FALSE;
null       return NIL;
"."        return '.';
":"        return ':';
","        return ',';
"("        return '(';
")"        return ')';
"+"        return '+';
"-"        return '-';
"*"        return '*';
"/"        return '/';
<ARR>{
	"]"   {
		if (--yyextra->esc_depth == 0) {
			yylval->j_json = GDKstrdup(yytext);
			BEGIN(INITIAL);
			return ARRAY;
		} else {
			yymore();
		}
	}
	"["   {yymore(); yyextra->esc_depth++;}
	.     yymore();
	\n    yymore();
	<<EOF>> {
		jaqlerror(yyextra, "unexpected end of input");
		yyextra->pos = yyextra->start = strlen(yyextra->buf);
		yyextra->scanbuf = yyextra->tokstart = NULL;
		YY_FLUSH_BUFFER;
		BEGIN(INITIAL);
		return EOF;
	}
}
"]"        return ']';
"["        return '[';
<OBJ>{
	"}"   {
		if (--yyextra->esc_depth == 0) {
			yylval->j_json = GDKstrdup(yytext);
			BEGIN(INITIAL);
			return OBJECT;
		} else {
			yymore();
		}
	}
	"{"   {yymore(); yyextra->esc_depth++;}
	.     yymore();
	\n    yymore();
	<<EOF>> {
		jaqlerror(yyextra, "unexpected end of input");
		yyextra->pos = yyextra->start = strlen(yyextra->buf);
		yyextra->scanbuf = yyextra->tokstart = NULL;
		YY_FLUSH_BUFFER;
		BEGIN(INITIAL);
		return EOF;
	}
}
"}"        return '}';
"{"        return '{';
";"        { yyextra->expect_json = ';'; BEGIN(SCOLON); }
<SCOLON>{
	";" /* ignore superfluous semi-colons */;
	[ \t\r\n]+ /* ignore whitespace */;
	<<EOF>> { yyextra->expect_json = 0; BEGIN(INITIAL); return ';'; }
	.  { unput(yytext[0]); yyextra->expect_json = 0; BEGIN(INITIAL); return ';'; }
}
[ \t\r\n]+ /* ignore whitespace */;
("#"|"//").*\n /* ignore comments */;
[a-zA-Z_][a-zA-Z0-9_]* {yylval->j_ident = GDKstrdup(yytext); return IDENT;}
<<EOF>>    return EOF;
.          {
	char buf[32];
	snprintf(buf, sizeof(buf), "unexpected character: %c", yytext[0]);
	jaqlerror(yyextra, buf);
	yyextra->pos = yyextra->start = strlen(yyextra->buf);
	yyextra->scanbuf = yyextra->tokstart = NULL;
	YY_FLUSH_BUFFER;
	BEGIN(INITIAL);
	return EOF;
}
%%
