/*-------------------------------------------------------------------------
 *
 * jsonpath_internal.h
 *     Private definitions for jsonpath scanner & parser
 *
 * Portions Copyright (c) 1996-2024, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 *
 * src/backend/utils/adt/jsonpath_internal.h
 *
 *-------------------------------------------------------------------------
 */

#ifndef JSONPATH_INTERNAL_H
#define JSONPATH_INTERNAL_H

#include "postgres_defines.h"
#include "postgres_defines_internal.h"

#include "jsonpath.h"

/* struct JsonPathString is shared between scan and gram */
typedef struct JsonPathString
{
	char	   *val;
	int			len;
	int			total;
} JsonPathString;

#include "jsonpath_gram.h"

#define YY_DECL extern int     jsonpath_yylex(YYSTYPE *yylval_param, yyscan_t yyscanner, \
							  JsonPathParseResult **result, \
							  struct Node *escontext)
YY_DECL;
extern void jsonpath_yyerror(yyscan_t yyscanner, JsonPathParseResult **result,
							 struct Node *escontext,
							 const char *message);

extern int	jsonpath_yyparse(yyscan_t yyscanner, JsonPathParseResult **result,
							 struct Node *escontext);

#endif							/* JSONPATH_INTERNAL_H */
