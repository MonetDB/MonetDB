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

#ifndef _SQL_PARSER_H_
#define _SQL_PARSER_H_

#include "sql_statement.h"

/* the next define makes the parser output more specific error messages,
 * instead of only a dull 'parse error' */
#define YYERROR_VERBOSE 1
/* enable this to get an idea of what the parser is doing on stdout
#define YYDEBUG 1
*/

typedef enum tokens {
	SQL_CREATE_SCHEMA,
	SQL_CREATE_TABLE,
	SQL_CREATE_VIEW,
	SQL_CREATE_INDEX,
	SQL_CREATE_ROLE,
	SQL_CREATE_USER,
	SQL_CREATE_TYPE,
	SQL_CREATE_PROC,
	SQL_CREATE_FUNC,
	SQL_CREATE_AGGR,
	SQL_CREATE_SEQ,
	SQL_CREATE_TRIGGER,
	SQL_DROP_SCHEMA,
	SQL_DROP_TABLE,
	SQL_DROP_VIEW,
	SQL_DROP_INDEX,
	SQL_DROP_ROLE,
	SQL_DROP_USER,
	SQL_DROP_TYPE,
	SQL_DROP_FUNC,
	SQL_DROP_PROC,
	SQL_DROP_SEQ,
	SQL_DROP_TRIGGER,
	SQL_ALTER_TABLE,
	SQL_ALTER_SEQ,
	SQL_ALTER_USER,
	SQL_RENAME_USER,
	SQL_DROP_COLUMN,
	SQL_DROP_CONSTRAINT,
	SQL_DROP_DEFAULT,
	SQL_DECLARE,
	SQL_SET,
	SQL_CALL,
	SQL_PREP,
	SQL_NAME,
	SQL_USER,
	SQL_PATH,
	SQL_CHARSET,
	SQL_SCHEMA,
	SQL_TABLE,
	SQL_TABLE_OPERATOR,
	SQL_TYPE,
	SQL_CASE,
	SQL_CAST,
	SQL_RETURN,
	SQL_IF,
	SQL_ELSE,
	SQL_WHILE,
	SQL_COLUMN,
	SQL_COLUMN_OPTIONS,
	SQL_COALESCE,
	SQL_CONSTRAINT,
	SQL_CHECK,
	SQL_DEFAULT,
	SQL_NOT_NULL,
	SQL_NULL,
	SQL_IS_NULL,
	SQL_IS_NOT_NULL,
	SQL_NULLIF,
	SQL_UNIQUE,
	SQL_PRIMARY_KEY,
	SQL_FOREIGN_KEY,
	SQL_BEGIN,
	TR_COMMIT,
	TR_ROLLBACK,
	TR_SAVEPOINT,
	TR_RELEASE,
	TR_START,
	TR_MODE,
	SQL_INSERT,
	SQL_DELETE,
	SQL_UPDATE,
	SQL_CROSS,
	SQL_JOIN,
	SQL_SELECT,
	SQL_CONNECT,
	SQL_DISCONNECT,
	SQL_DATABASE,
	SQL_PORT,
	SQL_WHERE,
	SQL_FROM,
	SQL_UNIONJOIN,
	SQL_UNION,
	SQL_EXCEPT,
	SQL_INTERSECT,
	SQL_VALUES,
	SQL_ASSIGN,
	SQL_ORDERBY,
	SQL_GROUPBY,
	SQL_DESC,
	SQL_AND,
	SQL_OR,
	SQL_NOT,
	SQL_EXISTS,
	SQL_NOT_EXISTS,
	SQL_OP,
	SQL_UNOP,
	SQL_BINOP,
	SQL_NOP,
	SQL_BETWEEN,
	SQL_NOT_BETWEEN,
	SQL_LIKE,
	SQL_NOT_LIKE,
	SQL_IN,
	SQL_NOT_IN,
	SQL_GRANT,
	SQL_GRANT_ROLES,
	SQL_REVOKE,
	SQL_REVOKE_ROLES,
	SQL_EXECUTE,
	SQL_PRIVILEGES,
	SQL_ROLE,
	SQL_PW_UNENCRYPTED,
	SQL_PW_ENCRYPTED,
	SQL_PARAMETER,
	SQL_FUNC,
	SQL_AGGR,
	SQL_RANK,
	SQL_COMPARE,
	SQL_TEMP_LOCAL,
	SQL_TEMP_GLOBAL,
	SQL_INT_VALUE,
	SQL_ATOM,
	SQL_USING,
	SQL_WHEN,
	SQL_ESCAPE,
	SQL_COPYFROM,
	SQL_BINCOPYFROM,
	SQL_COPYTO,
	SQL_EXPORT,
	SQL_NEXT,
	SQL_MULSTMT,
	SQL_WITH,
	SQL_XMLCOMMENT,
	SQL_XMLCONCAT,
	SQL_XMLDOCUMENT,
	SQL_XMLELEMENT,
	SQL_XMLATTRIBUTE,
	SQL_XMLFOREST,
	SQL_XMLPARSE,
	SQL_XMLPI,
	SQL_XMLQUERY,
	SQL_XMLTEXT,
	SQL_XMLVALIDATE,
	SQL_XMLNAMESPACES
} tokens;

typedef enum jt {
	jt_inner = 0,
	jt_left = 1,
	jt_right = 2,
	jt_full = 3,
	jt_union = 4
} jt;

extern char *token2string(int token);
extern void *sql_error(mvc *sql, int error_code, _In_z_ _Printf_format_string_ char *format, ...)
	__attribute__((__format__(__printf__, 3, 4)));
extern int parse_error(mvc *sql, const char *s);
extern int sqlparse(void *);

#endif /*_SQL_PARSER_H_*/

