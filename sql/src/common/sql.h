#ifndef _SQL_H_
#define _SQL_H_

#include <stdio.h>
#include <stdarg.h>
#include "list.h"
#include "catalog.h"
#include "context.h"
#include "symbol.h"
#include "statement.h"

#define D__SQL	16

#define _(String) (String)
#define N_(String) (String)

extern char *mkLower(char *v);
extern char *toLower(const char *v);

typedef enum tokens {
	SQL_CREATE_SCHEMA,
	SQL_CREATE_TABLE,
	SQL_CREATE_VIEW,
	SQL_DROP_SCHEMA,
	SQL_DROP_TABLE,
	SQL_DROP_VIEW,
	SQL_ALTER_TABLE,
	SQL_NAME,
	SQL_USER,
	SQL_PATH,
	SQL_CHARSET,
	SQL_TABLE,
	SQL_CASE,
	SQL_CAST,
	SQL_COLUMN,
	SQL_COLUMN_OPTIONS,
	SQL_COALESCE,
	SQL_CONSTRAINT,
	SQL_CHECK,
	SQL_DEFAULT,
	SQL_NOT_NULL,
	SQL_NULL,
	SQL_NULLIF,
	SQL_UNIQUE,
	SQL_PRIMARY_KEY,
	SQL_FOREIGN_KEY,
	SQL_BEGIN,
	SQL_COMMIT,
	SQL_ROLLBACK,
	SQL_SAVEPOINT,
	SQL_RELEASE,
	SQL_INSERT,
	SQL_DELETE,
	SQL_UPDATE,
	SQL_CROSS,
	SQL_JOIN,
	SQL_SELECT,
	SQL_WHERE,
	SQL_FROM,
	SQL_UNION,
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
	SQL_UNOP,
	SQL_BINOP,
	SQL_TRIOP,
	SQL_BETWEEN,
	SQL_NOT_BETWEEN,
	SQL_LIKE,
	SQL_NOT_LIKE,
	SQL_IN,
	SQL_NOT_IN,
	SQL_GRANT,
	SQL_REVOKE,
	SQL_PARAMETER,
	SQL_AGGR,
	SQL_COMPARE,
	SQL_TEMP_LOCAL,
	SQL_TEMP_GLOBAL,
	SQL_INT_VALUE,
	SQL_ATOM,
	SQL_USING,
	SQL_WHEN,
	SQL_ESCAPE,
	SQL_COPYFROM,
	SQL_COPYTO
} tokens;

typedef enum jt {
	jt_inner = 0,
	jt_left = 1,
	jt_right = 2,
	jt_full = 3,
	jt_union = 4
} jt;

extern const char *token2string(int token);

extern stmt *semantic(context * sql, symbol * sym);

#endif /*_SQL_H_*/
