#ifndef SYM_H
#define SYM_H

#ifdef _MSC_VER
#include <sql_config.h>
#endif

#ifdef _MSC_VER
#ifndef LIBSQL
#define sql_export extern __declspec(dllimport)
#else
#define sql_export extern __declspec(dllexport)
#endif
#else
#define sql_export extern
#endif

typedef enum symtype {
	type_int,
	type_string,
	type_list,
	type_atom,
	type_symbol,
	type_stmt,
	type_column,
	type_table,
	type_schema,
	type_type,
	type_aggr,
	type_func
} symtype;

typedef union symdata {
	int ival;
	char *sval;
	struct atom *aval;
	struct list *lval;
	struct stmt *stval;
	struct group *gval;
	struct column *cval;
	struct key *kval;
	struct table *tval;
	struct schema *schema;
	struct sql_subtype *typeval;
	struct sql_aggr *aggrval;
	struct sql_func *funcval;
} symdata;


#endif
