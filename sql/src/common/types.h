#ifndef TYPES_H
#define TYPES_H

#include "list.h"
#include <stream.h>

sql_export list *types;

typedef struct sql_type {
	char *sqlname;
	char *name;
	unsigned int digits;
	unsigned int scale;
	unsigned int radix; 
	int nr;
} sql_type;

typedef struct sql_subtype {
	sql_type *type;
	unsigned int digits;
	unsigned int scale;
} sql_subtype;

typedef struct sql_aggr {
	char *name;
	char *imp;
	char *tpe;
	char *res;
	int nr;
} sql_aggr;

typedef struct sql_func {
	char *name;
	char *imp;
	char *tpe1;
	char *tpe2;
	char *tpe3;
	char *res;
	int nr;
} sql_func;

sql_export sql_subtype *sql_bind_subtype( char *name, int digits, int scale );
extern sql_subtype *sql_bind_localtype( char *name );
extern sql_subtype *sql_create_subtype( sql_type *t, int s, int d );
sql_export sql_subtype *sql_dup_subtype( sql_subtype *t );
sql_export void sql_subtype_destroy( sql_subtype *t );

extern int subtype_cmp( sql_subtype *t1, sql_subtype *t2);

sql_export sql_type *sql_create_type(char *sqlname, int digits, 
					int scale, int radix, char *name );

extern sql_aggr *sql_bind_aggr(char *name, sql_subtype *type);
sql_export sql_aggr *sql_create_aggr(char *name, char *imp, char *tpe, char *res);

extern sql_func *sql_bind_func(char *name, sql_subtype *tp1, sql_subtype *tp2, sql_subtype *tp3);
extern sql_func *sql_bind_func_result(char *name, 
				sql_subtype *tp1, sql_subtype *tp2, sql_subtype *tp3, sql_subtype *res);

sql_export sql_func *sql_create_func(char *name, char *imp,
			     	char *tpe1, char *tpe2, char *tp3, char *res);

sql_export void parser_init(int debug);
sql_export void parser_exit();

#endif /* TYPES_H */
