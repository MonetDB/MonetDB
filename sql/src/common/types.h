#ifndef TYPES_H
#define TYPES_H

#include "list.h"
#include <stream.h>

typedef struct sql_type {
	char *sqlname;
	char *name;
	int nr;
} sql_type;

typedef struct sql_subtype {
	sql_type *type;
	unsigned int size;
	unsigned int digits;
} sql_subtype;

typedef struct sql_aggr {
	char *name;
	char *imp;
	sql_subtype *tpe;
	sql_subtype *res;
	int nr;
} sql_aggr;

typedef struct sql_func {
	char *name;
	char *imp;
	sql_subtype *tpe1;
	sql_subtype *tpe2;
	sql_subtype *tpe3;
	sql_subtype *res;
	int nr;
} sql_func;

#define new_subtype(type_name, size, digits) \
		sql_create_subtype( sql_bind_type(type_name), size, digits)
extern sql_subtype *sql_create_subtype( sql_type *t, int s, int d );
extern sql_subtype *sql_dup_subtype( sql_subtype *t );

extern int subtype_cmp( sql_subtype *t1, sql_subtype *t2);

extern sql_type *sql_bind_type(char *name);

extern sql_type *sql_create_type(char *sqlname, char *name );

extern sql_aggr *sql_bind_aggr(char *name, sql_subtype *type);
extern sql_aggr *sql_create_aggr(char *name, char *imp, char *tpe, char *res);

extern sql_func *sql_bind_func(char *name, sql_subtype *tp1, sql_subtype *tp2, sql_subtype *tp3);
extern sql_func *sql_bind_func_result(char *name, 
				sql_subtype *tp1, sql_subtype *tp2, sql_subtype *tp3, sql_subtype *res);

extern sql_func *sql_create_func(char *name, char *imp,
			     	char *tpe1, char *tpe2, char *tp3, char *res);

extern void types_init(int debug);
extern void types_exit();

extern void types_export(stream *s);

extern void sql_new_type( char *sqlname, char *name );
extern void sql_new_aggr( char *name, char *imp, char *tpe, char *res );
extern void sql_new_func( char *name, char *imp,
		      char *tpe1, char *tpe2, char *tp3, char *res );

#endif /* TYPES_H */
