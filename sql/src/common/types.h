#ifndef TYPES_H
#define TYPES_H

#include "list.h"
#include <stream.h>

typedef struct sql_type {
	char *sqlname;
	char *name;
	struct sql_type *cast;
	int nr;
} sql_type;

typedef struct sql_aggr {
	char *name;
	char *imp;
	sql_type *tpe;
	sql_type *res;
	int nr;
} sql_aggr;

typedef struct sql_func {
	char *name;
	char *imp;
	sql_type *tpe1;
	sql_type *tpe2;
	sql_type *tpe3;
	sql_type *res;
	int nr;
} sql_func;

extern sql_type *sql_bind_type(char *name);
extern sql_type *sql_create_type(char *sqlname, char *name, char *cast);

extern sql_aggr *sql_bind_aggr(char *name, char *type);
extern sql_aggr *sql_create_aggr(char *name, char *imp, char *tpe, char *res);

extern sql_func *sql_bind_func(char *name, char *tp1, char *tp2, char *tp3);
extern sql_func *sql_bind_func_result(char *name, 
				char *tp1, char *tp2, char *tp3, char *res);

extern sql_func *sql_create_func(char *name, char *imp,
			     	char *tpe1, char *tpe2, char *tp3, char *res);

extern void types_init(int debug);
extern void types_exit();

extern void types_export(stream *s);

extern void sql_new_type( char *sqlname, char *name, char *cast );
extern void sql_new_aggr( char *name, char *imp, char *tpe, char *res );
extern void sql_new_func( char *name, char *imp,
		      char *tpe1, char *tpe2, char *tp3, char *res );

#endif /* TYPES_H */
