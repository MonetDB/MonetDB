#ifndef TYPES_H
#define TYPES_H

#include "list.h"
#include <stream.h>

sql_export list *types;

#define SCALE_NONE	0
#define SCALE_FIX	1
#define SCALE_NOFIX	2
#define SCALE_ADD	3
#define SCALE_SUB	4

#define max(i1,i2) ((i1)<(i2))?(i2):(i1)

typedef struct sql_type {
	char *sqlname;
	char *name;
	unsigned int digits;
	unsigned int scale; /* indicates how scale is used in functions */
	unsigned int radix; 
	int nr;
	int localtype; 	/* localtype, need for coersions */
} sql_type;

typedef struct sql_subtype {
	sql_type *type;
	unsigned int digits;
	unsigned int scale;
} sql_subtype;

typedef struct sql_aggr {
	char *name;
	char *imp;
	sql_subtype *tpe;
	sql_subtype *res;
	int nr;
} sql_aggr;

typedef struct sql_subaggr {
	sql_aggr *aggr;
	sql_subtype res;
} sql_subaggr;

/* sql_func need type transform rules 
 * types are equal if underlying types are equal +
	scale is equal
 * if types do not mach we try type conversions
 * 	which means for simple 1 arg functions
 *
 *
 */

typedef struct sql_func {
	char *name;
	char *imp;
	sql_subtype *tpe1;
	sql_subtype *tpe2;
	list *ops; /* 2 + param list */
	sql_subtype *res;
		   /* res->scale
		      SCALE_NOFIX/SCALE_NONE => nothing 
		      SCALE_FIX => input scale fixing, 
		      SCALE_ADD => leave inputs as is and do add scales 
		      SCALE_SUB => first input scale, fix with second scale
				   result scale is equal to first input
		    */
	int nr;
} sql_func;

typedef struct sql_subfunc {
	sql_func *func;
	sql_subtype res;
} sql_subfunc;

sql_export sql_subtype *sql_bind_subtype( char *name, int digits, int scale );
extern sql_subtype *sql_bind_localtype( char *name );
extern sql_subtype *sql_create_subtype( sql_type *t, int s, int d );
sql_export sql_subtype *sql_dup_subtype( sql_subtype *t );
sql_export void sql_subtype_destroy( sql_subtype *t );

extern int subtype_cmp( sql_subtype *t1, sql_subtype *t2);

sql_export sql_type *sql_create_type(char *sqlname, int digits, 
					int scale, int radix, char *name );

extern sql_subaggr *sql_bind_aggr(char *name, sql_subtype *type);
sql_export sql_aggr *sql_create_aggr(char *name, char *imp, 
				sql_type *tpe, sql_type *res);

extern sql_subfunc *sql_bind_func(
	char *name, sql_subtype *tp1, sql_subtype *tp2);
extern sql_subfunc *sql_bind_func_result(
	char *name, sql_subtype *tp1, sql_subtype *tp2, sql_subtype *res);

extern sql_subfunc *sql_bind_func_( char *name, list *ops);
extern sql_subfunc *sql_bind_func_result_( char *name, list *ops, sql_subtype *res);

sql_export sql_func *sql_create_func(char *name, char *imp,
			     	sql_type *tpe1, sql_type *tpe2, 
				sql_type *res, int scale_fixing);

sql_export sql_func *sql_create_func_(char *name, char *imp,
				list *ops, sql_subtype *res);

sql_export void parser_init(int debug);
sql_export void parser_exit();

#endif /* TYPES_H */
