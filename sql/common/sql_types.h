/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0.  If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 *
 * Copyright 1997 - July 2008 CWI, August 2008 - 2018 MonetDB B.V.
 */

#ifndef SQL_TYPES_H
#define SQL_TYPES_H

#include "sql_mem.h"
#include "sql_list.h"
#include "sql_string.h"
#include "sql_catalog.h"
#include "sql_storage.h"
#include "stream.h"

#define EC_MAX 		17
#define EC_ANY	 	0
#define IS_ANY(e)	(e==EC_ANY)
#define EC_TABLE 	1
#define EC_BIT 		2
#define EC_CHAR 	3
#define EC_STRING 	4
#define EC_BLOB		5
#define EC_VARCHAR(e)	(e==EC_CHAR||e==EC_STRING)

#define EC_POS 		6
#define EC_NUM 		7
#define EC_MONTH 	8
#define EC_SEC	 	9
#define EC_DEC 		10
#define EC_FLT 		11
#define EC_INTERVAL(e)	(e==EC_MONTH||e==EC_SEC)
#define EC_NUMBER(e)	(e==EC_POS||e==EC_NUM||EC_INTERVAL(e)||e==EC_DEC||e==EC_FLT)
#define EC_COMPUTE(e)	(e==EC_NUM||e==EC_FLT)
#define EC_BOOLEAN(e)	(e==EC_BIT||e==EC_NUM||e==EC_FLT)

#define EC_TIME		12
#define EC_DATE		13
#define EC_TIMESTAMP	14
#define EC_TEMP(e)	(e==EC_TIME||e==EC_DATE||e==EC_TIMESTAMP)
#define EC_GEOM		15
#define EC_EXTERNAL	16

#define EC_TEMP_FRAC(e)	(e==EC_TIME||e==EC_TIMESTAMP)

#define EC_FIXED(e)	(e==EC_BIT||e==EC_CHAR||\
			 e==EC_POS||e==EC_NUM||EC_INTERVAL(e)||e==EC_DEC||EC_TEMP(e))

#define has_tz(e,n)	(EC_TEMP(e) && \
			((e == EC_TIME && strcmp(n, "timetz") == 0) || \
		 	(e == EC_TIMESTAMP && strcmp(n, "timestamptz") == 0)) )

#define type_has_tz(t)	has_tz((t)->type->eclass, (t)->type->sqlname)

extern list *aliases;
extern list *types;
extern list *aggrs;
extern list *funcs;

extern int bits2digits(int b);
extern int digits2bits(int d);

extern int sql_type_convert(int form, int to); /* return 1, convert possible but it's a down cast, 2 convert possible can be done savely */
extern int is_commutative(const char *fnm); 	/* return 1, if commutative */

extern char *sql_bind_alias(const char *alias);

extern int sql_find_subtype(sql_subtype *res, const char *name, unsigned int digits, unsigned int scale);
extern sql_subtype *sql_find_numeric(sql_subtype *r, int localtype, unsigned int digits);

extern sql_subtype *sql_bind_subtype(sql_allocator *sa, const char *name, unsigned int digits, unsigned int scale);
extern sql_subtype *sql_bind_localtype(const char *name);
extern sql_subtype *sql_create_subtype(sql_allocator *sa, sql_type *t, unsigned int s, unsigned int d);
extern void sql_init_subtype(sql_subtype *res, sql_type *t, unsigned int digits, unsigned int scale);
extern char *sql_subtype_string(sql_subtype *t);

extern int type_cmp(sql_type *t1, sql_type *t2);
extern int subtype_cmp(sql_subtype *t1, sql_subtype *t2);
extern int arg_subtype_cmp(sql_arg *a, sql_subtype *t);
extern int is_subtype(sql_subtype *t1, sql_subtype *t2);
extern char *subtype2string(sql_subtype *t);

extern sql_type *sql_create_type(sql_allocator *sa, const char *sqlname, unsigned int digits, unsigned int scale, unsigned char radix, unsigned char eclass, const char *name);
extern void type_destroy(sql_type *t);

extern sql_arg *sql_create_arg(sql_allocator *sa, const char *name, sql_subtype *t, char inout);
extern sql_arg *arg_dup(sql_allocator *sa, sql_arg *a);

extern sql_subaggr *sql_bind_aggr(sql_allocator *sa, sql_schema *s, const char *name, sql_subtype *type);
extern sql_subaggr *sql_bind_aggr_(sql_allocator *sa, sql_schema *s, const char *name, list *types);
extern sql_subaggr *sql_bind_member_aggr(sql_allocator *sa, sql_schema *s, const char *name, sql_subtype *tp, int nrargs);
extern sql_subaggr *sql_find_aggr(sql_allocator *sa, sql_schema *s, const char *name);
extern sql_func *sql_create_aggr(sql_allocator *sa, const char *name, const char *mod, const char *imp, sql_type *tpe, sql_type *res);
extern sql_func *sql_create_aggr2(sql_allocator *sa, const char *name, const char *mod, const char *imp, sql_type *tp1, sql_type *tp2, sql_type *res);
extern int subaggr_cmp( sql_subaggr *a1, sql_subaggr *a2);

extern int subfunc_cmp( sql_subfunc *f1, sql_subfunc *f2);
extern sql_subfunc *sql_find_func(sql_allocator *sa, sql_schema *s, const char *name, int nrargs, int type, sql_subfunc *prev);
extern list *sql_find_funcs(sql_allocator *sa, sql_schema *s, const char *name, int nrargs, int type);
extern sql_subfunc *sql_bind_member(sql_allocator *sa, sql_schema *s, const char *name, sql_subtype *tp, int nrargs, sql_subfunc *prev);
extern sql_subfunc *sql_bind_func(sql_allocator *sa, sql_schema *s, const char *name, sql_subtype *tp1, sql_subtype *tp2, int type);
extern sql_subfunc *sql_bind_func3(sql_allocator *sa, sql_schema *s, const char *name, sql_subtype *tp1, sql_subtype *tp2, sql_subtype *tp3, int type);
extern sql_subfunc *sql_bind_func_result(sql_allocator *sa, sql_schema *s, const char *name, sql_subtype *tp1, sql_subtype *tp2, sql_subtype *res);
extern sql_subfunc *sql_bind_func_result3(sql_allocator *sa, sql_schema *s, const char *name, sql_subtype *tp1, sql_subtype *tp2, sql_subtype *tp3, sql_subtype *res);

extern sql_subfunc *sql_bind_func_(sql_allocator *sa, sql_schema *s, const char *name, list *ops, int type);
extern sql_subfunc *sql_bind_func_result_(sql_allocator *sa, sql_schema *s, const char *name, list *ops, sql_subtype *res);

extern sql_func *sql_create_func(sql_allocator *sa, const char *name, const char *mod, const char *imp, sql_type *tpe1, sql_type *tpe2, sql_type *res, int scale_fixing);
extern sql_func *sql_create_funcSE(sql_allocator *sa, const char *name, const char *mod, const char *imp, sql_type *tpe1, sql_type *tpe2, sql_type *res, int scale_fixing);
extern sql_func *sql_create_func3(sql_allocator *sa, const char *name, const char *mod, const char *imp, sql_type *tpe1, sql_type *tpe2, sql_type *tpe3, sql_type *res, int scale_fixing);
extern sql_func *sql_create_func4(sql_allocator *sa, const char *name, const char *mod, const char *imp, sql_type *tpe1, sql_type *tpe2, sql_type *tpe3, sql_type *tpe4, sql_type *res, int scale_fixing);

extern sql_func *sql_create_func_(sql_allocator *sa, const char *name, const char *mod, const char *imp, list *ops, sql_arg *res, bit side_effect, int type, int fix_scale);

extern sql_func *sql_create_sqlfunc(sql_allocator *sa, const char *name, const char *imp, list *ops, sql_arg *res);
extern sql_subfunc* sql_dup_subfunc(sql_allocator *sa, sql_func *f, list *ops, sql_subtype *member);

extern char *sql_func_imp(sql_func *f);
extern char *sql_func_mod(sql_func *f);
extern int is_sqlfunc(sql_func *f);

extern void types_init(sql_allocator *sa, int debug);

#endif /* SQL_TYPES_H */
