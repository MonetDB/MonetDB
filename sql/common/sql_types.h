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

#ifndef SQL_TYPES_H
#define SQL_TYPES_H

#include <sql_mem.h>
#include <sql_list.h>
#include <sql_string.h>
#include <sql_catalog.h>
#include <sql_storage.h>
#include <stream.h>

#define EC_MAX 		14
#define EC_ANY	 	0
#define EC_TABLE 	1
#define EC_BIT 		2
#define EC_CHAR 	3
#define EC_STRING 	4
#define EC_BLOB		5
#define EC_VARCHAR(e)	(e==EC_CHAR||e==EC_STRING)

#define EC_NUM 		6
#define EC_INTERVAL 	7
#define EC_DEC 		8
#define EC_FLT 		9
#define EC_NUMBER(e)	(e==EC_NUM||e==EC_INTERVAL||e==EC_DEC||e==EC_FLT)

#define EC_TIME		10
#define EC_DATE		11
#define EC_TIMESTAMP	12
#define EC_TEMP(e)	(e==EC_TIME||e==EC_DATE||e==EC_TIMESTAMP)
#define EC_EXTERNAL	13

#define EC_FIXED(e)	(e==EC_BIT||e==EC_CHAR||\
			 e==EC_NUM||e==EC_INTERVAL||e==EC_DEC||EC_TEMP(e))

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

extern int sql_type_convert(int form, int to); /* return 1, convert possible but its a down cast, 2 convert possible can be done savely */
extern int is_commutative(char *fnm); 	/* return 1, if commutative */

extern char *sql_bind_alias(char *alias);

extern int sql_find_subtype(sql_subtype *res, char *name, unsigned int digits, unsigned int scale);
extern sql_subtype *sql_find_numeric(sql_subtype *r, int localtype, unsigned int digits);

extern sql_subtype *sql_bind_subtype(char *name, unsigned int digits, unsigned int scale);
extern sql_subtype *sql_bind_localtype(char *name);
extern sql_subtype *sql_create_subtype(sql_type *t, unsigned int s, unsigned int d);
extern void sql_init_subtype(sql_subtype *res, sql_type *t, unsigned int digits, unsigned int scale);
extern void sql_subtype_destroy(sql_subtype *t);
extern char *sql_subtype_string(sql_subtype *t);

extern int type_cmp(sql_type *t1, sql_type *t2);
extern int subtype_cmp(sql_subtype *t1, sql_subtype *t2);
extern int is_subtype(sql_subtype *t1, sql_subtype *t2);
extern char *subtype2string(sql_subtype *t);

extern sql_type *sql_create_type(char *sqlname, unsigned int digits, unsigned int scale, unsigned char radix, unsigned char eclass, char *name);
extern void type_destroy(sql_type *t);

extern sql_arg *arg_dup(sql_arg *a);
extern void arg_destroy(sql_arg *a);

extern sql_subaggr *sql_bind_aggr(sql_allocator *sa, sql_schema *s, char *name, sql_subtype *type);
extern sql_subaggr *sql_find_aggr(sql_allocator *sa, sql_schema *s, char *name);
extern sql_func *sql_create_aggr(char *name, char *mod, char *imp, sql_type *tpe, sql_type *res);
extern int subaggr_cmp( sql_subaggr *a1, sql_subaggr *a2);

extern int subfunc_cmp( sql_subfunc *f1, sql_subfunc *f2);
extern sql_subfunc *sql_find_func(sql_allocator *sa, sql_schema *s, char *name, int nrargs);
extern sql_subfunc *sql_bind_member(sql_allocator *sa, sql_schema *s, char *name, sql_subtype *tp, int nrargs);
extern sql_subfunc *sql_bind_func(sql_allocator *sa, sql_schema *s, char *name, sql_subtype *tp1, sql_subtype *tp2);
extern sql_subfunc *sql_bind_func3(sql_allocator *sa, sql_schema *s, char *name, sql_subtype *tp1, sql_subtype *tp2, sql_subtype *tp3);
extern sql_subfunc *sql_bind_func_result(sql_allocator *sa, sql_schema *s, char *name, sql_subtype *tp1, sql_subtype *tp2, sql_subtype *res);
extern sql_subfunc *sql_bind_func_result3(sql_allocator *sa, sql_schema *s, char *name, sql_subtype *tp1, sql_subtype *tp2, sql_subtype *tp3, sql_subtype *res);

extern sql_subfunc *sql_bind_func_(sql_allocator *sa, sql_schema *s, char *name, list *ops);
extern sql_subfunc *sql_bind_func_result_(sql_allocator *sa, sql_schema *s, char *name, list *ops, sql_subtype *res);
extern sql_subfunc *sql_bind_proc(sql_allocator *sa, sql_schema *s, char *name, list *ops);

extern void func_destroy(sql_func *t);
extern sql_func *sql_create_func(char *name, char *mod, char *imp, sql_type *tpe1, sql_type *tpe2, sql_type *res, int scale_fixing);
extern sql_func *sql_create_funcSE(char *name, char *mod, char *imp, sql_type *tpe1, sql_type *tpe2, sql_type *res, int scale_fixing);
extern sql_func *sql_create_func3(char *name, char *mod, char *imp, sql_type *tpe1, sql_type *tpe2, sql_type *tpe3, sql_type *res, int scale_fixing);
extern sql_func *sql_create_func4(char *name, char *mod, char *imp, sql_type *tpe1, sql_type *tpe2, sql_type *tpe3, sql_type *tpe4, sql_type *res, int scale_fixing);

extern sql_func *sql_create_func_(char *name, char *mod, char *imp, list *ops, sql_subtype *res, bit side_effect, bit aggr, int fix_scale);

extern sql_func *sql_create_sqlfunc(char *name, char *imp, list *ops, sql_subtype *res);

extern char *sql_func_imp(sql_func *f);
extern char *sql_func_mod(sql_func *f);
extern int is_sqlfunc(sql_func *f);

extern void types_init(int debug);
extern void types_exit(void);

#endif /* SQL_TYPES_H */
