/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0.  If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 *
 * Copyright 1997 - July 2008 CWI, August 2008 - 2019 MonetDB B.V.
 */

#ifndef SQL_TYPES_H
#define SQL_TYPES_H

#include "sql_mem.h"
#include "sql_list.h"
#include "sql_string.h"
#include "sql_catalog.h"
#include "sql_storage.h"
#include "stream.h"

extern list *aliases;
extern list *types;
extern list *aggrs;
extern list *funcs;

extern int bits2digits(int b);
extern int digits2bits(int d);

extern int sql_type_convert(int form, int to); /* return 1, convert possible but it's a down cast, 2 convert possible can be done savely */
extern bool is_commutative(const char *fnm); 	/* return true if commutative */

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
extern char *subtype2string2(sql_subtype *tpe);

extern sql_arg *sql_create_arg(sql_allocator *sa, const char *name, sql_subtype *t, char inout);
extern sql_arg *arg_dup(sql_allocator *sa, sql_arg *a);

extern sql_subaggr *sql_bind_aggr(sql_allocator *sa, sql_schema *s, const char *name, sql_subtype *type);
extern sql_subaggr *sql_bind_aggr_(sql_allocator *sa, sql_schema *s, const char *name, list *types);
extern sql_subaggr *sql_bind_member_aggr(sql_allocator *sa, sql_schema *s, const char *name, sql_subtype *tp, int nrargs);
extern sql_subaggr *sql_find_aggr(sql_allocator *sa, sql_schema *s, const char *name);
extern int subaggr_cmp( sql_subaggr *a1, sql_subaggr *a2);

extern int subfunc_cmp( sql_subfunc *f1, sql_subfunc *f2);
extern sql_subfunc *sql_find_func_by_name(sql_allocator *sa, sql_schema *s, const char *name, int nrargs, int type);
extern sql_subfunc *sql_find_func(sql_allocator *sa, sql_schema *s, const char *name, int nrargs, int type, sql_subfunc *prev);
extern list *sql_find_funcs(sql_allocator *sa, sql_schema *s, const char *name, int nrargs, int type);
extern sql_subfunc *sql_bind_member(sql_allocator *sa, sql_schema *s, const char *name, sql_subtype *tp, int nrargs, sql_subfunc *prev);
extern sql_subfunc *sql_bind_func(sql_allocator *sa, sql_schema *s, const char *name, sql_subtype *tp1, sql_subtype *tp2, int type);
extern sql_subfunc *sql_bind_func3(sql_allocator *sa, sql_schema *s, const char *name, sql_subtype *tp1, sql_subtype *tp2, sql_subtype *tp3, int type);
extern sql_subfunc *sql_bind_func_result(sql_allocator *sa, sql_schema *s, const char *name, sql_subtype *tp1, sql_subtype *tp2, sql_subtype *res);
extern sql_subfunc *sql_bind_func_result3(sql_allocator *sa, sql_schema *s, const char *name, sql_subtype *tp1, sql_subtype *tp2, sql_subtype *tp3, sql_subtype *res);

extern sql_subfunc *sql_bind_func_(sql_allocator *sa, sql_schema *s, const char *name, list *ops, int type);

extern sql_subfunc* sql_dup_subfunc(sql_allocator *sa, sql_func *f, list *ops, sql_subtype *member);

extern char *sql_func_imp(sql_func *f);
extern char *sql_func_mod(sql_func *f);
extern int is_sqlfunc(sql_func *f);

extern void types_init(sql_allocator *sa, int debug);

#endif /* SQL_TYPES_H */
