/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0.  If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 *
 * Copyright 1997 - July 2008 CWI, August 2008 - 2021 MonetDB B.V.
 */

#ifndef SQL_TYPES_H
#define SQL_TYPES_H

#include "sql_mem.h"
#include "sql_list.h"
#include "sql_string.h"
#include "sql_catalog.h"
#include "sql_storage.h"
#include "sql_backend.h"
#include "stream.h"

extern list *aliases;
extern list *types;
extern list *funcs;

extern unsigned int bits2digits(unsigned int b);
extern unsigned int digits2bits(unsigned int d);
extern unsigned int type_digits_to_char_digits(sql_subtype *t);

extern int sql_type_convert(int form, int to); /* return 1, convert possible but it's a down cast, 2 convert possible can be done savely */
extern bool is_commutative(const char *fnm); 	/* return true if commutative */

extern char *sql_bind_alias(const char *alias);

extern sql_subtype *arg_type( sql_arg *a);

extern int sql_find_subtype(sql_subtype *res, const char *name, unsigned int digits, unsigned int scale);
extern sql_subtype *sql_find_numeric(sql_subtype *r, int localtype, unsigned int digits);

extern sql_subtype *sql_bind_subtype(sql_allocator *sa, const char *name, unsigned int digits, unsigned int scale);
extern sql_subtype *sql_bind_localtype(const char *name);
extern sql_subtype *sql_create_subtype(sql_allocator *sa, sql_type *t, unsigned int s, unsigned int d);
sql_export void sql_init_subtype(sql_subtype *res, sql_type *t, unsigned int digits, unsigned int scale);

extern int type_cmp(sql_type *t1, sql_type *t2);
extern int subtype_cmp(sql_subtype *t1, sql_subtype *t2);
extern int arg_subtype_cmp(sql_arg *a, sql_subtype *t);
extern int is_subtype(sql_subtype *t1, sql_subtype *t2);

extern char *sql_subtype_string(sql_allocator *sa, sql_subtype *t);
extern char *subtype2string2(sql_allocator *sa, sql_subtype *tpe);

extern sql_arg *sql_create_arg(sql_allocator *sa, const char *name, sql_subtype *t, char inout);

extern int subfunc_cmp(sql_subfunc *f1, sql_subfunc *f2);
extern sql_subfunc *sql_dup_subfunc(sql_allocator *sa, sql_func *f, list *ops, sql_subtype *member);

extern char *sql_func_imp(sql_func *f);
extern char *sql_func_mod(sql_func *f);
extern int is_sqlfunc(sql_func *f);

extern void types_init(sql_allocator *sa);

#endif /* SQL_TYPES_H */
