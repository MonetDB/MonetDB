/*
 * SPDX-License-Identifier: MPL-2.0
 *
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0.  If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 *
 * Copyright 2024 MonetDB Foundation;
 * Copyright August 2008 - 2023 MonetDB B.V.;
 * Copyright 1997 - July 2008 CWI.
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

extern list *types;
extern list *funcs;

extern unsigned int bits2digits(unsigned int b);
extern unsigned int digits2bits(unsigned int d);
extern unsigned int type_digits_to_char_digits(sql_subtype *t);

extern int sql_type_convert(int form, int to); /* return 1, convert possible but it's a down cast, 2 convert possible can be done savely */
extern int sql_type_convert_preference(int form, int to);
extern bool is_commutative(const char *sname, const char *fnm); 	/* return true if commutative */

extern sql_subtype *arg_type( sql_arg *a);

sql_export int sql_find_subtype(sql_subtype *res, const char *name, unsigned int digits, unsigned int scale);
extern sql_subtype *sql_find_numeric(sql_subtype *r, int localtype, unsigned int digits);

sql_export sql_subtype *sql_bind_subtype(allocator *sa, const char *name, unsigned int digits, unsigned int scale);
extern sql_subtype *sql_bind_localtype(const char *name);
extern sql_subtype *sql_create_subtype(allocator *sa, sql_type *t, unsigned int s, unsigned int d);
sql_export void sql_init_subtype(sql_subtype *res, sql_type *t, unsigned int digits, unsigned int scale);

extern int type_cmp(sql_type *t1, sql_type *t2);
extern int subtype_cmp(sql_subtype *t1, sql_subtype *t2);
extern int arg_subtype_cmp(sql_arg *a, sql_subtype *t);
extern int is_subtype(sql_subtype *t1, sql_subtype *t2);

extern char *sql_subtype_string(allocator *sa, sql_subtype *t);
extern char *subtype2string2(allocator *sa, sql_subtype *tpe);

extern sql_arg *sql_create_arg(allocator *sa, const char *name, sql_subtype *t, char inout);

extern int subfunc_cmp(sql_subfunc *f1, sql_subfunc *f2);
extern sql_subfunc *sql_dup_subfunc(allocator *sa, sql_func *f, list *ops, sql_subtype *member);
extern sql_subtype *supertype(sql_subtype *super, sql_subtype *r, sql_subtype *i);
extern sql_subtype *cmp_supertype(sql_subtype *super, sql_subtype *r, sql_subtype *i);

extern char *sql_func_imp(sql_func *f);
extern char *sql_func_mod(sql_func *f);

extern void types_init(allocator *sa);

#endif /* SQL_TYPES_H */
