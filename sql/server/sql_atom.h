/*
 * SPDX-License-Identifier: MPL-2.0
 *
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0.  If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 *
 * Copyright 2024, 2025 MonetDB Foundation;
 * Copyright August 2008 - 2023 MonetDB B.V.;
 * Copyright 1997 - July 2008 CWI.
 */

#ifndef _SQL_ATOM_H_
#define _SQL_ATOM_H_

#include "sql_mem.h"
#include "sql_types.h"

#define atom_null(a) (((atom*)a)->isnull)

extern void atom_init( atom *a );
extern atom *atom_bool( allocator *sa, sql_subtype *tpe, bit t);
#ifdef HAVE_HGE
extern atom *atom_int( allocator *sa, sql_subtype *tpe, hge val);
#else
extern atom *atom_int( allocator *sa, sql_subtype *tpe, lng val);
#endif
extern atom *atom_float( allocator *sa, sql_subtype *tpe, dbl val);
extern atom *atom_string( allocator *sa, sql_subtype *tpe, const char *val);
extern atom *atom_general( allocator *sa, sql_subtype *tpe, const char *val, long tz_offset);
#ifdef HAVE_HGE
extern atom *atom_dec( allocator *sa, sql_subtype *tpe, hge val);
#else
extern atom *atom_dec( allocator *sa, sql_subtype *tpe, lng val);
#endif
extern atom *atom_ptr( allocator *sa, sql_subtype *tpe, void *v);
extern atom *atom_general_ptr( allocator *sa, sql_subtype *tpe, void *v);

extern unsigned int atom_num_digits(atom *a);

/* cast atom a to type tp (success returns not NULL, fail returns NULL) */
extern atom *atom_cast(allocator *sa, atom *a, sql_subtype *tp);
extern atom *atom_cast_inplace(atom *a, sql_subtype *tp);

extern char *atom2string(allocator *sa, atom *a);
extern char *atom2sql(allocator *sa, atom *a, int timezone);
extern sql_subtype *atom_type(atom *a);
extern atom *atom_set_type(allocator *sa, atom *a, sql_subtype *t);

#ifdef HAVE_HGE
extern hge atom_get_int(atom *a);
#else
extern lng atom_get_int(atom *a);
#endif

extern int atom_cmp(atom *a1, atom *a2);

extern atom *atom_absolute(allocator *sa, atom *a);
extern atom *atom_neg(allocator *sa, atom *a);
extern atom *atom_add(allocator *sa, atom *a1, atom *a2);
extern atom *atom_sub(allocator *sa, atom *a1, atom *a2);
extern atom *atom_mul(allocator *sa, atom *a1, atom *a2);
extern atom *atom_div(allocator *sa, atom *a1, atom *a2);
extern atom *atom_inc(allocator *sa, atom *a);

extern int atom_is_true(atom *a);
extern int atom_is_false(atom *a);
extern int atom_is_zero(atom *a);
extern int atom_is_one(atom *a);

extern unsigned int atom_digits(atom *a);

#ifdef HAVE_HGE
#define MAX_SCALE 39
extern const hge scales[MAX_SCALE];
#else
#define MAX_SCALE 19
extern const lng scales[MAX_SCALE];
#endif

extern atom *atom_zero_value(allocator *sa, sql_subtype *tpe);
extern atom *atom_max_value(allocator *sa, sql_subtype *tpe);

#endif /* _SQL_ATOM_H_ */
