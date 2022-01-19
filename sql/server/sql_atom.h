/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0.  If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 *
 * Copyright 1997 - July 2008 CWI, August 2008 - 2022 MonetDB B.V.
 */

#ifndef _SQL_ATOM_H_
#define _SQL_ATOM_H_

#include "sql_mem.h"
#include "sql_types.h"

#define atom_null(a) (((atom*)a)->isnull)

extern void atom_init( atom *a );
extern atom *atom_bool( sql_allocator *sa, sql_subtype *tpe, bit t);
#ifdef HAVE_HGE
extern atom *atom_int( sql_allocator *sa, sql_subtype *tpe, hge val);
#else
extern atom *atom_int( sql_allocator *sa, sql_subtype *tpe, lng val);
#endif
extern atom *atom_float( sql_allocator *sa, sql_subtype *tpe, dbl val);
extern atom *atom_string( sql_allocator *sa, sql_subtype *tpe, const char *val);
extern atom *atom_general( sql_allocator *sa, sql_subtype *tpe, const char *val);
#ifdef HAVE_HGE
extern atom *atom_dec( sql_allocator *sa, sql_subtype *tpe, hge val);
#else
extern atom *atom_dec( sql_allocator *sa, sql_subtype *tpe, lng val);
#endif
extern atom *atom_ptr( sql_allocator *sa, sql_subtype *tpe, void *v);
extern atom *atom_general_ptr( sql_allocator *sa, sql_subtype *tpe, void *v);

extern unsigned int atom_num_digits( atom *a );

/* cast atom a to type tp (success returns not NULL, fail returns NULL) */
extern atom *atom_cast(sql_allocator *sa, atom *a, sql_subtype *tp);

extern char *atom2string(sql_allocator *sa, atom *a);
extern char *atom2sql(sql_allocator *sa, atom *a, int timezone);
extern sql_subtype *atom_type(atom *a);
extern atom *atom_set_type(sql_allocator *sa, atom *a, sql_subtype *t);

#ifdef HAVE_HGE
extern hge atom_get_int(atom *a);
#else
extern lng atom_get_int(atom *a);
#endif

extern int atom_cmp(atom *a1, atom *a2);

extern atom *atom_neg(sql_allocator *sa, atom *a);
extern atom *atom_add(sql_allocator *sa, atom *a1, atom *a2);
extern atom *atom_sub(sql_allocator *sa, atom *a1, atom *a2);
extern atom *atom_mul(sql_allocator *sa, atom *a1, atom *a2);
extern atom *atom_inc(sql_allocator *sa, atom *a);
extern int atom_is_true(atom *a);
extern int atom_is_false(atom *a);
extern int atom_is_zero(atom *a);

#ifdef HAVE_HGE
#define MAX_SCALE 39
extern const hge scales[MAX_SCALE];
#else
#define MAX_SCALE 19
extern const lng scales[MAX_SCALE];
#endif

extern atom *atom_zero_value(sql_allocator *sa, sql_subtype* tpe);
extern atom *atom_max_value(sql_allocator *sa, sql_subtype *tpe);
#endif /* _SQL_ATOM_H_ */
