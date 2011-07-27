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

#ifndef _SQL_ATOM_H_
#define _SQL_ATOM_H_

#include <sql_mem.h>
#include <sql_types.h>

typedef struct atom {
	int isnull;
	sql_subtype tpe;
	ValRecord data;
	dbl d;
} atom;

#define atom_null(a) a->isnull

extern atom *atom_bool( sql_allocator *sa, sql_subtype *tpe, bit t);
extern atom *atom_int( sql_allocator *sa, sql_subtype *tpe, lng val);
extern atom *atom_float( sql_allocator *sa, sql_subtype *tpe, double val);
extern atom *atom_string( sql_allocator *sa, sql_subtype *tpe, char *val);
extern atom *atom_general( sql_allocator *sa, sql_subtype *tpe, char *val);
extern atom *atom_dec( sql_allocator *sa, sql_subtype *tpe, lng val, double dval);
extern atom *atom_ptr( sql_allocator *sa, sql_subtype *tpe, void *v);

extern int atom_neg( atom *a );
extern unsigned int atom_num_digits( atom *a );

/* duplicate atom */
extern atom *atom_dup( sql_allocator *sa, atom *a);

/* cast atom a to type tp (success == 1, fail == 0) */
extern int atom_cast(atom *a, sql_subtype *tp);

extern char *atom2string(sql_allocator *sa, atom *a);
extern char *atom2sql(atom *a);
extern sql_subtype *atom_type(atom *a);

extern void atom_dump(atom *a, stream *s);

extern lng atom_get_int(atom *a);

extern int atom_cmp(atom *a1, atom *a2);
#endif /* _SQL_ATOM_H_ */

