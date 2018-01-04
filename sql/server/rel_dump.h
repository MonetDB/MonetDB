/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0.  If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 *
 * Copyright 1997 - July 2008 CWI, August 2008 - 2018 MonetDB B.V.
 */

#ifndef _REL_DUMP_H_
#define _REL_DUMP_H_

#include "sql_relation.h"
#include "sql_mvc.h"

extern void rel_print_(mvc *sql, stream  *fout, sql_rel *rel, int depth, list *refs, int decorate);
extern void rel_print_refs(mvc *sql, stream* fout, sql_rel *rel, int depth, list *refs, int decorate);
extern const char *op2string(operator_type op);

extern sql_rel *rel_read(mvc *sql, char *ra, int *pos, list *refs);

#endif /*_REL_DUMP_H_*/
