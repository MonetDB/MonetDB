/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0.  If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 *
 * Copyright 2008-2015 MonetDB B.V.
 */

#ifndef _REL_DUMP_H_
#define _REL_DUMP_H_

#include "rel_semantic.h"

extern void rel_print(mvc *sql, sql_rel *rel, int depth);
extern void _rel_print(mvc *sql, sql_rel *rel);
extern const char *op2string(operator_type op);

extern str rel2str( mvc *sql, sql_rel *rel);

extern sql_rel *rel_read(mvc *sql, char *ra, int *pos, list *refs);

#endif /*_REL_DUMP_H_*/
