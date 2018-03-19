/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0.  If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 *
 * Copyright 1997 - July 2008 CWI, August 2008 - 2018 MonetDB B.V.
 */

#ifndef _REL_PSM_H_
#define _REL_PSM_H_

#include "sql_list.h"
#include "sql_relation.h"
#include "sql_symbol.h"
#include "sql_mvc.h"

extern sql_func *resolve_func( mvc *sql, sql_schema *s, const char *name, dlist *typelist, int type, char *op, int if_exists);
extern sql_rel *rel_psm(mvc *sql, symbol *sym);
extern sql_rel *rel_select_with_into( mvc *sql, symbol *sq);

extern sql_rel *rel_psm_stmt(sql_allocator *sa, sql_exp *e);

#endif /*_REL_PSM_H_*/
