/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0.  If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 *
 * Copyright 1997 - July 2008 CWI, August 2008 - 2018 MonetDB B.V.
 */

#ifndef _REL_SCHEMA_H_
#define _REL_SCHEMA_H_

#include "sql_list.h"
#include "sql_symbol.h"

extern sql_rel *rel_schemas(mvc *sql, symbol *sym);

extern sql_rel *rel_create_table(mvc *sql, sql_schema *ss, int temp, const char *sname, const char *name, symbol *table_elements_or_subquery, int commit_action, const char *loc, int if_not_exists);
extern sql_rel *rel_list(sql_allocator *sa, sql_rel *l, sql_rel *r);
extern sql_table * mvc_create_table_as_subquery( mvc *sql, sql_rel *sq, sql_schema *s, const char *tname, dlist *column_spec, int temp, int commit_action );

#endif /*_REL_SCHEMA_H_*/
