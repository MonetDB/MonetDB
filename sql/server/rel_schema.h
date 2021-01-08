/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0.  If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 *
 * Copyright 1997 - July 2008 CWI, August 2008 - 2021 MonetDB B.V.
 */

#ifndef _REL_SCHEMA_H_
#define _REL_SCHEMA_H_

#include "sql_list.h"
#include "sql_symbol.h"

extern sql_rel *rel_schemas(sql_query *query, symbol *sym);

extern sql_rel *rel_table(mvc *sql, int cat_type, const char *sname, sql_table *t, int nr);

extern sql_rel *rel_create_table(sql_query *query, int temp,
				 const char *sname, const char *name, bool global,
				 symbol *table_elements_or_subquery,
				 int commit_action, const char *loc,
				 const char *username, const char *passwd,
				 bool pw_encrypted, symbol* partition_def, int if_not_exists);

extern sql_rel *rel_list(sql_allocator *sa, sql_rel *l, sql_rel *r);
extern sql_table *mvc_create_table_as_subquery(mvc *sql, sql_rel *sq, sql_schema *s, const char *tname, dlist *column_spec, int temp, int commit_action, const char* action);

#endif /*_REL_SCHEMA_H_*/
