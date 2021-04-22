/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0.  If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 *
 * Copyright 1997 - July 2008 CWI, August 2008 - 2021 MonetDB B.V.
 */

#ifndef _REL_UPDATES_H_
#define _REL_UPDATES_H_

#include "sql_list.h"
#include "sql_symbol.h"
#include "sql_mvc.h"
#include "sql_relation.h"
#include "sql_query.h"

extern sql_table *get_table(sql_rel *t); /* needed for rel_read at rel_dump */
extern sql_rel *rel_update(mvc *sql, sql_rel *t, sql_rel *uprel, sql_exp **updates, list *exps);

extern sql_rel *rel_insert(mvc *sql, sql_rel *t, sql_rel *inserts);
extern sql_rel *rel_delete(sql_allocator *sa, sql_rel *t, sql_rel *deletes);
extern sql_rel *rel_truncate(sql_allocator *sa, sql_rel *t, int restart_sequences, int drop_action);

extern sql_exp *rel_parse_val(mvc *m, sql_schema *s, char *query, sql_subtype *tpe, char emode, sql_rel *from);

extern sql_rel *rel_updates(sql_query *query, symbol *sym);

extern sql_exp *update_check_column(mvc *sql, sql_table *t, sql_column *c, sql_exp *v, sql_rel *r, char *cname, const char *action);
extern sql_table *insert_allowed(mvc *sql, sql_table *t, char *tname, char *op, char *opname);
extern sql_table *update_allowed(mvc *sql, sql_table *t, char *tname, char *op, char *opname, int is_delete);

#endif /*_REL_UPDATES_H_*/
