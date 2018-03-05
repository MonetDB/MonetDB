/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0.  If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 *
 * Copyright 1997 - July 2008 CWI, August 2008 - 2018 MonetDB B.V.
 */

#ifndef _REL_UPDATES_H_
#define _REL_UPDATES_H_

#include "sql_list.h"
#include "sql_symbol.h"
#include "sql_mvc.h"
#include "sql_relation.h"

extern sql_exp **table_update_array(mvc *sql, sql_table *t);
extern sql_rel *rel_update(mvc *sql, sql_rel *t, sql_rel *uprel, sql_exp **updates, list *exps);

extern sql_rel *rel_insert(mvc *sql, sql_rel *t, sql_rel *inserts);
extern sql_rel *rel_delete(sql_allocator *sa, sql_rel *t, sql_rel *deletes);
extern sql_rel *rel_truncate(sql_allocator *sa, sql_rel *t, int drop_action, int check_identity);

extern sql_exp * rel_parse_val(mvc *m, char *query, char emode);

extern sql_rel *rel_updates(mvc *sql, symbol *sym);

#endif /*_REL_UPDATES_H_*/
