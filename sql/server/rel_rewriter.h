/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0.  If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 *
 * Copyright 1997 - July 2008 CWI, August 2008 - 2020 MonetDB B.V.
 */

#ifndef _REL_REWRITER_H_
#define _REL_REWRITER_H_

#include "sql_relation.h"
#include "sql_mvc.h"

#define is_ifthenelse_func(sf) (strcmp((sf)->func->base.name, "ifthenelse") == 0)
#define is_isnull_func(sf) (strcmp((sf)->func->base.name, "isnull") == 0)
#define is_not_func(sf) (strcmp((sf)->func->base.name, "not") == 0) 

extern sql_exp *rewrite_simplify_exp(mvc *sql, sql_rel *rel, sql_exp *e, int depth, int *changes);
extern sql_rel *rewrite_simplify(mvc *sql, sql_rel *rel, int *changes);
extern sql_rel *rel_remove_empty_select(mvc *sql, sql_rel *rel, int *changes);

extern sql_exp *exp_push_down(mvc *sql, sql_exp *e, sql_rel *f, sql_rel *t);

#endif /*_REL_REWRITER_H_*/
