/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0.  If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 *
 * Copyright 1997 - July 2008 CWI, August 2008 - 2021 MonetDB B.V.
 */

#ifndef _REL_OPTIMIZER_H_
#define _REL_OPTIMIZER_H_

#include "sql_relation.h"
#include "sql_mvc.h"

extern sql_rel *rel_optimizer(mvc *sql, sql_rel *rel, int value_based_opt, int storage_based_opt);

extern int exp_joins_rels(sql_exp *e, list *rels);

extern void *name_find_column(sql_rel *rel, const char *rname, const char *name, int pnr, sql_rel **bt);
extern int exps_unique(mvc *sql, sql_rel *rel, list *exps);

extern sql_rel *rel_dce(mvc *sql, sql_rel *rel);

#endif /*_REL_OPTIMIZER_H_*/
