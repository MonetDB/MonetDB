/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0.  If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 *
 * Copyright 1997 - July 2008 CWI, August 2008 - 2022 MonetDB B.V.
 */

#ifndef _REL_OPTIMIZER_H_
#define _REL_OPTIMIZER_H_

#include "sql_mvc.h"
#include "sql_relation.h"

/* a single SQL optimizer run */
typedef struct {
	const char *name;
	int nchanges;
	lng time;
} sql_optimizer_run;

/* an optimized SQL query */
typedef struct {
	sql_optimizer_run *runs;
	sql_rel *rel;
} sql_optimized_query;

extern sql_rel *rel_optimizer(mvc *sql, sql_rel *rel, int instantiate, int value_based_opt, int storage_based_opt);

#endif /*_REL_OPTIMIZER_H_*/
