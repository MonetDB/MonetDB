/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0.  If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 *
 * Copyright 1997 - July 2008 CWI, August 2008 - 2017 MonetDB B.V.
 */

#ifndef _REL_BIN_H_
#define _REL_BIN_H_

#include "rel_semantic.h"
#include "sql_statement.h"

extern stmt * exp_bin(mvc *sql, sql_exp *e, stmt *left, stmt *right, stmt *grp, stmt *ext, stmt *cnt, stmt *sel);
extern stmt * rel_bin(mvc *sql, sql_rel *rel);
extern stmt * output_rel_bin(mvc *sql, sql_rel *rel);

extern stmt * sql_parse(mvc *m, sql_allocator *sa, char *query, char mode);

extern list *rel_dependencies(sql_allocator *sa, sql_rel *r);

#endif /*_REL_BIN_H_*/
