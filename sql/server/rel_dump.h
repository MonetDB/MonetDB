/*
 * SPDX-License-Identifier: MPL-2.0
 *
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0.  If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 *
 * Copyright 2024, 2025 MonetDB Foundation;
 * Copyright August 2008 - 2023 MonetDB B.V.;
 * Copyright 1997 - July 2008 CWI.
 */

#ifndef _REL_DUMP_H_
#define _REL_DUMP_H_

#include "sql_relation.h"
#include "sql_mvc.h"

extern void rel_print_(mvc *sql, stream  *fout, sql_rel *rel, int depth, list *refs, int decorate);
extern void rel_print_refs(mvc *sql, stream* fout, sql_rel *rel, int depth, list *refs, int decorate);

extern str rel2str( mvc *sql, sql_rel *rel);
extern str exp2str( mvc *sql, sql_exp *exp);
extern str exp2sql( mvc *sql, sql_exp *exp);
extern sql_rel *rel_read(mvc *sql, char *ra, int *pos, list *refs);
extern sql_exp *exp_read(mvc *sql, sql_rel *lrel, sql_rel *rrel, list *top_exps, char *ra, int *pos, int grp);
extern void exp_print(mvc *sql, stream *fout, sql_exp *e, int depth, list *refs, int comma, int alias, int decorate);

#endif /*_REL_DUMP_H_*/
