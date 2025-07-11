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

#ifndef _SQL2MAL_H
#define _SQL2MAL_H

#include "sql.h"
#include "mal_backend.h"
#include "sql_atom.h"
#include "sql_statement.h"
#include "sql_env.h"
#include "sql_mvc.h"
#include "mal_function.h"

extern int backend_dumpproc(backend *be, Client c, cq *q, sql_rel *r);
extern int backend_dumpstmt(backend *be, MalBlkPtr mb, sql_rel *r, int top, int addend, const char *query);
extern int monet5_has_module(ptr M, char *module);
extern void monet5_freecode(const char *mod, int clientid, const char *name);
extern int monet5_resolve_function(ptr M, sql_func *f, const char *fimp, bool *side_effect);
extern int backend_create_mal_func(mvc *m, sql_subfunc *sf);
extern int backend_create_subfunc(backend *be, sql_subfunc *f, list *ops);

extern int monet5_create_relational_function(mvc *m, const char *mod, const char *name, sql_rel *rel, stmt *call, list *rel_ops, int inline_func);

extern void rel_print(mvc *sql, sql_rel *rel, int depth);
extern void _rel_print(mvc *sql, sql_rel *rel);

extern void _exp_print(mvc *sql, sql_exp *e);
extern void _exps_print(mvc *sql, list *l);

extern InstrPtr table_func_create_result(MalBlkPtr mb, InstrPtr q, sql_func *f, list *restypes);
extern sql_rel *relational_func_create_result_part1(mvc *sql, sql_rel *r, int *nargs);
extern InstrPtr relational_func_create_result_part2(MalBlkPtr mb, InstrPtr q, sql_rel *r);

#endif /* _SQL2MAL_H */
