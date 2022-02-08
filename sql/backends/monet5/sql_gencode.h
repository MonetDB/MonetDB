/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0.  If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 *
 * Copyright 1997 - July 2008 CWI, August 2008 - 2022 MonetDB B.V.
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
extern int monet5_resolve_function(ptr M, sql_func *f, const char *fimp, bit *side_effect);
extern int backend_create_mal_func(mvc *m, sql_func *f);
extern int backend_create_subfunc(backend *be, sql_subfunc *f, list *ops);

extern int monet5_create_relational_function(mvc *m, const char *mod, const char *name, sql_rel *rel, stmt *call, list *rel_ops, int inline_func);

extern void rel_print(mvc *sql, sql_rel *rel, int depth);
extern void _rel_print(mvc *sql, sql_rel *rel);

extern int constantAtom(backend *be, MalBlkPtr mb, atom *a);
extern InstrPtr table_func_create_result(MalBlkPtr mb, InstrPtr q, sql_func *f, list *restypes);
extern InstrPtr relational_func_create_result(mvc *sql, MalBlkPtr mb, InstrPtr q, sql_rel *f);

#endif /* _SQL2MAL_H */
