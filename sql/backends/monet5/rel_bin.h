/*
 * SPDX-License-Identifier: MPL-2.0
 *
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0.  If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 *
 * For copyright information, see the file debian/copyright.
 */

#ifndef _REL_BIN_H_
#define _REL_BIN_H_

#include "rel_semantic.h"
#include "sql_statement.h"
#include "mal_backend.h"

sql_export stmt * exp_bin(backend *be, sql_exp *e, stmt *left, stmt *right, stmt *grp, stmt *ext, stmt *cnt, stmt *sel, int depth, int reduce, int push);
extern stmt *output_rel_bin(backend *be, sql_rel *rel, int top)
	__attribute__((__visibility__("hidden")));

extern stmt * column(backend *be, stmt *val);
extern stmt * rel2bin_materialize(backend *be, sql_rel *rel, list *refs, bool top);
extern stmt *rel2bin_sql_table(backend *be, sql_table *t, list *aliases);


/* private */
#define is_equi_exp_(e) ((e)->flag == cmp_equal)

extern int add_to_rowcount_accumulator(backend *be, int nr);
extern stmt* stmt_selectnil(backend *be, stmt *col, stmt *sel);

extern stmt* bin_find_smallest_column(backend *be, stmt *sub);
extern stmt* list_find_column(backend *be, list *l, const char *rname, const char *name);
extern stmt* list_find_column_nid(backend *be, list *l, int label);

extern stmt *subrel_bin(backend *be, sql_rel *rel, list *refs);
extern stmt *subrel_project( backend *be, stmt *s, list *refs, sql_rel *rel);
extern stmt *refs_find_rel(list *refs, sql_rel *rel);
extern stmt *sql_Nop_(backend *be, const char *fname, stmt *a1, stmt *a2, stmt *a3, stmt *a4);
extern stmt *sql_unop_(backend *be, const char *fname, stmt *rs);

extern bool can_join_exp(sql_rel *rel, sql_exp *e, bool anti);
extern list *get_simple_equi_joins_first(mvc *sql, sql_rel *rel, list *exps);

extern stmt *subres_assign_resultvars(backend *be, stmt *rel_stmt, list *vars);
extern stmt *rel_rename(backend *be, sql_rel *rel, stmt *sub);

#endif /*_REL_BIN_H_*/
