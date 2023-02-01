/*
 * SPDX-License-Identifier: MPL-2.0
 *
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0.  If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 *
 * Copyright 1997 - July 2008 CWI, August 2008 - 2023 MonetDB B.V.
 */

#ifndef _REL_BIN_H_
#define _REL_BIN_H_

#include "rel_semantic.h"
#include "sql_statement.h"
#include "mal_backend.h"

extern stmt *output_rel_bin(backend *be, sql_rel *rel, int top);

// Defined in rel_bin.c but also used in rel_copy.c.
// Maybe these should go to another header file..
extern stmt * exp_bin(backend *be, sql_exp *e, stmt *left, stmt *right, stmt *grp, stmt *ext, stmt *cnt, stmt *sel, int depth, int reduce, int push);
extern int add_to_rowcount_accumulator(backend *be, int nr);


/* private */
extern stmt* bin_find_smallest_column(backend *be, stmt *sub);
extern stmt* list_find_column(backend *be, list *l, const char *rname, const char *name );

extern stmt *subrel_bin(backend *be, sql_rel *rel, list *refs);
extern stmt *subrel_project( backend *be, stmt *s, list *refs, sql_rel *rel);

extern bool get_need_pipeline(backend *be);
extern void set_need_pipeline(backend *be);

/* TODO inline part of .h */
extern void set_pipeline(backend *be, stmt *pp);
extern stmt * get_pipeline(backend *be);

extern int pp_nr_slices(sql_rel *rel);
extern int pp_dynamic_slices(backend *be, stmt *sub);
extern stmt *rel2bin_slicer(backend *be, stmt *sub, int slicer);

#endif /*_REL_BIN_H_*/
