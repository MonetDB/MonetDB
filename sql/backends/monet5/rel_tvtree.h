/*
 * SPDX-License-Identifier: MPL-2.0
 *
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0.  If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 *
 * Copyright 2024 - 2025 MonetDB Foundation;
 * Copyright August 2008 - 2023 MonetDB B.V.;
 * Copyright 1997 - July 2008 CWI.
 */

#ifndef _REL_TVTREE_H_
#define _REL_TVTREE_H_

#include <stddef.h>
#include <stdint.h>
#include <stdbool.h>
#include <stdio.h>
#include <assert.h>
#include "sql_relation.h"
#include "mal_backend.h"
#include "sql_list.h"
#include "sql_catalog.h"
#include "sql_statement.h"

/* type-values tree (tv-tree) represents all possible column types
 * (basic, composites, arrays and their combination) together with the
 * data of each. the goal is to handle INSERT VALUES dml statements
 * without type ambiguity.
 *
 * NOT to be confused with Lin et al. "The TV-tree" 10.1007/BF01231606
 */

typedef enum tv_type {
	TV_BASIC, // basic type
	TV_COMP,  // composite type
	TV_MSET,  // multiset of composite type
	TV_SETOF,  // setof of composite type
} tv_type;

typedef struct type_values_tree {
	tv_type tvt;
	sql_subtype *st;
	int rid_idx;  // mset values needs to know to which row they correspond to
	list *ctl;    // list of child tv nodes (underlying type for sets/subfields for composite)
	/* next members are lists of stmts IF they are instantiated */
	list *rid;    // row id for multisets
	list *msid;   // multiset id (always refers to a row id)
	list *msnr;   // multiset number is the index inside a multiset entry
	list *vals;
} tv_tree;

tv_tree * tv_create(backend *be, sql_subtype *st);
bool tv_parse_values(backend *be, tv_tree *t, sql_exp *col_vals, stmt *left, stmt *sel);
stmt * tv_generate_stmts(backend *be, tv_tree *t);

#endif /*_REL_TVTREE_H_*/
