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

/* tree-values tree (tv-tree) represents all possible column types
 * (basic, composites, arrays and their combination) together with the
 * data of each. the goal is to handle INSERT VALUES dml statements
 * without type ambiguity.
 *
 * NOT to be confused with Lin et al. "The TV-tree" 10.1007/BF01231606
 */

typedef enum tv_type {
	TV_BASIC,    // basic type
	TV_MS_BSC,   // multiset of basic type
	TV_SO_BSC,   // setof of basic type
	TV_COMP,     // composite type
	TV_MS_COMP,  // multiset of composite type
	TV_SO_COMP   // setof of composite type
} tv_type;

typedef struct type_values_tree {
	tv_type tvt;
	sql_subtype *st;
	list *cf;    // list of composite type (sub)fields
	/* next members are lists of stmts IF they are instantiated */
	list *rid;
	list *msid;
	list *msnr;
	list *vals;
} tv_tree;

tv_tree * tv_create(backend *be, sql_subtype *st);
bool tv_parse_values(backend *be, tv_tree *t, list *vals, stmt *left, stmt *sel);
stmt * tv_generate_stmts(backend *be, tv_tree *t);

#endif /*_REL_TVTREE_H_*/
