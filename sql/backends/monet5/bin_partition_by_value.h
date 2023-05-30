/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0.  If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 *
 * Copyright 1997 - July 2008 CWI, August 2008 - 2022 MonetDB B.V.
 */

#ifndef _BIN_VALUE_PARTITION_H_
#define _BIN_VALUE_PARTITION_H_

#include "sql_statement.h"
#include "mal_backend.h"

extern int pp_dynamic_slices(backend *be, stmt *sub);
extern stmt *rel2bin_slicer(backend *be, stmt *sub, int slicer);

extern bool rel_groupby_partition(backend *be, sql_rel *rel);

extern stmt *rel2bin_groupby_partition(backend *be, sql_rel *rel, list *refs);
extern stmt *rel_pp_groupby(backend *be, sql_rel *rel, list *gbstmts, stmt *grp, stmt *ext, stmt *cnt, stmt *cursub, stmt *pp, list *sub, bool _2phases);

#endif /*_BIN_VALUE_PARTITION_H_*/
