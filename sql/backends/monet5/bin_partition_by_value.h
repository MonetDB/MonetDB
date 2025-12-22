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
//extern stmt *rel2bin_slicer(backend *be, stmt *sub);
extern stmt *rel2bin_slicer_pp(backend *be, stmt *sub);

extern bool rel_groupby_partition(sql_rel *rel);

extern stmt *rel2bin_partition(backend *be, sql_rel *rel, list *refs);
extern stmt *rel2bin_groupby_partition(backend *be, sql_rel *rel, list *refs, bool neededpp);

extern int mat_nr_parts(backend *be, int m);
extern InstrPtr mat_counters_get(backend *be, stmt *mat, int seqnr);
extern stmt *mats_fetch_slices(backend *be, stmt *mats, int mid, int sid);

#endif /*_BIN_VALUE_PARTITION_H_*/
