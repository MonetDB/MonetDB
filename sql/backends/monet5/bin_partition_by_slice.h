/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0.  If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 *
 * Copyright 1997 - July 2008 CWI, August 2008 - 2022 MonetDB B.V.
 */

#ifndef _BIN_SLICE_PARTITION_H_
#define _BIN_SLICE_PARTITION_H_

#include "sql_statement.h"
#include "mal_backend.h"

extern bool rel_groupby_2_phases(mvc *sql, sql_rel *rel);
extern bool rel_groupby_can_pp(sql_rel *rel, bool _2phases);
extern bool exp_need_serialize(sql_exp *e);
extern bool rel_groupby_serialize(sql_rel *rel);

extern list *rel_groupby_prepare_pp(list **aggrresults, list **serializedresults, backend *be, sql_rel *rel, bool _2phases, bool need_serialize);
//todo remove from rel_bin
//extern stmt *rel_groupby_core_pp(backend *be, sql_rel *rel, list *gbstmts, stmt *grp, stmt *ext, stmt *cnt, stmt *cursub, stmt *pp, list *sub, bool _2phases);
extern stmt *rel_groupby_combine_pp(backend *be, sql_rel *rel, list *gbstmts, stmt *grp, stmt *ext, stmt *cnt, stmt *cursub, stmt *pp, list *sub, stmt **cnt_aggr);
extern stmt *rel_groupby_finish_pp(backend *be, sql_rel *rel, stmt *cursub, bool _2phases);
extern stmt *rel_groupby_count_gt_0(backend *be, stmt *cursub, stmt *cnt, int *ext);

#endif /*_BIN_SLICE_PARTITION_H_*/
