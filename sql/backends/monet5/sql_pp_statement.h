/*
 * SPDX-License-Identifier: MPL-2.0
 *
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0.  If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 *
 * Copyright 1997 - July 2008 CWI, August 2008 - 2023 MonetDB B.V.
 */

#ifndef _SQL_PP_STATEMENT_H_
#define _SQL_PP_STATEMENT_H_

extern InstrPtr stmt_hash_new(backend *be, int tt, lng estimate, int parent);
extern InstrPtr stmt_hash_new_payload(backend *be, int tt, lng nr_slots, lng pld_size, int parent);
extern InstrPtr stmt_hash_build_table(backend *be, int ht_sink, int key, stmt *pp);
extern InstrPtr stmt_hash_build_combined_table(backend *be, int ht_sink, int key, int prnt_sltid, int prnt_ht, stmt *pp);
extern stmt *stmt_hash_add_payload(backend *be, InstrPtr ht_sink, stmt *payload, int prnt_slts, stmt *pp);

extern InstrPtr stmt_hash_hash(backend *be, int key, stmt *pp);
extern InstrPtr stmt_hash_probe(backend *be, int key, int hsh, int rht, stmt *pp);
extern InstrPtr stmt_hash_combined_hash(backend *be, int key, int sel, int prnt, stmt *pp);
extern InstrPtr stmt_hash_combined_probe(backend *be, int key, int hsh, int sel, int rht, stmt *pp);
extern stmt *stmt_hash_expand(backend *be, stmt *col, int sel, int prnt, int rhp, stmt *pp);
extern stmt *stmt_hash_fetch_payload(backend *be, int slt, stmt* hp, stmt *pp);

extern InstrPtr stmt_part_new(backend *be, int nr_parts);
extern InstrPtr stmt_mat_new(backend *be, int tt, int nr_parts);

extern stmt *stmt_pp_aggr(backend *be, stmt *op1, stmt *grp, stmt *ext, sql_subfunc *op, int reduce, int no_nil, int nil_if_empty);
extern stmt *stmt_heapn_projection(backend *be, int sel, int del, int ins, stmt *c, stmt *all);

extern stmt *stmt_group_locked(backend *be, stmt *op1, stmt *grp, stmt *ext, stmt *cnt, stmt *pp);
extern stmt *stmt_group_partitioned(backend *be, stmt *op1, stmt *grp, stmt *ext, stmt *cnt);

extern stmt *stmt_limit_partitioned(backend *sa, stmt *c, stmt *piv, stmt *gid, stmt *offset, stmt *limit);
extern stmt *stmt_unique_sharedout(backend *be, stmt *op1, int output);

extern stmt *stmt_slice(backend *be, stmt *col, stmt *limit);
extern stmt *stmt_nth_slice(backend *be, stmt *col, int slicer);
extern stmt *stmt_no_slices(backend *be, stmt *col); /* call mal nr of slices */

extern stmt *stmt_pp_start_nrparts(backend *ba, int nrparts); /* create barrier label := true; part := part_nr(); leave: label:= part >= nrparts; */
extern stmt *stmt_pp_start_dynamic(backend *ba, int input); /* create barrier label := true; part := part_nr(); leave: label:= part >= nrparts; */
extern int stmt_pp_jump(backend *ba, stmt *pp, int nrparts);    /* redo: label := part < nrparts; */
extern int stmt_pp_end(backend *ba, stmt *pp);    /* exit: label ; */

#endif /* _SQL_PP_STATEMENT_H_ */
