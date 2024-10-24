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

extern InstrPtr stmt_oahash_new(backend *be, int tt, int estimate, bit freq, int parent);
extern InstrPtr stmt_oahash_new_payload(backend *be, int tt, int pld_size, int parent, int previous);
extern InstrPtr stmt_oahash_build_ht(backend *be, int ht_sink, stmt *key, stmt *pp);
extern InstrPtr stmt_oahash_build_combined_ht(backend *be, int ht_sink, stmt *key, int prnt_slts, int prnt_ht, stmt *pp);
extern InstrPtr stmt_oahash_add_payload(backend *be, int hp_sink, stmt *payload, int payload_pos, stmt *pp);

extern InstrPtr stmt_oahash_hash(backend *be, stmt *key, stmt *pp);
extern InstrPtr stmt_oahash_probe(backend *be, stmt *key, int hsh, int rhs_ht, stmt *pp);
extern InstrPtr stmt_oahash_combined_hash(backend *be, stmt *key, int sel, int prnt_sltid, stmt *pp);
extern InstrPtr stmt_oahash_combined_probe(backend *be, stmt *key, int hsh, int sel, int rhs_ht, stmt *pp);

extern InstrPtr stmt_oahash_project(backend *be, stmt *col, int sel, stmt *pp);
extern InstrPtr stmt_oahash_expand(backend *be, stmt *col, int sel, int slotid, stmt *freq_sink, bit append_vals, stmt *pp);
extern InstrPtr stmt_oahash_fetch_payload(backend *be, stmt *hp_sink, int slotid, stmt *freq_sink, stmt *norows_prb, bit outer, stmt *pp);

extern InstrPtr stmt_part_new(backend *be, int nr_parts);
extern InstrPtr stmt_mat_new(backend *be, int tt, int nr_parts);
extern InstrPtr stmt_sop_new(backend *be, int nr_workers);

extern stmt *stmt_pp_aggr(backend *be, stmt *op1, stmt *grp, stmt *ext, sql_subfunc *op, int reduce, int no_nil, int nil_if_empty);
extern stmt *stmt_heapn_projection(backend *be, int sel, int del, int ins, stmt *c, stmt *all);

extern stmt *stmt_group_locked(backend *be, stmt *op1, stmt *grp, stmt *ext, stmt *cnt, stmt *pp);
extern stmt *stmt_group_partitioned(backend *be, stmt *op1, stmt *grp, stmt *ext, stmt *cnt);

extern stmt *stmt_limit_partitioned(backend *sa, stmt *c, stmt *piv, stmt *gid, stmt *offset, stmt *limit);
extern stmt *stmt_unique_sharedout(backend *be, stmt *op1, int output);

extern stmt *stmt_slice(backend *be, stmt *col, stmt *limit);
extern stmt *stmt_nth_slice(backend *be, stmt *col, int slicer);
extern stmt *stmt_no_slices(backend *be, stmt *col); /* call mal nr of slices */

extern stmt *pipeline_start(backend *be, int nrparts, int input);
extern stmt *pipeline_leave(backend *be, stmt *start);

extern stmt *stmt_pp_start_nrparts(backend *ba, int nrparts); /* create barrier label := true; part := part_nr(); leave: label:= part >= nrparts; */
extern stmt *stmt_pp_start_dynamic(backend *ba, int input); /* create barrier label := true; part := part_nr(); leave: label:= part >= nrparts; */
extern stmt *stmt_pp_start_generator(backend *ba); /* create barrier label := true; part := part_nr(); leave: label:= part >= nrparts; */
extern int stmt_pp_jump(backend *ba, stmt *pp, int nrparts);    /* redo: label := part < nrparts; */
extern int stmt_pp_end(backend *ba, stmt *pp);    /* exit: label ; */
extern void pp_cleanup(backend *ba, int var);    /* register to be cleanup variables at end of pipeline block */

extern stmt *stmt_merge(backend *be, stmt *lobc, stmt *robc, bool asc, bool nlast, stmt *zl, stmt *zb, stmt *za);
extern stmt *stmt_mproject(backend *be, stmt *zl, stmt *lc, stmt *rc, int pipeline);

extern stmt *stmt_pp_alias(backend *be, InstrPtr q, sql_exp *e, int colnr);

#endif /* _SQL_PP_STATEMENT_H_ */
