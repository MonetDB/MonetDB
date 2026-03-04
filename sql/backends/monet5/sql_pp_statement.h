/*
 * SPDX-License-Identifier: MPL-2.0
 *
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0.  If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 *
 * For copyright information, see the file debian/copyright.
 */

#ifndef _SQL_PP_STATEMENT_H_
#define _SQL_PP_STATEMENT_H_

extern void set_need_pipeline(backend *be);
extern bool get_need_pipeline(backend *be);

extern void set_pipeline(backend *be, stmt *pp);
extern stmt * get_pipeline(backend *be);

extern stmt *stmt_oahash_new(backend *be, sql_subtype *tpe, int estimate, int parent, int nrparts);
extern stmt *stmt_oahash_hshmrk_init(backend *be, stmt *stmts_ht, bool moveup);
extern stmt *stmt_oahash_build_ht(backend *be, stmt *ht, stmt *key, stmt *prnt, const stmt *pp);
extern stmt *stmt_oahash_frequency(backend *be, stmt *freq, stmt *prnt, bool occ_cnt, const stmt *pp);

extern stmt *stmt_oahash_hash(backend *be, stmt *key, stmt *prev, stmt *ht);
extern stmt *stmt_oahash_probe(backend *be, stmt *key, stmt *prev, stmt *rhs_ht, stmt *freq, stmt *outer, bool single, bool semantics, bool eq, bool outerjoin, bool groupedjoin, const stmt *pp);

extern stmt *stmt_algebra_project(backend *be, stmt *inout, stmt *pos, stmt *val, const char *fname, const stmt *pp);
extern stmt *stmt_oahash_project_cart(backend *be, stmt *col, stmt *repeat, bool outer, bool expand);

extern stmt *stmt_oahash_expand(backend *be, const stmt *prb_res, const stmt *freq, bit outer);
extern stmt *stmt_oahash_explode(backend *be, const stmt *prb_res, const stmt *freq, const stmt *ht_sink, bit outer);

extern stmt *stmt_oahash_explode_unmatched(backend *be, const stmt *ht, const stmt *mrk, const stmt *freq);

extern InstrPtr stmt_part_new(backend *be, int nr_parts);
extern stmt *stmt_mat_new(backend *be, sql_subtype *t, int nr_parts);
extern InstrPtr stmt_sop_new(backend *be, int nr_workers);

extern stmt *stmt_pp_aggr(backend *be, stmt *op1, stmt *grp, stmt *ext, sql_subfunc *op, int reduce, int no_nil, int nil_if_empty);

extern stmt *stmt_group_locked(backend *be, stmt *op1, stmt *grp, stmt *ext, stmt *cnt, stmt *pp);
extern stmt *stmt_group_partitioned(backend *be, stmt *op1, stmt *grp, stmt *ext, stmt *cnt);

extern stmt *stmt_limit_partitioned(backend *sa, stmt *c, stmt *piv, stmt *gid, stmt *offset, stmt *limit);
extern stmt *stmt_unique_sharedout(backend *be, stmt *op1, int output);

extern stmt *stmt_slice(backend *be, stmt *col, stmt *limit);
extern stmt *stmt_nth_slice(backend *be, stmt *col, bool hash);
extern stmt *stmt_no_slices(backend *be, stmt *col, bool hash); /* call mal nr of slices */

extern stmt *stmt_pp_start_nrparts(backend *ba, int nrparts); /* create barrier label := true; part := part_nr(); leave: label:= part >= nrparts; */
extern stmt *stmt_pp_start_dynamic(backend *ba, int input); /* create barrier label := true; part := part_nr(); leave: label:= part >= nrparts; */
extern stmt *stmt_pp_start_generator(backend *ba, int source, bool leave); /* create barrier label := true; part := part_nr(); leave: label:= part >= nrparts; */
extern int stmt_pp_jump(backend *ba, stmt *pp, int nrparts);    /* redo: label := part < nrparts; */
extern int stmt_pp_end(backend *ba, stmt *pp);    /* exit: label ; */
extern void pp_cleanup(backend *ba, int var);    /* register to be cleanup variables at end of pipeline block */

extern stmt *stmt_merge(backend *be, stmt *lobc, stmt *robc, bool asc, bool nlast, stmt *zl, stmt *zb, stmt *za);
extern stmt *stmt_mproject(backend *be, stmt *zl, stmt *lc, stmt *rc, int pipeline);

extern stmt *stmt_pp_alias(backend *be, InstrPtr q, sql_exp *e, int colnr);

extern int stmt_concat(backend *be, int parent_block, int nr);
extern int stmt_concat_barrier(backend *be, int concat, int blockid, int prev);
extern int stmt_concat_barrier_end(backend *be, int barrier);
extern int stmt_concat_add_source(backend *be);
extern int stmt_concat_add_subconcat(backend *be, int p_source, int p_concatcnt );

extern int pp_counter(backend *be, int nr_slices, int var_nr_slices, bool sync);
extern int pp_counter_get(backend *be, int counter);

extern int pp_claim(backend *be, int resultset, int nrrows);

#endif /* _SQL_PP_STATEMENT_H_ */
