/*
 * SPDX-License-Identifier: MPL-2.0
 *
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0.  If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 *
 * Copyright 1997 - July 2008 CWI, August 2008 - 2023 MonetDB B.V.
 */

#include "rel_rel.h"
#include "sql_mvc.h"

/* This file should be included by SQL optimizers or essential rewriters only! */

/* relations counts */
typedef struct global_props {
	int cnt[ddl_maxops];
	uint8_t
		instantiate:1,
		needs_mergetable_rewrite:1,
		needs_remote_replica_rewrite:1,
		needs_distinct:1,
		needs_setjoin_rewrite:1,
		has_special_modify:1, /* Don't prune updates as pruning will possibly result in removing the joins which therefor cannot be used for constraint checking */
		opt_level:1; /* 0 run necessary rewriters, 1 run all optimizers */
	uint8_t opt_cycle; /* the optimization cycle number */
} global_props;

typedef sql_rel *(*run_optimizer)(visitor *v, global_props *gp, sql_rel *rel);

/* the definition of a single SQL optimizer */
typedef struct sql_optimizer {
	const int index; /* because some optimizers runs after the main loop, an index track is needed */
	const char *name;
	run_optimizer (*bind_optimizer)(visitor *v, global_props *gp); /* if cannot run (disabled or not needed) returns NULL */
} sql_optimizer;

/* For every rewriter that cannot run twice on a relation, use the 'used' flag from the relation to mark when visited.
   Please update this list accordingly when new rewriters are added */
#define rewrite_fix_count_used    (1 << 0)
#define rewrite_values_used       (1 << 1)
#define rel_merge_select_rse_used (1 << 2)
#define statistics_gathered       (1 << 3)
#define rel_remote_func_used      (1 << 4)
#define rewrite_gt_zero_used      (1 << 5)

#define is_rewrite_fix_count_used(X)    ((X & rewrite_fix_count_used) == rewrite_fix_count_used)
#define is_rewrite_values_used(X)       ((X & rewrite_values_used) == rewrite_values_used)
#define is_rel_merge_select_rse_used(X) ((X & rel_merge_select_rse_used) == rel_merge_select_rse_used)
#define are_statistics_gathered(X)      ((X & statistics_gathered) == statistics_gathered)
#define is_rel_remote_func_used(X)      ((X & rel_remote_func_used) == rel_remote_func_used)
#define is_rewrite_gt_zero_used(X)      ((X & rewrite_gt_zero_used) == rewrite_gt_zero_used)

/* At the moment the follwowing optimizers 'packs' can be disabled,
   later we could disable individual optimizers from the 'pack' */
#define split_select                        (1 << 0)
#define push_project_down                   (1 << 1)
#define merge_projects                      (1 << 2)
#define push_project_up                     (1 << 3)
#define split_project                       (1 << 4)
#define remove_redundant_join               (1 << 5)
#define simplify_math                       (1 << 6)
#define optimize_exps                       (1 << 7)
#define optimize_select_and_joins_bottomup  (1 << 8)
#define project_reduce_casts                (1 << 9)
#define optimize_unions_bottomup           (1 << 10)
#define optimize_projections               (1 << 11)
#define optimize_joins                     (1 << 12)
#define join_order                         (1 << 13)
#define optimize_semi_and_anti             (1 << 14)
#define optimize_select_and_joins_topdown  (1 << 15)
#define optimize_unions_topdown            (1 << 16)
#define dce                                (1 << 17)
#define push_func_and_select_down          (1 << 18)
#define push_topn_and_sample_down          (1 << 19)
#define distinct_project2groupby           (1 << 20)
#define push_select_up                     (1 << 21)

extern run_optimizer bind_split_select(visitor *v, global_props *gp) __attribute__((__visibility__("hidden")));
extern run_optimizer bind_push_project_down(visitor *v, global_props *gp) __attribute__((__visibility__("hidden")));
extern run_optimizer bind_merge_projects(visitor *v, global_props *gp) __attribute__((__visibility__("hidden")));
extern run_optimizer bind_push_project_up(visitor *v, global_props *gp) __attribute__((__visibility__("hidden")));
extern run_optimizer bind_split_project(visitor *v, global_props *gp) __attribute__((__visibility__("hidden")));
extern run_optimizer bind_remove_redundant_join(visitor *v, global_props *gp) __attribute__((__visibility__("hidden")));
extern run_optimizer bind_simplify_math(visitor *v, global_props *gp) __attribute__((__visibility__("hidden")));
extern run_optimizer bind_optimize_exps(visitor *v, global_props *gp) __attribute__((__visibility__("hidden")));
extern run_optimizer bind_optimize_select_and_joins_bottomup(visitor *v, global_props *gp) __attribute__((__visibility__("hidden")));
extern run_optimizer bind_project_reduce_casts(visitor *v, global_props *gp) __attribute__((__visibility__("hidden")));
extern run_optimizer bind_optimize_unions_bottomup(visitor *v, global_props *gp) __attribute__((__visibility__("hidden")));
extern run_optimizer bind_optimize_projections(visitor *v, global_props *gp) __attribute__((__visibility__("hidden")));
extern run_optimizer bind_optimize_joins(visitor *v, global_props *gp) __attribute__((__visibility__("hidden")));
extern run_optimizer bind_join_order(visitor *v, global_props *gp) __attribute__((__visibility__("hidden")));
extern run_optimizer bind_optimize_semi_and_anti(visitor *v, global_props *gp) __attribute__((__visibility__("hidden")));
extern run_optimizer bind_optimize_select_and_joins_topdown(visitor *v, global_props *gp) __attribute__((__visibility__("hidden")));
extern run_optimizer bind_optimize_unions_topdown(visitor *v, global_props *gp) __attribute__((__visibility__("hidden")));
extern run_optimizer bind_dce(visitor *v, global_props *gp) __attribute__((__visibility__("hidden")));
extern run_optimizer bind_push_func_and_select_down(visitor *v, global_props *gp) __attribute__((__visibility__("hidden")));
extern run_optimizer bind_push_topn_and_sample_down(visitor *v, global_props *gp) __attribute__((__visibility__("hidden")));
extern run_optimizer bind_distinct_project2groupby(visitor *v, global_props *gp) __attribute__((__visibility__("hidden")));
extern run_optimizer bind_merge_table_rewrite(visitor *v, global_props *gp) __attribute__((__visibility__("hidden")));
extern run_optimizer bind_setjoins_2_joingroupby(visitor *v, global_props *gp) __attribute__((__visibility__("hidden")));
extern run_optimizer bind_get_statistics(visitor *v, global_props *gp) __attribute__((__visibility__("hidden")));
extern run_optimizer bind_join_order2(visitor *v, global_props *gp) __attribute__((__visibility__("hidden")));
extern run_optimizer bind_final_optimization_loop(visitor *v, global_props *gp) __attribute__((__visibility__("hidden")));
extern run_optimizer bind_rewrite_remote(visitor *v, global_props *gp) __attribute__((__visibility__("hidden")));
extern run_optimizer bind_rewrite_replica(visitor *v, global_props *gp) __attribute__((__visibility__("hidden")));
extern run_optimizer bind_remote_func(visitor *v, global_props *gp) __attribute__((__visibility__("hidden")));

/* these rewriters are shared by multiple optimizers */
extern sql_rel *rel_split_project_(visitor *v, sql_rel *rel, int top) __attribute__((__visibility__("hidden")));
extern sql_exp *exp_push_down_prj(mvc *sql, sql_exp *e, sql_rel *f, sql_rel *t) __attribute__((__visibility__("hidden")));
extern sql_exp *add_exp_too_project(mvc *sql, sql_exp *e, sql_rel *rel) __attribute__((__visibility__("hidden")));

/* these functions are used across diferent optimizers */
extern sql_rel *rel_find_ref(sql_rel *r) __attribute__((__visibility__("hidden")));
extern void rel_rename_exps(mvc *sql, list *exps1, list *exps2) __attribute__((__visibility__("hidden")));
extern atom *exp_flatten(mvc *sql, bool value_based_opt, sql_exp *e) __attribute__((__visibility__("hidden")));
extern atom *reduce_scale(mvc *sql, atom *a) __attribute__((__visibility__("hidden")));
extern int exp_range_overlap(atom *min, atom *max, atom *emin, atom *emax, bool min_exclusive, bool max_exclusive) __attribute__((__visibility__("hidden")));
extern int is_numeric_upcast(sql_exp *e) __attribute__((__visibility__("hidden")));
extern sql_exp *list_exps_uses_exp(list *exps, const char *rname, const char *name) __attribute__((__visibility__("hidden")));
extern sql_exp *exps_uses_exp(list *exps, sql_exp *e) __attribute__((__visibility__("hidden")));
extern int exp_keyvalue(sql_exp *e) __attribute__((__visibility__("hidden")));
