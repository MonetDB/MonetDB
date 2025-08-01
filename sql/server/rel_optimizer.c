/*
 * SPDX-License-Identifier: MPL-2.0
 *
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0.  If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 *
 * Copyright 2024, 2025 MonetDB Foundation;
 * Copyright August 2008 - 2023 MonetDB B.V.;
 * Copyright 1997 - July 2008 CWI.
 */

#include "monetdb_config.h"
#include "rel_optimizer.h"
#include "rel_optimizer_private.h"
#include "rel_rel.h"
#include "rel_basetable.h"
#include "rel_exp.h"
#include "rel_propagate.h"
#include "rel_statistics.h"
#include "sql_privileges.h"
#include "sql_storage.h"

static sql_rel *
rel_properties(visitor *v, sql_rel *rel)
{
	global_props *gp = (global_props*)v->data;

	/* Don't flag any changes here! */
	gp->cnt[(int)rel->op]++;
	gp->needs_distinct |= need_distinct(rel);
	gp->recursive |= is_recursive(rel);
	if (gp->instantiate && is_basetable(rel->op)) {
		mvc *sql = v->sql;
		sql_table *t = (sql_table *) rel->l;
		sql_part *pt;

		/* If the plan has a merge table or a child of one, then rel_merge_table_rewrite has to run */
		gp->needs_mergetable_rewrite |= (isMergeTable(t) || (t->s && t->s->parts && (pt = partition_find_part(sql->session->tr, t, NULL))));
		gp->needs_remote_replica_rewrite |= (isRemote(t) || isReplicaTable(t));
	}
	return rel;
}

typedef struct {
	atom *lval;
	atom *hval;
	bte anti:1,
		semantics:1;
	int flag;
	list *values;
} range_limit;

typedef struct {
	list *cols;
	list *ranges;
	sql_rel *sel;
} merge_table_prune_info;

static sql_rel *merge_table_prune_and_unionize(visitor *v, sql_rel *mt_rel, merge_table_prune_info *info);

static sql_rel *
rel_wrap_select_around_mt_child(visitor *v, sql_rel *t, merge_table_prune_info *info)
{
	// TODO: it has to be a table (merge table component) add checks
	sql_table *subt = (sql_table *)t->l;

	if (isMergeTable(subt)) {
		if ((t = merge_table_prune_and_unionize(v, t, info)) == NULL)
			return NULL;
	}

	if (info) {
		t = rel_select(v->sql->sa, t, NULL);
		t->exps = exps_copy(v->sql, info->sel->exps);
		set_processed(t);
		set_processed(t);
	}
	return t;
}

static sql_rel *
rel_unionize_mt_tables_munion(visitor *v, sql_rel* mt, list* tables, merge_table_prune_info *info)
{
	/* create the list of all the operand rels */
	list *rels = sa_list(v->sql->sa);
	for (node *n = tables->h; n; n = n->next) {
		sql_rel *r = rel_wrap_select_around_mt_child(v, n->data, info);
		append(rels, r);
	}

	/* create the munion */
	sql_rel *mu = rel_setop_n_ary(v->sql->sa, rels, op_munion);
	rel_setop_n_ary_set_exps(v->sql, mu, rel_projections(v->sql, mt, NULL, 1, 1), true);
	set_processed(mu);

	return mu;
}

static sql_rel *
merge_table_prune_and_unionize(visitor *v, sql_rel *mt_rel, merge_table_prune_info *info)
{
	if (mvc_highwater(v->sql))
		return sql_error(v->sql, 10, SQLSTATE(42000) "Query too complex: running out of stack space");

	sql_rel *nrel = NULL;
	sql_table *mt = (sql_table*) mt_rel->l;
	const char *mtalias = exp_relname(mt_rel->exps->h->data);
	list *tables = sa_list(v->sql->sa);

	for (node *nt = mt->members->h; nt; nt = nt->next) {
		sql_part *pd = nt->data;
		sql_table *pt = find_sql_table_id(v->sql->session->tr, mt->s, pd->member);
		sqlstore *store = v->sql->session->tr->store;
		int skip = 0;

		/* At the moment we throw an error in the optimizer, but later this rewriter should move out from the optimizers */
		if ((isMergeTable(pt) || isReplicaTable(pt)) && list_empty(pt->members))
			return sql_error(v->sql, 02, SQLSTATE(42000) "%s '%s'.'%s' should have at least one table associated",
							TABLE_TYPE_DESCRIPTION(pt->type, pt->properties), pt->s->base.name, pt->base.name);
		/* Do not include empty partitions */
		if (isTable(pt) && pt->access == TABLE_READONLY && !store->storage_api.count_col(v->sql->session->tr, ol_first_node(pt->columns)->data, CNT_ACTIVE)) /* count active rows only */
			continue;

		for (node *n = mt_rel->exps->h; n && !skip; n = n->next) { /* for each column of the child table */
			sql_exp *e = n->data;
			int i = 0;
			bool first_attempt = true;
			atom *cmin = NULL, *cmax = NULL, *rmin = NULL, *rmax = NULL;
			list *inlist = NULL;
			const char *cname = e->r;
			sql_column *mt_col = NULL, *col = NULL;

			if (cname[0] == '%') /* Ignore TID and indexes here */
				continue;

			mt_col = ol_find_name(mt->columns, cname)->data;
			col = ol_fetch(pt->columns, mt_col->colnr);
			assert(e && e->type == e_column && col);

			if (isTable(pt) && info && !list_empty(info->cols) && ATOMlinear(exp_subtype(e)->type->localtype)) {
				for (node *nn = info->cols->h ; nn && !skip; nn = nn->next) { /* test if it passes all predicates around it */
					if (nn->data == e) {
						range_limit *next = list_fetch(info->ranges, i);
						atom *lval = next->lval, *hval = next->hval;
						list *values = next->values;

						/* I don't handle cmp_in or cmp_notin cases with anti or null semantics yet */
						if (next->flag == cmp_in && (next->anti || next->semantics))
							continue;

						assert(col && (lval || values));
						if (!skip && pt->access == TABLE_READONLY) {
							/* check if the part falls within the bounds of the select expression else skip this (keep at least on part-table) */
							if (!cmin && !cmax && first_attempt) {
								void *min = NULL, *max = NULL;
								if (sql_trans_ranges(v->sql->session->tr, col, &min, &max) && min && max) {
									cmin = atom_general_ptr(v->sql->sa, &col->type, min);
									cmax = atom_general_ptr(v->sql->sa, &col->type, max);
								}
								first_attempt = false; /* no more attempts to read from storage */
							}

							if (cmin && cmax) {
								if (lval) {
									if (!next->semantics && ((lval && lval->isnull) || (hval && hval->isnull))) {
										skip = 1; /* NULL values don't match, skip them */
									} else if (!next->semantics) {
										if (next->flag == cmp_equal) {
											skip |= next->anti ? exp_range_overlap(cmin, cmax, lval, hval, false, false) != 0 :
																	exp_range_overlap(cmin, cmax, lval, hval, false, false) == 0;
										} else if (hval != lval) { /* range case */
											comp_type lower = range2lcompare(next->flag), higher = range2rcompare(next->flag);
											skip |= next->anti ? exp_range_overlap(cmin, cmax, lval, hval, higher == cmp_lt, lower == cmp_gt) != 0 :
																	exp_range_overlap(cmin, cmax, lval, hval, higher == cmp_lt, lower == cmp_gt) == 0;
										} else {
											switch (next->flag) {
												case cmp_gt:
													skip |= next->anti ? VALcmp(&(lval->data), &(cmax->data)) < 0 : VALcmp(&(lval->data), &(cmax->data)) >= 0;
													break;
												case cmp_gte:
													skip |= next->anti ? VALcmp(&(lval->data), &(cmax->data)) <= 0 : VALcmp(&(lval->data), &(cmax->data)) > 0;
													break;
												case cmp_lt:
													skip |= next->anti ? VALcmp(&(lval->data), &(cmax->data)) < 0 : VALcmp(&(cmin->data), &(lval->data)) >= 0;
													break;
												case cmp_lte:
													skip |= next->anti ? VALcmp(&(lval->data), &(cmax->data)) <= 0 : VALcmp(&(cmin->data), &(lval->data)) > 0;
													break;
												default:
													break;
											}
										}
									}
								} else if (next->flag == cmp_in) {
									int nskip = 1;
									for (node *m = values->h; m && nskip; m = m->next) {
										atom *a = m->data;

										if (a->isnull)
											continue;
										nskip &= exp_range_overlap(cmin, cmax, a, a, false, false) == 0;
									}
									skip |= nskip;
								}
							}
						}
						if (!skip && isPartitionedByColumnTable(mt) && strcmp(mt->part.pcol->base.name, col->base.name) == 0) {
							if (!next->semantics && ((lval && lval->isnull) || (hval && hval->isnull))) {
								skip = 1; /* NULL values don't match, skip them */
							} else if (next->semantics) {
								/* TODO NOT NULL prunning for partitions that just hold NULL values is still missing */
								skip |= next->flag == cmp_equal && !next->anti && lval && lval->isnull ? pd->with_nills == 0 : 0; /* *= NULL case */
							} else {
								if (isRangePartitionTable(mt)) {
									if (!rmin || !rmax) { /* initialize lazily */
										rmin = atom_general_ptr(v->sql->sa, &col->type, pd->part.range.minvalue);
										rmax = atom_general_ptr(v->sql->sa, &col->type, pd->part.range.maxvalue);
									}

									/* Prune range partitioned tables */
									if (rmin->isnull && rmax->isnull) {
										if (pd->with_nills == 1) /* the partition just holds null values, skip it */
											skip = 1;
										/* otherwise it holds all values in the range, cannot be pruned */
									} else if (rmin->isnull) { /* MINVALUE to limit */
										if (lval) {
											if (hval != lval) { /* range case */
												/* There's need to call range2lcompare, because the partition's upper limit is always exclusive */
												skip |= next->anti ? VALcmp(&(lval->data), &(rmax->data)) < 0 : VALcmp(&(lval->data), &(rmax->data)) >= 0;
											} else {
												switch (next->flag) { /* upper limit always exclusive */
													case cmp_equal:
													case cmp_gt:
													case cmp_gte:
														skip |= next->anti ? VALcmp(&(lval->data), &(rmax->data)) < 0 : VALcmp(&(lval->data), &(rmax->data)) >= 0;
														break;
													default:
														break;
												}
											}
										} else if (next->flag == cmp_in) {
											int nskip = 1;
											for (node *m = values->h; m && nskip; m = m->next) {
												atom *a = m->data;

												if (a->isnull)
													continue;
												nskip &= VALcmp(&(a->data), &(rmax->data)) >= 0;
											}
											skip |= nskip;
										}
									} else if (rmax->isnull) { /* limit to MAXVALUE */
										if (lval) {
											if (hval != lval) { /* range case */
												comp_type higher = range2rcompare(next->flag);
												if (higher == cmp_lt) {
													skip |= next->anti ? VALcmp(&(rmin->data), &(hval->data)) < 0 : VALcmp(&(rmin->data), &(hval->data)) >= 0;
												} else if (higher == cmp_lte) {
													skip |= next->anti ? VALcmp(&(rmin->data), &(hval->data)) <= 0 : VALcmp(&(rmin->data), &(hval->data)) > 0;
												} else {
													assert(0);
												}
											} else {
												switch (next->flag) {
													case cmp_lt:
														skip |= next->anti ? VALcmp(&(rmin->data), &(hval->data)) < 0 : VALcmp(&(rmin->data), &(hval->data)) >= 0;
														break;
													case cmp_equal:
													case cmp_lte:
														skip |= next->anti ? VALcmp(&(rmin->data), &(hval->data)) <= 0 : VALcmp(&(rmin->data), &(hval->data)) > 0;
														break;
													default:
														break;
												}
											}
										} else if (next->flag == cmp_in) {
											int nskip = 1;
											for (node *m = values->h; m && nskip; m = m->next) {
												atom *a = m->data;

												if (a->isnull)
													continue;
												nskip &= VALcmp(&(rmin->data), &(a->data)) > 0;
											}
											skip |= nskip;
										}
									} else { /* limit1 to limit2 (general case), limit2 is exclusive */
										bool max_differ_min = ATOMcmp(col->type.type->localtype, &rmin->data.val, &rmax->data.val) != 0;

										if (lval) {
											if (next->flag == cmp_equal) {
												skip |= next->anti ? exp_range_overlap(rmin, rmax, lval, hval, false, max_differ_min) != 0 :
																		exp_range_overlap(rmin, rmax, lval, hval, false, max_differ_min) == 0;
											} else if (hval != lval) { /* For the between case */
												comp_type higher = range2rcompare(next->flag);
												skip |= next->anti ? exp_range_overlap(rmin, rmax, lval, hval, higher == cmp_lt, max_differ_min) != 0 :
																		exp_range_overlap(rmin, rmax, lval, hval, higher == cmp_lt, max_differ_min) == 0;
											} else {
												switch (next->flag) {
													case cmp_gt:
														skip |= next->anti ? VALcmp(&(lval->data), &(rmax->data)) < 0 : VALcmp(&(lval->data), &(rmax->data)) >= 0;
														break;
													case cmp_gte:
														if (max_differ_min)
															skip |= next->anti ? VALcmp(&(lval->data), &(rmax->data)) < 0 : VALcmp(&(lval->data), &(rmax->data)) >= 0;
														else
															skip |= next->anti ? VALcmp(&(lval->data), &(rmax->data)) <= 0 : VALcmp(&(lval->data), &(rmax->data)) > 0;
														break;
													case cmp_lt:
														skip |= next->anti ? VALcmp(&(rmin->data), &(lval->data)) < 0 : VALcmp(&(rmin->data), &(lval->data)) >= 0;
														break;
													case cmp_lte:
														skip |= next->anti ? VALcmp(&(rmin->data), &(lval->data)) <= 0 : VALcmp(&(rmin->data), &(lval->data)) > 0;
														break;
													default:
														break;
												}
											}
										} else if (next->flag == cmp_in) {
											int nskip = 1;
											for (node *m = values->h; m && nskip; m = m->next) {
												atom *a = m->data;

												if (a->isnull)
													continue;
												nskip &= exp_range_overlap(rmin, rmax, a, a, false, max_differ_min) == 0;
											}
											skip |= nskip;
										}
									}
								}

								if (isListPartitionTable(mt) && (next->flag == cmp_equal || next->flag == cmp_in) && !next->anti) {
									/* if we find a value equal to one of the predicates, we don't prune */
									/* if the partition just holds null values, it will be skipped */
									if (!inlist) { /* initialize lazily */
										inlist = sa_list(v->sql->sa);
										for (node *m = pd->part.values->h; m; m = m->next) {
											sql_part_value *spv = (sql_part_value*) m->data;
											atom *pa = atom_general_ptr(v->sql->sa, &col->type, spv->value);

											list_append(inlist, pa);
										}
									}

									if (next->flag == cmp_equal) {
										int nskip = 1;
										for (node *m = inlist->h; m && nskip; m = m->next) {
											atom *pa = m->data;
											assert(!pa->isnull);
											nskip &= VALcmp(&(pa->data), &(lval->data)) != 0;
										}
										skip |= nskip;
									} else if (next->flag == cmp_in) {
										for (node *o = values->h; o && !skip; o = o->next) {
											atom *a = o->data;
											int nskip = 1;

											if (a->isnull)
												continue;
											for (node *m = inlist->h; m && nskip; m = m->next) {
												atom *pa = m->data;
												assert(!pa->isnull);
												nskip &= VALcmp(&(pa->data), &(a->data)) != 0;
											}
											skip |= nskip;
										}
									}
								}
							}
						}
					}
					i++;
				}
			}
		}
		if (!skip)
			append(tables, rel_rename_part(v->sql, rel_basetable(v->sql, pt, pt->base.name), mt_rel, mtalias));
	}
	if (list_empty(tables)) { /* No table passed the predicates, generate dummy relation */
		list *converted = sa_list(v->sql->sa);
		nrel = rel_project_exp(v->sql, exp_atom_bool(v->sql->sa, 1));
		nrel = rel_select(v->sql->sa, nrel, exp_atom_bool(v->sql->sa, 0));
		set_processed(nrel);

		for (node *n = mt_rel->exps->h ; n ; n = n->next) {
			sql_exp *e = n->data, *a = exp_atom(v->sql->sa, atom_general(v->sql->sa, exp_subtype(e), NULL, 0));
			exp_prop_alias(v->sql->sa, a, e);
			list_append(converted, a);
		}
		nrel = rel_project(v->sql->sa, nrel, converted);
	} else { /* Unionize children tables */

		if (mvc_debug_on(v->sql, 16)) {
			/* In case of a single table there in nothing to unionize */
			if (tables->cnt == 1) {
				nrel = rel_wrap_select_around_mt_child(v, tables->h->data, info);
			} else {
				//nrel = rel_unionize_mt_tables_balanced(v, mt_rel, tables, info);
				nrel = rel_setop_n_ary(v->sql->sa, tables, op_munion);
			}
		} else if (mvc_debug_on(v->sql, 32)) {
			for (node *n = tables->h; n ; n = n->next) {
				sql_rel *next = n->data;
				sql_table *subt = (sql_table *) next->l;

				if (isMergeTable(subt)) { /* apply select predicate recursively for nested merge tables */
					if (!(next = merge_table_prune_and_unionize(v, next, info)))
						return NULL;
				} else if (info) { /* propagate select under union */
					next = rel_select(v->sql->sa, next, NULL);
					next->exps = exps_copy(v->sql, info->sel->exps);
					set_processed(next);
				}

				if (nrel) {
					nrel = rel_setop_n_ary(v->sql->sa, append(append(sa_list(v->sql->sa), nrel), next), op_munion);
					rel_setop_n_ary_set_exps(v->sql, nrel, rel_projections(v->sql, mt_rel, NULL, 1, 1), true);
					set_processed(nrel);
				} else {
					nrel = next;
				}
			}
		} else {
			if (tables->cnt == 1) {
				nrel = rel_wrap_select_around_mt_child(v, tables->h->data, info);
			} else {
				nrel = rel_unionize_mt_tables_munion(v, mt_rel, tables, info);
			}
		}
	}
	return nrel;
}

/* rewrite merge tables into union of base tables */
static sql_rel *
rel_merge_table_rewrite_(visitor *v, sql_rel *rel)
{
	if (is_groupby(rel->op)) {
		sql_rel *l = rel->l;
		if (is_modify(l->op))
			return rel_propagate(v, rel);
	}
	if (is_modify(rel->op)) {
		return rel_propagate(v, rel);
	} else {
		sql_rel *bt = rel, *sel = NULL, *nrel = NULL;

		if (is_select(rel->op)) {
			sel = rel;
			bt = rel->l;
		}
		if (is_basetable(bt->op) && rel_base_table(bt) && isMergeTable((sql_table*)bt->l)) {
			sql_table *mt = rel_base_table(bt);
			merge_table_prune_info *info = NULL;

			if (list_empty(mt->members)) /* in DDL statement cases skip if mergetable is empty */
				return rel;
			if (sel && !list_empty(sel->exps)) { /* prepare prunning information once */
				info = SA_NEW(v->sql->sa, merge_table_prune_info);
				*info = (merge_table_prune_info) {
					.cols = sa_list(v->sql->sa),
					.ranges = sa_list(v->sql->sa),
					.sel = sel
				};
				for (node *n = sel->exps->h; n; n = n->next) {
					sql_exp *e = n->data, *c = e->l;
					int flag = e->flag;

					if (e->type != e_cmp || (!is_theta_exp(flag) && flag != cmp_in) || is_symmetric(e) || !(c = rel_find_exp(rel, c)))
						continue;

					if (flag == cmp_gt || flag == cmp_gte || flag == cmp_lte || flag == cmp_lt || flag == cmp_equal) {
						sql_exp *l = e->r, *h = e->f;
						atom *lval = exp_flatten(v->sql, v->value_based_opt, l);
						atom *hval = h ? exp_flatten(v->sql, v->value_based_opt, h) : lval;

						if (lval && hval) {
							range_limit *next = SA_NEW(v->sql->sa, range_limit);

							*next = (range_limit) {
								.lval = lval,
								.hval = hval,
								.flag = flag,
								.anti = is_anti(e),
								.semantics = is_semantics(e),
							};
							list_append(info->cols, c);
							list_append(info->ranges, next);
						}
					}
					if (flag == cmp_in) { /* handle in lists */
						list *vals = e->r, *vlist = sa_list(v->sql->sa);

						node *m = NULL;
						for (m = vals->h; m; m = m->next) {
							sql_exp *l = m->data;
							atom *lval = exp_flatten(v->sql, v->value_based_opt, l);

							if (!lval)
								break;
							list_append(vlist, lval);
						}
						if (!m) {
							range_limit *next = SA_NEW(v->sql->sa, range_limit);

							*next = (range_limit) {
								.values = vlist, /* mark high as value list */
								.flag = flag,
								.anti = is_anti(e),
								.semantics = is_semantics(e),
							};
							list_append(info->cols, c);
							list_append(info->ranges, next);
						}
					}
				}
			}
			if (!(nrel = merge_table_prune_and_unionize(v, bt, info)))
				return NULL;
			/* Always do relation inplace. If the mt relation has more than 1 reference, this is required */
			if (is_munion(nrel->op)) {
				rel = rel_inplace_setop_n_ary(v->sql, rel, nrel->l, op_munion, nrel->exps);
			} else if (is_select(nrel->op)) {
				rel = rel_inplace_select(rel, nrel->l, nrel->exps);
			} else if (is_basetable(nrel->op)) {
				rel = rel_inplace_basetable(rel, nrel);
			} else {
				assert(is_simple_project(nrel->op));
				rel = rel_inplace_project(v->sql->sa, rel, nrel->l, nrel->exps);
				rel->card = exps_card(nrel->exps);
			}
			/* make sure that we do NOT destroy the subrels */
			nrel->l = nrel->r = NULL;
			rel_destroy(nrel);
			v->changes++;
		}
	}
	return rel;
}

static sql_rel *
rel_merge_table_rewrite(visitor *v, global_props *gp, sql_rel *rel)
{
	(void) gp;
	return rel_visitor_topdown(v, rel, &rel_merge_table_rewrite_);
}

run_optimizer
bind_merge_table_rewrite(visitor *v, global_props *gp)
{
	(void) v;
	return gp->needs_mergetable_rewrite ? rel_merge_table_rewrite : NULL;
}

/* these optimizers/rewriters run in a cycle loop */
const sql_optimizer pre_sql_optimizers[] = {
	{ 0, "split_select", bind_split_select},
	{ 1, "push_project_down", bind_push_project_down},
	{ 2, "merge_projects", bind_merge_projects},
	{ 3, "push_project_up", bind_push_project_up},
	{ 4, "split_project", bind_split_project},
	{ 5, "remove_redundant_join", bind_remove_redundant_join},
	{ 6, "simplify_math", bind_simplify_math},
	{ 7, "optimize_exps", bind_optimize_exps},
	{ 8, "optimize_select_and_joins_bottomup", bind_optimize_select_and_joins_bottomup},
	{ 9, "project_reduce_casts", bind_project_reduce_casts},
	{10, "optimize_unions_bottomup", bind_optimize_unions_bottomup},
	{11, "optimize_projections", bind_optimize_projections},
	{12, "optimize_joins", bind_optimize_joins},
	{13, "join_order", bind_join_order},
	{14, "optimize_semi_and_anti", bind_optimize_semi_and_anti},
	{15, "optimize_select_and_joins_topdown", bind_optimize_select_and_joins_topdown},
	{16, "optimize_unions_topdown", bind_optimize_unions_topdown},
	{17, "dce", bind_dce},
	{18, "push_func_and_select_down", bind_push_func_and_select_down},
	{19, "push_topn_and_sample_down", bind_push_topn_and_sample_down},
	{20, "distinct_project2groupby", bind_distinct_project2groupby},
	{21, "merge_table_rewrite", bind_merge_table_rewrite},
	{ 0, NULL, NULL}
};

/* these optimizers/rewriters only run once after the cycle loop */
const sql_optimizer post_sql_optimizers[] = {
	/* Merge table rewrites may introduce remote or replica tables */
	/* At the moment, make sure the remote table rewriters always run after the merge table one */
	{23, "rewrite_remote", bind_rewrite_remote},
	{24, "rewrite_replica", bind_rewrite_replica},
	{25, "remote_func", bind_remote_func},
	{26, "get_statistics", bind_get_statistics}, /* gather statistics */
	{27, "join_order2", bind_join_order2}, /* run join order one more time with statistics */
	{28, "final_optimization_loop", bind_final_optimization_loop}, /* run select and group by order with statistics gathered  */
	{ 0, NULL, NULL}
	/* If an optimizer is going to be added, don't forget to update NSQLREWRITERS macro */
};


/* for trivial queries don't run optimizers */
static int
calculate_opt_level(mvc *sql, sql_rel *rel)
{
	if (rel->card <= CARD_ATOM) {
		if (is_insert(rel->op))
			return rel->r ? calculate_opt_level(sql, rel->r) : 0;
		if (is_simple_project(rel->op))
			return rel->l ? calculate_opt_level(sql, rel->l) : 0;
	}
	return 1;
}

static inline sql_rel *
run_optimizer_set(visitor *v, sql_optimizer_run *runs, sql_rel *rel, global_props *gp, const sql_optimizer *set)
{
	/* if 'runs' is set, it means profiling is intended */
	for (int i = 0 ; set[i].name ; i++) {
		run_optimizer opt = NULL;

		if ((opt = set[i].bind_optimizer(v, gp))) {
			if (runs) {
				sql_optimizer_run *run = &(runs[set[i].index]);
				run->name = set[i].name;
				int changes = v->changes;
				lng clk = GDKusec();
				rel = opt(v, gp, rel);
				run->time += (GDKusec() - clk);
				run->nchanges += (v->changes - changes);
			} else {
				rel = opt(v, gp, rel);
			}
		}
	}
	return rel;
}

/* 'profile' means to benchmark each individual optimizer run */
/* 'instantiate' means to rewrite logical tables: (merge, remote, replica tables) */
static sql_rel *
rel_optimizer_one(mvc *sql, sql_rel *rel, int profile, int instantiate, int value_based_opt, int storage_based_opt)
{
	global_props gp = (global_props) {.cnt = {0}, .instantiate = (uint8_t)instantiate, .opt_cycle = 0 };
	visitor v = { .sql = sql, .value_based_opt = value_based_opt, .storage_based_opt = storage_based_opt, .changes = 1, .data = &gp };

	sql->runs = !(ATOMIC_GET(&GDKdebug) & TESTINGMASK) && profile ? sa_zalloc(sql->sa, NSQLREWRITERS * sizeof(sql_optimizer_run)) : NULL;
	for ( ;rel && gp.opt_cycle < 20 && v.changes; gp.opt_cycle++) {
		v.changes = 0;
		gp = (global_props) {.cnt = {0}, .instantiate = (uint8_t)instantiate, .opt_cycle = gp.opt_cycle};
		rel = rel_visitor_topdown(&v, rel, &rel_properties); /* collect relational tree properties */
		gp.opt_level = calculate_opt_level(sql, rel);
		if (gp.opt_level == 0 && !gp.needs_mergetable_rewrite)
			break;
		sql->recursive = gp.recursive;
		rel = run_optimizer_set(&v, sql->runs, rel, &gp, pre_sql_optimizers);
	}
#ifndef NDEBUG
	assert(gp.opt_cycle < 20);
#endif

	/* these optimizers run statistics gathered by the last optimization cycle */
	rel = run_optimizer_set(&v, sql->runs, rel, &gp, post_sql_optimizers);
	return rel;
}

static sql_exp *
exp_optimize_one(visitor *v, sql_rel *rel, sql_exp *e, int depth )
{
       (void)rel;
       (void)depth;
       if (e->type == e_psm && e->flag == PSM_REL && e->l) {
               e->l = rel_optimizer_one(v->sql, e->l, 0, v->changes, v->value_based_opt, v->storage_based_opt);
       }
       return e;
}

sql_rel *
rel_optimizer(mvc *sql, sql_rel *rel, int profile, int instantiate, int value_based_opt, int storage_based_opt)
{
	if (rel && rel->op == op_ddl && rel->flag == ddl_psm) {
		if (!list_empty(rel->exps)) {
			bool changed = 0;
			visitor v = { .sql = sql, .value_based_opt = value_based_opt, .storage_based_opt = storage_based_opt, .changes = instantiate };
			for(node *n = rel->exps->h; n; n = n->next) {
				sql_exp *e = n->data;
				n->data = exp_visitor(&v, rel, e, 1, exp_optimize_one, true, true, true, &changed);
			}
		}
		return rel;
	} else {
		return rel_optimizer_one(sql, rel, profile, instantiate, value_based_opt, storage_based_opt);
	}
}
