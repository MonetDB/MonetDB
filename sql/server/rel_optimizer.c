/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0.  If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 *
 * Copyright 1997 - July 2008 CWI, August 2008 - 2022 MonetDB B.V.
 */

#include "monetdb_config.h"
#include "rel_optimizer.h"
#include "rel_optimizer_private.h"
#include "rel_rel.h"
#include "rel_basetable.h"
#include "rel_exp.h"
#include "rel_prop.h"
#include "rel_dump.h"
#include "rel_select.h"
#include "rel_planner.h"
#include "rel_propagate.h"
#include "rel_distribute.h"
#include "rel_rewriter.h"
#include "sql_mvc.h"
#include "sql_privileges.h"

static sql_rel *
rel_properties(visitor *v, sql_rel *rel)
{
	global_props *gp = (global_props*)v->data;

	/* Don't flag any changes here! */
	gp->cnt[(int)rel->op]++;
	gp->needs_distinct |= need_distinct(rel);
	if (gp->instantiate && is_basetable(rel->op)) {
		mvc *sql = v->sql;
		sql_table *t = (sql_table *) rel->l;
		sql_part *pt;

		/* If the plan has a merge table or a child of one, then rel_merge_table_rewrite has to run */
		gp->needs_mergetable_rewrite |= (isMergeTable(t) || (t->s && t->s->parts && (pt = partition_find_part(sql->session->tr, t, NULL))));
		gp->needs_remote_replica_rewrite |= (isRemote(t) || isReplicaTable(t));
	} else if (is_join(rel->op)) {
		/* check for setjoin rewrite */
		if (!list_empty(rel->attr)) {
			gp->needs_setjoin_rewrite = 1;
			return rel;
		}
		if (!list_empty(rel->exps)) {
			for (node *n = rel->exps->h; n ; n = n->next) {
				sql_exp *e = n->data;

				if (e->type == e_cmp && (e->flag == mark_in || e->flag == mark_notin)) {
					gp->needs_setjoin_rewrite = 1;
					return rel;
				}
			}
		}
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
		int skip = 0, allowed = 1;

		/* At the moment we throw an error in the optimizer, but later this rewriter should move out from the optimizers */
		if ((isMergeTable(pt) || isReplicaTable(pt)) && list_empty(pt->members))
			return sql_error(v->sql, 02, SQLSTATE(42000) "The %s '%s.%s' should have at least one table associated",
							 TABLE_TYPE_DESCRIPTION(pt->type, pt->properties), pt->s->base.name, pt->base.name);
		/* Do not include empty partitions */
		if (isTable(pt) && pt->access == TABLE_READONLY && !store->storage_api.count_col(v->sql->session->tr, ol_first_node(pt->columns)->data, 0))
			continue;

		if (!table_privs(v->sql, pt, PRIV_SELECT)) /* Test for privileges */
			allowed = 0;

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

			mt_col = ol_find_name(mt->columns, exp_name(e))->data;
			col = ol_fetch(pt->columns, mt_col->colnr);
			assert(e && e->type == e_column && col);
			if (!allowed && !column_privs(v->sql, col, PRIV_SELECT))
				return sql_error(v->sql, 02, SQLSTATE(42000) "The user %s SELECT permissions on table '%s.%s' don't match %s '%s.%s'", get_string_global_var(v->sql, "current_user"),
								 pt->s->base.name, pt->base.name, TABLE_TYPE_DESCRIPTION(mt->type, mt->properties), mt->s->base.name, mt->base.name);
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
			sql_exp *e = n->data, *a = exp_atom(v->sql->sa, atom_general(v->sql->sa, exp_subtype(e), NULL));
			exp_prop_alias(v->sql->sa, a, e);
			list_append(converted, a);
		}
		nrel = rel_project(v->sql->sa, nrel, converted);
	} else { /* Unionize children tables */
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
				nrel = rel_setop(v->sql->sa, nrel, next, op_union);
				rel_setop_set_exps(v->sql, nrel, rel_projections(v->sql, mt_rel, NULL, 1, 1), true);
				set_processed(nrel);
			} else {
				nrel = next;
			}
		}
	}
	return nrel;
}

/* rewrite merge tables into union of base tables */
static sql_rel *
rel_merge_table_rewrite_(visitor *v, sql_rel *rel)
{
	if (is_modify(rel->op)) {
		sql_query *query = query_create(v->sql);
		return rel_propagate(query, rel, &v->changes);
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
			if (is_union(nrel->op)) {
				rel = rel_inplace_setop(v->sql, rel, nrel->l, nrel->r, op_union, nrel->exps);
			} else if (is_select(nrel->op)) {
				rel = rel_inplace_select(rel, nrel->l, nrel->exps);
			} else if (is_basetable(nrel->op)) {
				rel = rel_inplace_basetable(rel, nrel);
			} else {
				assert(is_simple_project(nrel->op));
				rel = rel_inplace_project(v->sql->sa, rel, nrel->l, nrel->exps);
				rel->card = exps_card(nrel->exps);
			}
			nrel->l = nrel->r = NULL;
			rel_destroy(nrel);
			v->changes++;
		}
	}
	return rel;
}

bool
can_merge_table_rewrite(visitor *v, global_props *gp)
{
	(void) v;
	return gp->needs_mergetable_rewrite;
}

sql_rel *
rel_merge_table_rewrite(visitor *v, global_props *gp, sql_rel *rel)
{
	(void) gp;
	return rel_visitor_topdown(v, rel, &rel_merge_table_rewrite_);
}


static sql_rel *
rel_setjoins_2_joingroupby_(visitor *v, sql_rel *rel)
{
	if (rel && is_join(rel->op) && (!list_empty(rel->exps) || !list_empty(rel->attr))) {
		sql_exp *me = NULL;
		bool needed = false;

		if (!list_empty(rel->exps)) {
			for (node *n = rel->exps->h; n && !needed; n = n->next) {
				sql_exp *e = n->data;

				if (e->type == e_cmp && (e->flag == mark_in || e->flag == mark_notin)) {
					me = e;
					needed = true;
				}
			}
		}
		if (needed && rel->op == op_join && list_empty(rel->attr)) {
			rel->op = (me->flag == mark_in)?op_semi:op_anti;
			return rel;
		}
		if (needed || !list_empty(rel->attr)) {
			assert(needed || !list_empty(rel->attr));
			sql_exp *nequal = NULL;
			sql_exp *lid = NULL, *rid = NULL;
			sql_rel *l = rel->l, *p = rel;
			sql_rel *pp = NULL; /* maybe one project in between (TODO keep list) */

			if (me && rel->op == op_left) {
				/* find parent of join involving the right hand side of the mark expression */
				sql_rel *c = p->r;
				while (c) {
					if (is_join(c->op) && !is_processed(c) && rel_find_exp(c->r, me->r)) {
						p = c;
						c = p->r;
					} if (!pp && is_project(c->op) && c->l && rel_find_exp(c->l, me->r)) {
						pp = c;
						c = c->l;
					} else {
						c = NULL;
					}
				}
			}
			if (p && p->r == pp)
				pp = NULL;

			if (!(rel->l = l = rel_add_identity(v->sql, l, &lid)))
				return NULL;
			if (rel->op == op_left) {
				if (!(p->r = rel_add_identity(v->sql, p->r, &rid)))
					return NULL;
				if (pp)
					list_append(pp->exps, exp_ref(v->sql, rid));
			}

			list *aexps = sa_list(v->sql->sa);
			if (!list_empty(rel->exps)) {
				for (node *n = rel->exps->h; n;) {
					node *next = n->next;
					sql_exp *e = n->data;

					if (e->type == e_cmp && (e->flag == mark_in || e->flag == mark_notin)) {
						sql_exp *le = e->l, *re = e->r, *ne = NULL;
						sql_subfunc *ea = sql_bind_func3(v->sql, "sys", e->flag==mark_in?"anyequal":"allnotequal",
														 exp_subtype(le), exp_subtype(re), rid ? exp_subtype(rid) : NULL, F_AGGR, true);

						if (rid) {
							sql_exp *rid_ref = exp_ref(v->sql, rid);
							ne = exp_aggr3(v->sql->sa, le, re, rid_ref, ea, 0, 0, CARD_AGGR, has_nil(le));
						} else {
							ne = exp_aggr2(v->sql->sa, le, re, ea, 0, 0, CARD_AGGR, has_nil(le));
						}
						append(aexps, ne);
						nequal = ne;
						list_remove_node(rel->exps, NULL, n);
					}
					n = next;
				}
			}

			if (!list_empty(rel->attr)) {
				sql_exp *a = rel->attr->h->data;

				exp_setname(v->sql->sa, nequal, exp_find_rel_name(a), exp_name(a));
				rel->attr = NULL;
			} else {
				exp_label(v->sql->sa, nequal, ++v->sql->label);
			}
			list *lexps = rel_projections(v->sql, l, NULL, 1, 1);
			aexps = list_merge(aexps, lexps, (fdup)NULL);
			if (rel_is_ref(rel)) {
				sql_rel *l = rel_dup_copy(v->sql->sa, rel);
				rel = rel_inplace_groupby(rel, l, list_append(sa_list(v->sql->sa), exp_ref(v->sql, lid)), aexps);
			} else {
				rel = rel_groupby(v->sql, rel, list_append(sa_list(v->sql->sa), exp_ref(v->sql, lid)));
				rel->exps = aexps;
				set_processed(rel);
			}
		}
	}
	return rel;
}

bool
can_setjoins_2_joingroupby(visitor *v, global_props *gp)
{
	(void) v;
	return gp->needs_setjoin_rewrite;
}

sql_rel *
rel_setjoins_2_joingroupby(visitor *v, global_props *gp, sql_rel *rel)
{
	(void) gp;
	return rel_visitor_bottomup(v, rel, &rel_setjoins_2_joingroupby_);
}


/* the definition of a single SQL optimizer */
typedef struct sql_optimizer {
	const char *name;
	bool (*can_run_optimizer)(visitor *v, global_props *gp);
	sql_rel *(*run_optimizer)(visitor *v, global_props *gp, sql_rel *rel);
} sql_optimizer;

/* these optimizers/rewriters run in a cycle loop */
const sql_optimizer pre_sql_optimizers[] = {
	{"split_select", can_split_select, rel_split_select},
	{"push_project_down", can_push_project_down, rel_push_project_down},
	{"merge_projects", can_merge_projects, rel_merge_projects},
	{"push_project_up", can_push_project_up, rel_push_project_up},
	{"split_project", can_split_project, rel_split_project},
	{"remove_redundant_join", can_remove_redundant_join, rel_remove_redundant_join},
	{"simplify_math", can_simplify_math, rel_simplify_math},
	{"optimize_exps", can_optimize_exps, rel_optimize_exps},
	{"optimize_select_and_joins_bottomup", can_optimize_select_and_joins_bottomup, rel_optimize_select_and_joins_bottomup},
	{"project_reduce_casts", can_project_reduce_casts, rel_project_reduce_casts},
	{"optimize_unions_bottomup", can_optimize_unions_bottomup, rel_optimize_unions_bottomup},
	{"optimize_projections", can_optimize_projections, rel_optimize_projections},
	{"optimize_joins", can_optimize_joins, rel_optimize_joins},
	{"join_order", can_join_order, rel_join_order},
	{"optimize_semi_and_anti", can_optimize_semi_and_anti, rel_optimize_semi_and_anti},
	{"optimize_select_and_joins_topdown", can_optimize_select_and_joins_topdown, rel_optimize_select_and_joins_topdown},
	{"optimize_unions_topdown", can_optimize_unions_topdown, rel_optimize_unions_topdown},
	{"dce", can_dce, rel_dce},
	{"push_func_and_select_down", can_push_func_and_select_down, rel_push_func_and_select_down},
	{"push_topn_and_sample_down", can_push_topn_and_sample_down, rel_push_topn_and_sample_down},
	{"distinct_project2groupby", can_distinct_project2groupby, rel_distinct_project2groupby},
	{"merge_table_rewrite", can_merge_table_rewrite, rel_merge_table_rewrite},
	{NULL, NULL, NULL}
};

/* these optimizers/rewriters only run once after the cycle loop */
const sql_optimizer post_sql_optimizers[] = {
	{"push_select_up", can_push_select_up, rel_push_select_up}, /* run rel_push_select_up only once at the end to avoid an infinite optimization loop */
	{"setjoins_2_joingroupby", can_setjoins_2_joingroupby, rel_setjoins_2_joingroupby},
	{NULL, NULL, NULL}
};

/* make sure the outer project (without order by or distinct) has all the aliases */
static sql_rel *
rel_keep_renames(mvc *sql, sql_rel *rel)
{
	if (!rel || !is_simple_project(rel->op) || (!rel->r && !need_distinct(rel)) || list_length(rel->exps) <= 1)
		return rel;

	int needed = 0;
	for(node *n = rel->exps->h; n && !needed; n = n->next) {
		sql_exp *e = n->data;

		if (exp_name(e) && (e->type != e_column || strcmp(exp_name(e), e->r) != 0))
			needed = 1;
	}
	if (!needed)
		return rel;

	list *new_outer_exps = sa_list(sql->sa);
	list *new_inner_exps = sa_list(sql->sa);
	for(node *n = rel->exps->h; n; n = n->next) {
		sql_exp *e = n->data, *ie, *oe;
		const char *rname = exp_relname(e);
		const char *name = exp_name(e);

		exp_label(sql->sa, e, ++sql->label);
		ie = e;
		oe = exp_ref(sql, ie);
		exp_setname(sql->sa, oe, rname, name);
		append(new_inner_exps, ie);
		append(new_outer_exps, oe);
	}
	rel->exps = new_inner_exps;
	rel = rel_project(sql->sa, rel, new_outer_exps);
	return rel;
}

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

static sql_rel *
run_optimizer_set(visitor *v, sql_rel *rel, global_props *gp, const sql_optimizer *set)
{
	for (int i = 0 ; set[i].name ; i++) {
		if (set[i].can_run_optimizer(v, gp))
			rel = set[i].run_optimizer(v, gp, rel);
	}
	return rel;
}

/* 'instantiate' means to rewrite logical tables: (merge, remote, replica tables) */
sql_rel *
rel_optimizer(mvc *sql, sql_rel *rel, int instantiate, int value_based_opt, int storage_based_opt)
{
	global_props gp = (global_props) {.cnt = {0}, .instantiate = (uint8_t)instantiate, .opt_cycle = 0};
	visitor v = { .sql = sql, .value_based_opt = value_based_opt, .storage_based_opt = storage_based_opt, .changes = 1, .data = &gp };

	if (!(rel = rel_keep_renames(sql, rel)))
		return rel;

	for ( ;rel && gp.opt_cycle < 20 && v.changes; gp.opt_cycle++) {
		v.changes = 0;
		gp = (global_props) {.cnt = {0}, .instantiate = (uint8_t)instantiate, .opt_cycle = gp.opt_cycle};
		rel = rel_visitor_topdown(&v, rel, &rel_properties); /* collect relational tree properties */
		gp.opt_level = calculate_opt_level(sql, rel);
		if (gp.opt_level == 0 && !gp.needs_mergetable_rewrite)
			break;
		rel = run_optimizer_set(&v, rel, &gp, pre_sql_optimizers);
	}
#ifndef NDEBUG
	assert(gp.opt_cycle < 20);
#endif

	/* these optimizers run statistics gathered by the last optimization cycle */
	rel = run_optimizer_set(&v, rel, &gp, post_sql_optimizers);

	/* merge table rewrites may introduce remote or replica tables */
	/* at the moment, make sure the remote table rewriters always run last */
	if (gp.needs_mergetable_rewrite || gp.needs_remote_replica_rewrite) {
		rel = rel_visitor_bottomup(&v, rel, &rel_rewrite_remote);
		rel = rel_visitor_bottomup(&v, rel, &rel_rewrite_replica);
		rel = rel_visitor_bottomup(&v, rel, &rel_remote_func);
	}
	return rel;
}
