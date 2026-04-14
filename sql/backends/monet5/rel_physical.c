/*
 * SPDX-License-Identifier: MPL-2.0
 *
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0.  If a copy of the MPL was not distributed with this
 * file, You can obtain one at https://mozilla.org/MPL/2.0/.
 *
 * For copyright information, see the file debian/copyright.
 */

#include "monetdb_config.h"
#include "rel_physical.h"
#include "rel_optimizer_private.h"
#include "rel_rewriter.h"
#include "rel_exp.h"
#include "rel_rel.h"
#include "sql_storage.h"
#include "sql_scenario.h"
#include "rel_bin.h"
#include "bin_partition_by_value.h"

#define IS_ORDER_BASED_AGGR(fname, argc) (\
				(argc == 2 && (strcmp((fname), "quantile") == 0 || strcmp((fname), "quantile_avg") == 0)) || \
				(argc == 1 && (strcmp((fname), "median") == 0 || strcmp((fname), "median_avg") == 0)))

static int do_oahash_join(sql_rel *rel);

/* Returns the row count of a base table or any count info we can get fom the
 * PROP_COUNT of this 'rel' (i.e.  get_rel_count()). */
static lng
rel_getcount(mvc *sql, sql_rel *rel)
{
	if (!sql->session->tr)
		return 0;

	switch(rel->op) {
	case op_basetable: {
		sql_table *t = rel->l;

		if (t && isTable(t) && t->persistence != SQL_DECLARED_TABLE) {
			sqlstore *store = sql->session->tr->store;
			lng nr = (lng)store->storage_api.count_col(sql->session->tr, ol_first_node(t->columns)->data, 0);
			assert(nr >= 0);
			return nr;
		}
		return 0;
	}
	case op_groupby:
		if (rel->l && rel->r)
			return rel_getcount(sql, rel->l);
		return 1; /* Global GROUP BY always returns 1 row. */
	case op_munion:
		if (rel->l) {
			BUN cnt = 0;
			list *l = rel->l;
			for (node *n = l->h; n; n = n->next) {
				BUN ncnt = rel_getcount(sql, n->data);
				if (ncnt != BUN_NONE)
					cnt += ncnt;
			}
			return cnt;
		}
		return 1;
	default:
		if (rel->l)
			return rel_getcount(sql, rel->l);
		if (rel->p)
			return get_rel_count(rel);
		return 0;
	}
}

static void
find_basetables(mvc *sql, sql_rel *rel, list *tables )
{
	if (mvc_highwater(sql)) {
		(void) sql_error(sql, 10, SQLSTATE(42000) "Query too complex: running out of stack space");
		return;
	}

	if (!rel)
		return;
	switch (rel->op) {
	case op_basetable: {
		sql_table *t = rel->l;

		if (t && isTable(t))
			append(tables, rel);
		break;
	}
	case op_table:
		if (IS_TABLE_PROD_FUNC(rel->flag) || rel->flag == TABLE_FROM_RELATION)
			if (rel->l)
				find_basetables(sql, rel->l, tables);
		break;
	case op_join:
	case op_left:
	case op_right:
	case op_full:
	case op_inter:
	case op_except:
	case op_insert:
	case op_update:
	case op_delete:
		if (rel->l)
			find_basetables(sql, rel->l, tables);
		if (rel->r)
			find_basetables(sql, rel->r, tables);
		break;
	case op_munion:
		assert(rel->l);
		for (node *n = ((list*)rel->l)->h; n; n = n->next)
			find_basetables(sql, n->data, tables);
		break;
	case op_semi:
	case op_anti:
	case op_groupby:
	case op_project:
	case op_select:
	case op_topn:
	case op_sample:
	case op_truncate:
		if (rel->l)
			find_basetables(sql, rel->l, tables);
		break;
	case op_ddl:
		if (rel->flag == ddl_output || rel->flag == ddl_create_seq || rel->flag == ddl_alter_seq/* || rel->flag == ddl_alter_table || rel->flag == ddl_create_table || rel->flag == ddl_create_view*/) {
			if (rel->l)
				find_basetables(sql, rel->l, tables);
		} else if (rel->flag == ddl_list || rel->flag == ddl_exception) {
			if (rel->l)
				find_basetables(sql, rel->l, tables);
			if (rel->r)
				find_basetables(sql, rel->r, tables);
		}
		break;
	case op_buildhash:
	case op_probehash:
	case op_partition:
		return ;
	}
}

static int
_rel_partition(mvc *sql, sql_rel *rel)
{
	list *tables = sa_list(sql->sa);
	/* find basetable relations */
	/* mark one (largest) with rel->partition */
	find_basetables(sql, rel, tables);
	if (list_length(tables)) {
		sql_rel *r;
		node *n;
		int i, mi = 0;
		lng *sizes = SA_NEW_ARRAY(sql->sa, lng, list_length(tables)), m = 0;

		for(i=0, n = tables->h; n; i++, n = n->next) {
			r = n->data;
			sizes[i] = rel_getcount(sql, r);
			if (sizes[i] > m) {
				m = sizes[i];
				mi = i;
			}
		}
		for(i=0, n = tables->h; i<mi; i++, n = n->next)
			;
		r = n->data;
		/*  TODO, we now pick first (okay?)! In case of self joins we need to pick the correct table */
		r->partition = 1;
	}
	return 0;
}

static int
has_groupby(sql_rel *rel)
{
	if (!rel)
		return 0;

	switch (rel->op) {
		case op_groupby:
			return 1;
		case op_join:
		case op_left:
		case op_right:
		case op_full:

		case op_semi:
		case op_anti:

		case op_inter:
		case op_except:
			return has_groupby(rel->l) || has_groupby(rel->r);
		case op_munion:
			for (node *n = ((list*)rel->l)->h; n; n = n->next)
				if (has_groupby(n->data))
					return 1;
			return 0;
		case op_project:
		case op_select:
		case op_topn:
		case op_sample:
		case op_buildhash:
		case op_probehash:
		case op_partition:
			return has_groupby(rel->l);
		case op_insert:
		case op_update:
		case op_delete:
		case op_truncate:
			return has_groupby(rel->r);
		case op_ddl:
			if (rel->flag == ddl_output || rel->flag == ddl_create_seq || rel->flag == ddl_alter_seq || rel->flag == ddl_alter_table || rel->flag == ddl_create_table || rel->flag == ddl_create_view)
				return has_groupby(rel->l);
			if (rel->flag == ddl_list || rel->flag == ddl_exception)
				return has_groupby(rel->l) || has_groupby(rel->r);
			return 0;
		case op_table:
			if (IS_TABLE_PROD_FUNC(rel->flag) || rel->flag == TABLE_FROM_RELATION)
				return has_groupby(rel->l);
			return 0;
		case op_basetable:
			return 0;
	}
	return 0;
}

/* To start parallel processing within a (query plan) graph, we need to mark
 * the places where partitioning is needed, and where to start or end a
 * parallel block.
 * SPB: Start Parallel Block
 * EPB: End Parallel Block
 * A nested parallel blocks is lifted by an extra reference, making sure the inner
 * block is executed before the outer block.
 */
#define SPB 2
#define EPB 3

static sql_rel *
rel_partition(mvc *sql, sql_rel *rel)
{
	if (mvc_highwater(sql))
		return sql_error(sql, 10, SQLSTATE(42000) "Query too complex: running out of stack space");

	switch (rel->op) {
	case op_basetable:
	case op_sample:
		rel->partition = 1;
		break;
	case op_project:
	case op_select:
	case op_groupby:
	case op_topn:
		if (rel->l)
			rel_partition(sql, rel->l);
		break;
	case op_semi:
	case op_anti:

	case op_inter:
	case op_except:
		if (rel->l)
			rel_partition(sql, rel->l);
		if (rel->r)
			rel_partition(sql, rel->r);
		break;
	case op_munion:
		for (node *n = ((list*)rel->l)->h; n; n = n->next)
			rel_partition(sql, n->data);
		break;
	case op_insert:
	case op_update:
	case op_delete:
	case op_truncate:
		if (rel->r /*&& rel->card <= CARD_AGGR*/)
			rel_partition(sql, rel->r);
		break;
	case op_join:
	case op_left:
	case op_right:
	case op_full:
		if (has_groupby(rel->l) || has_groupby(rel->r)) {
			if (rel->l)
				rel_partition(sql, rel->l);
			if (rel->r)
				rel_partition(sql, rel->r);
		} else {
			_rel_partition(sql, rel);
		}
		break;
	case op_ddl:
		if (rel->flag == ddl_output || rel->flag == ddl_create_seq || rel->flag == ddl_alter_seq || rel->flag == ddl_alter_table || rel->flag == ddl_create_table || rel->flag == ddl_create_view) {
			if (rel->l)
				rel_partition(sql, rel->l);
		} else if (rel->flag == ddl_list || rel->flag == ddl_exception) {
			if (rel->l)
				rel_partition(sql, rel->l);
			if (rel->r)
				rel_partition(sql, rel->r);
		}
		break;
	case op_table:
		if ((IS_TABLE_PROD_FUNC(rel->flag) || rel->flag == TABLE_FROM_RELATION) && rel->l)
			rel_partition(sql, rel->l);
		sql_exp *op = rel->r;
        if (rel->flag != TRIGGER_WRAPPER && op) {
            sql_subfunc *f = op->f;
            if (f->func->pipeline) {
				if (strcmp(f->func->base.name, "file_loader") == 0 || strcmp(f->func->base.name, "copyfrom") == 0) {
					rel = rel_dup(rel);
					f->pipeline = true;
				}
            }
		}
		break;
	default:
		assert(0);
		break;
	}
	return rel;
}

static sql_rel *
rel_add_orderby(visitor *v, sql_rel *rel)
{
	if (is_groupby(rel->op)) {
		if (list_empty(rel->exps)) /* empty */
			return rel_project_exp(v->sql, exp_atom_bool(v->sql->sa, 1));
		if (rel->exps && !rel->r) { /* find quantiles */
			sql_exp *obe = NULL, *oberef = NULL;
			for(node *n = rel->exps->h; n; n = n->next) {
				sql_exp *e = n->data;

				if (is_aggr(e->type)) {
					sql_subfunc *af = e->f;
					list *aa = e->l;

					/* for now we only handle one sort order */
					if (aa && IS_ORDER_BASED_AGGR(af->func->base.name, list_length(aa))) {
						sql_exp *nobe = aa->h->data;
						if (nobe && !obe) {
							sql_rel *l = rel->l = rel_project(v->sql->sa, rel->l, rel_projections(v->sql, rel->l, NULL, 1, 1));
							obe = nobe;
							oberef = nobe;
							if (l) {
								if (!is_alias(nobe->type)) {
									oberef = nobe = exp_label(v->sql->sa, exp_copy(v->sql, nobe), ++v->sql->label);
									append(l->exps, nobe);
								}
								set_nulls_first(nobe);
								set_ascending(nobe);
								aa->h->data = exp_ref(v->sql, nobe);
								list *o = l->r = sa_list(v->sql->sa);
								if (o)
									append(o, nobe);
							}
						} else if (exp_match_exp(nobe, obe)) {
							aa->h->data = exp_ref(v->sql, oberef);
						}
					}
				}
			}
			return rel;
		}
	}
	return rel;
}

static sql_exp *
exp_timezone(visitor *v, sql_rel *rel, sql_exp *e, int depth)
{
	(void)depth;
	(void)rel;
	if (e && e->type == e_func) {
		list *l = e->l;
		sql_subfunc *f = e->f;
		const char *fname = f->func->base.name;
		if (list_length(l) == 2) {
		   if (strcmp(fname, "timestamp_to_str") == 0 || strcmp(fname, "time_to_str") == 0) {
                sql_exp *e = l->h->data;
                sql_subtype *t = exp_subtype(e);
                if (t->type->eclass == EC_TIMESTAMP_TZ || t->type->eclass == EC_TIME_TZ) {
                    sql_exp *offset = exp_atom_lng(v->sql->sa, v->sql->timezone);
                    list_append(l, offset);
                }
            } else if (strcmp(fname, "str_to_timestamp") == 0 || strcmp(fname, "str_to_time") == 0 || strcmp(fname, "str_to_date") == 0) {
                sql_exp *offset = exp_atom_lng(v->sql->sa, v->sql->timezone);
                list_append(l, offset);
            }
		}
	}
	return e;
}

static bool
rel_groupby_partition_safe(sql_rel *rel)
{
	if (rel->l) {
		sql_rel *l = rel->l;
		if (is_simple_project(l->op) && list_empty(l->exps))
			return false;
	}
	if (list_empty(rel->r)) {
		for(node *n = rel->exps->h; n; n = n->next ) {
			sql_exp *e = n->data;

			if (is_aggr(e->type)) {
				sql_subfunc *sf = e->f;

				if (!(strcmp(sf->func->base.name, "min") == 0 || strcmp(sf->func->base.name, "max") == 0 ||
					strcmp(sf->func->base.name, "avg") == 0 || strcmp(sf->func->base.name, "count") == 0 ||
					strcmp(sf->func->base.name, "null") == 0 ||
					strcmp(sf->func->base.name, "sum") == 0 || strcmp(sf->func->base.name, "prod") == 0))
					return false;
			}
		}
	}
	if (!list_empty(rel->r)) {
		for(node *n = rel->exps->h; n; n = n->next ) {
			sql_exp *e = n->data;

			if (is_aggr(e->type)) {
				sql_subfunc *sf = e->f;
				if (sf->func->lang == FUNC_LANG_R || sf->func->lang == FUNC_LANG_PY)
					return false;
			}
		}
	}
	return true;
}

static int
do_oahash_join(sql_rel *rel)
{
	ATOMIC_TYPE oahash_enabled = (1U<<19);
	if (!(GDKdebug & oahash_enabled))
		return 0;

	// TODO full outer join
	//if (rel->op == op_full)
	//	return 0;

	// TODO groupjoin other then mark/exist
    if (list_length(rel->attr) == 1) {
        sql_exp *e = rel->attr->h->data;
        if (exp_is_atom(e))
			return 1;
		return 0;
    }
	return 1;
}

static void
find_payload_exps(mvc *sql, list **exps_hsh, list **exps_prb, const list *exps, sql_rel *rel_hsh, sql_rel *rel_prb, const list *attr)
{
	assert(exps);

	/* Find out if an expression of the exps belong to rel_hsh or
	 * rel_prb or is a constant. */
	for (node *n = exps->h; n; n = n->next) { /* TODO handle consts seperate */
		sql_exp *e = n->data, *ne = NULL;

		if (list_find_exp(attr, e))
			continue;
		if (exp_is_atom(e))
			continue;
		if ((ne = rel_find_exp(rel_prb->l, e)) != NULL) {
			if (exp_is_atom(ne))
				ne = e;
			append(*exps_prb, exp_ref(sql, ne));
		} else if (exps_hsh) {
			ne = rel_find_exp(rel_hsh, e);
			if (exp_is_atom(ne))
				ne = e;
			assert(ne);
			append(*exps_hsh, exp_ref(sql, ne));
		}
	}
}

static void
find_cmp_exps(list **exps_hsh, list **exps_prb, const list *exps, sql_rel *rel_hsh, sql_rel *rel_prb)
{
	assert(exps);

	/* Find out if a sub-expression of the (compare) exps belong to rel_hsh or
	 * rel_prb or is a constant. */
	(void)rel_prb;
	for (node *n = exps->h; n; n = n->next) {
		sql_exp *e = n->data;

		assert(e->type == e_cmp);

		/* search first for the not-atom exp, otherwise rel_find_exp()
		 * incorrectly returns TRUE for an atom-typed exp */
		if (exp_is_atom(e->l) || e->flag != cmp_equal || exp_is_atom(e->r)) {
			assert(0);
		} else {
			if (rel_find_exp(rel_hsh, e->r)) {
				append(*exps_hsh, e->r);
				append(*exps_prb, e->l);
			} else {
				assert(rel_find_exp(rel_prb, e->r));
				append(*exps_hsh, e->l);
				append(*exps_prb, e->r);
			}
		}
	}
}

static sql_rel *
rel_build_partition(visitor *v, sql_rel *rel)
{
	sql_rel *r = rel_create(v->sql->sa);
	if (is_select(rel->op)) {
		list *exps = rel_projections(v->sql, rel, NULL, 1, 1);
		assert(!list_empty(exps));
		rel = rel_project(v->sql->sa, rel, exps);
	}
	r->exps = rel_projections(v->sql, rel, NULL, 1, 1);
	r->op = op_partition;
	r->l = rel;
	return r;
}

static sql_rel *
rel_probehash(visitor *v, sql_rel *rel, sql_rel **iprj)
{
	/* todo inplace for hash sharing */
	sql_rel *r = rel_create(v->sql->sa);
	if (0 && rel_is_ref(rel)) {
		sql_rel *l = r;
		*l = *rel;
		l->ref.refcnt = 1;
		if (is_select(rel->op)) {
			list *exps = rel_projections(v->sql, l, NULL, 1, 1);
			assert(!list_empty(exps));
			l = rel_project(v->sql->sa, l, exps);
			if (iprj)
				*iprj = l;
		}
		r = rel;
		*r = (sql_rel){ .ref.refcnt = r->ref.refcnt, .op = op_probehash, .l = l };
	} else {
		if (is_select(rel->op)) {
			list *exps = rel_projections(v->sql, rel, NULL, 1, 1);
			assert(!list_empty(exps));
			rel = rel_project(v->sql->sa, rel, exps);
			if (iprj)
				*iprj = rel;
		}
		r->op = op_probehash;
		r->l = rel;
	}
	return r;
}

static sql_rel *
rel_buildhash(visitor *v, sql_rel *rel, sql_rel **iprj, bool crossproduct)
{
	if (crossproduct && rel->op == op_basetable) {
		return rel_dup(rel);
	}
	/* Inplace for hash sharing */
	sql_rel *r = rel_create(v->sql->sa);
	if (0 && rel_is_ref(rel)) {
		sql_rel *l = r;
		*l = *rel;
		l->ref.refcnt = 1;
		if (is_select(rel->op)) {
			list *exps = rel_projections(v->sql, l, NULL, 1, 1);
			assert(!list_empty(exps));
			l = rel_project(v->sql->sa, l, exps);
			if (iprj)
				*iprj = l;
		}
		r = rel;
		*r = (sql_rel){ .ref.refcnt = r->ref.refcnt, .op = op_buildhash, .l = l };
	} else {
		if (is_select(rel->op)) {
			list *exps = rel_projections(v->sql, rel, NULL, 1, 1);
			assert(!list_empty(exps));
			rel = rel_project(v->sql->sa, rel, exps);
			if (iprj)
				*iprj = rel;
		}
		r->op = op_buildhash;
		r->l = rel;
	}
	r = rel_dup(r);
	return r;
}

static list *
clean_exp_list(list *exps, list *nl, sql_rel *inner)
{
	if (list_empty(exps))
		return nl;
	for(node *n = exps->h; n; n = n->next) {
		sql_exp *e = n->data;
		if (e->type == e_convert) {
			sql_exp *i = e->l;
			if (i->alias.label != i->nid) {
				i = rel_find_exp(inner, i);
				append(nl, i);
			} else
				append(nl, i);
		}
		if (e->type == e_column)
			append(nl, e);
		if (e->type == e_func)
			(void)clean_exp_list(exps, e->l, inner);
	}
	return nl;
}

static int
rel_pipeline(visitor *v, sql_rel *rel, bool materialize, int pb)
{
	sql_rel *p = v->parent;
	int res = 0, lres = 0, rres = 0;

	if (v->opt >= 0 && rel->opt >= v->opt) /* only once */
        return 0;

	if (mvc_highwater(v->sql)) {
		sql_error(v->sql, 10, SQLSTATE(42000) "Query too complex: running out of stack space");
		return 0;
	}
	if (find_prop(rel->p, PROP_REMOTE))
		return 0;
	if (rel_is_ref(rel))
		materialize = true;

	v->parent = rel;
	if (is_basetable(rel->op)) {
		if (pb) {
			rel->spb = 1;
			rel->partition = 1;
			res = SPB;
		}
	} else if (is_groupby(rel->op)) {
		bool safe = rel_groupby_partition_safe(rel);
		if (safe && rel_groupby_partition(rel)) {
			sql_rel *p = rel->l = rel_build_partition(v, rel->l);
			p->attr = exps_copy(v->sql, rel->r);
		}
		if (rel->l)
			/* if `safe`, process this GROUP BY + subtree in a `pb`. */
			res = rel_pipeline(v, rel->l, !safe, safe?SPB:0);
		if (safe) {
			rel->parallel = 1;
			if (pb) {
				rel_dup(rel);
				res = 0;
			} else {
				/* GROUP BY is a blocking operation, so it always ends a `pb`
				 * if it has started one.
				 */
				res = EPB;
			}
		} else if (res == SPB) {
			rel_dup(rel->l); /* materialize ! */
		}
	} else if (is_topn(rel->op)) {
		/* e.g. pp is not useful for "SELECT 42 LIMIT 2" */
		bool pp_useful = (get_rel_count(rel->l) > 1) && topn_limit(rel) /*&& !(list_length(rel->exps) > 1)*/ /* no offset */;
		//pp_useful &= !rel->grouped; /* grouped topn isn't pipelined yet */
		/* op_topn always has rel->l */
		res = rel_pipeline(v, rel->l, pp_useful, pp_useful?SPB/*:rel->grouped?0*/:0);
		if (pp_useful && res) { /* topn is blocking */
			rel->parallel = 1;
			if (pb) { /* nested */
				rel_dup(rel);
				res = 0;
				rel->partition = 1;
			}
			res = EPB;
		} else if (pb) {
			rel->spb = 1;
		}
		/* else: !pp_useful: either there was no 'pb' at all, or a 'pb'
		 * has been started in the subtree (e.g. by a GROUP BY). In the
		 * 2nd case, don't try to end the 'pb', instead, leave it to
		 * the upper tree to end it, and this topN might be computed
		 * multiple times */
	} else if (is_simple_project(rel->op) || is_select(rel->op) || is_sample(rel->op)) {
		if (pb && (is_simple_project(rel->op) || is_select(rel->op)) && exps_have_unsafe(rel->exps, 1, false)) {
			if (p)
				rel_dup(rel);
			if (rel->l)
				res = rel_pipeline(v, rel->l, materialize, 0);
			/*
			if (pb && !rel_is_ref(rel)) {
				rel->spb = 1;
				res = SPB;
			}
			*/
			res = EPB;
		} else {
			if (rel->l)
				res = rel_pipeline(v, rel->l, materialize, pb?pb:!list_empty(rel->r)?SPB:0);
			/* handle streaming projections and blocking order by */
			if (list_empty(rel->r) || (p && p->op == op_topn && topn_limit(p))) {
				if (res && p && p->op == op_project && exps_have_unsafe(p->exps, 1, false)) {
					rel_dup(rel);
					res = 0;
				} else
				if (pb) {
					sql_rel *l = rel->l;
					if (!res && l && is_groupby(l->op) && l->l) {
						sql_rel *p = l->l;
						if (p->op == op_partition) { /* TODO pass that we can handle a partitioned result (l->partition
						= 1), needs a check if next operator can handle that */
							//l->partition = 1;
							res = SPB;
						}
					}
					if (rel->spb)
						res = SPB;
				}
			} else {
				rel->parallel = 1;
				if (pb || (p && p->op == op_topn && !topn_limit(p))
					   || (p && p->op == op_project && exps_have_unsafe(p->exps, 1, false))
					   || !list_empty(rel->r)) { /* nested */
					if (p)
						rel_dup(rel);
					res = 0;
					rel->partition = 1;
				} else {
					res = EPB;
				}
			}
		}
	} else if (is_semi(rel->op)) {
		list *eq_exps = sa_list(v->sql->sa);
		list *other = sa_list(v->sql->sa);
		split_join_exps_pp(rel, eq_exps, other, true);
		bool needs_payload = (!list_empty(other));
		bool need_all = false;
		bool cross = list_empty(eq_exps);

		rel->oahash = 2;

		sql_rel *rel_hsh = rel->r, *rel_prb = rel->l;
		list *found_exps_cmp_hsh = NULL;

		/* get full projection list from parent */
		assert(p);
		if (rel_hsh->op == op_buildhash)
			found_exps_cmp_hsh = rel_hsh->attr;
		list *exps_cmp_hsh = sa_list(v->sql->sa), *exps_cmp_prb = sa_list(v->sql->sa);
		find_cmp_exps(&exps_cmp_hsh, &exps_cmp_prb, eq_exps, rel_hsh, rel_prb);

		if (found_exps_cmp_hsh) {
			printf("# todo needs check\n");
		} else {
			if (rel_is_ref(rel_hsh))
				need_all = true;
			rel->r = rel_hsh = rel_buildhash(v, rel_hsh, NULL, list_empty(eq_exps));
			rel_hsh->flag = (int)(needs_payload?op_join:op_semi);
		}
		rel->l = rel_prb = rel_probehash(v, rel_prb, NULL);
		rel_hsh->attr = cross?NULL:exps_cmp_hsh;
		rel_prb->attr = exps_cmp_prb;

		list *exps_hsh = NULL, *exps_prb = rel_prb->exps = sa_list(v->sql->sa);
		if (needs_payload)
			exps_hsh = sa_list(v->sql->sa);
		find_payload_exps(v->sql, &exps_hsh, &exps_prb, p->exps, rel_hsh, rel_prb, rel->attr);

		if (need_all && !is_base(rel_hsh->op))
			rel_hsh->exps = !rel_hsh->exps?rel_hsh->attr:list_distinct(list_merge(rel_hsh->exps, rel_hsh->attr, NULL), (fcmp) exp_equal, NULL);
		if (needs_payload) { /* TODO improve ie only keep attributes needed for 'other' exps */
			if (!is_base(rel_hsh->op)) {
				sql_rel *l = rel_hsh->l;
				if (!is_project(l->op) && !is_base(l->op))
					l = rel_hsh->l = rel_project(v->sql->sa, l, rel_projections(v->sql, l, NULL, 1, 1));
				list *nl = sa_list(v->sql->sa);
				for(node *n = l->exps->h; n; n = n->next)
					append(nl, exp_ref(v->sql, n->data));
				rel_hsh->exps = !rel_hsh->exps?nl:list_distinct(list_merge(rel_hsh->exps, nl, NULL), (fcmp) exp_equal, NULL);
			}
			if (!is_base(rel_prb->op)) {
				sql_rel *l = rel_prb->l;
				if (!is_project(l->op) && !is_base(l->op))
					l = rel_prb->l = rel_project(v->sql->sa, l, rel_projections(v->sql, l, NULL, 1, 1));
				list *nl = sa_list(v->sql->sa);
				for(node *n = l->exps->h; n; n = n->next)
					append(nl, exp_ref(v->sql, n->data));
				rel_prb->exps = !rel_prb->exps?nl:list_distinct(list_merge(rel_prb->exps, nl, NULL), (fcmp) exp_equal, NULL);
			}
		}

		(void) rel_pipeline(v, rel_prb, false, 1);
		if (rel_hsh && rel_hsh->op == op_buildhash)
			(void) rel_pipeline(v, rel_hsh, true, 1);

		rel->parallel = 1;
		if (pb)
			rel->spb = 1;
		res = SPB;
	} else if (rel->op == op_munion) {
		list *rels = rel->l;
		if (is_recursive(rel) || need_distinct(rel) || is_single(rel)) {
			res = 0;
		} else {
			int started_pb = 0;
			if (rel_is_ref(rel))
				pb = 0;
			else
				pb |= materialize;
			for(node *n = rels->h; n; n = n->next) {
				int lres = rel_pipeline(v, n->data, false, pb?SPB:0);
				if (lres == EPB) {
					rel->partition = 1;
					if (pb)
						rel_dup(n->data);
				}
				if (lres == SPB)
					started_pb = 1;
			}
			if (pb) {
				rel->parallel = 1;
				rel->spb = 1;
			} else if (started_pb) {
				rel->parallel = 1;
				rel->spb = 1;
			}
			res = pb;
		}
	} else if (is_set(rel->op)) {
		if (pb) { /* somewhat simplified */
			rel->spb = 1;
			res = SPB;
			if (rel->l)
				lres = rel_pipeline(v, rel->l, true, 0);
			if (rel->r)
				rres = rel_pipeline(v, rel->r, true, 0);
			if (pb)
				rel->spb = 1;
			else {
				if (!lres || !rres)
					res = 0;
				else
					res = pb;
			}
		}
	} else if (is_insert(rel->op) || is_update(rel->op) || is_delete(rel->op) || is_truncate(rel->op)) {
		if (rel->r)
			res = rel_pipeline(v, rel->r, false, pb);
		if (is_delete(rel->op) && !rel->r && pb)
			rel_dup(rel);
	} else if (is_join(rel->op)) {
		if (do_oahash_join(rel)) {
			list *eq_exps = sa_list(v->sql->sa);
			list *other = sa_list(v->sql->sa);
			if (!list_empty(rel->attr))
				rel->exps = get_simple_equi_joins_first(v->sql, rel, rel->exps);
			split_join_exps_pp(rel, eq_exps, other, true);

			sql_rel *l = rel->l, *r = rel->r;
			sql_rel *rel_hsh = NULL, *rel_prb = NULL, *iprj = NULL, *pprj = NULL;
			bool need_all = false;
			bool cross = list_empty(eq_exps);

			list *found_exps_cmp_hsh = NULL, *found_exps_prj_hsh = NULL;

			if (l->op == op_buildhash) {
				rel_hsh = l;
				rel_prb = r;
				rel->oahash = 1;
			} else if (r->op == op_buildhash) {
				rel_hsh = r;
				rel_prb = l;
				rel->oahash = 2;
			}

			if (!rel_hsh) {
				/* For both left-outer join and all single outer joins, we hash the RHS */
				if (rel->single || rel->op == op_left)
					rel->oahash = 2;
				else if (rel->op == op_right)
					rel->oahash = 2;
				else if (rel_getcount(v->sql, l) < rel_getcount(v->sql, r))
					rel->oahash = 1;
				else
					rel->oahash = 2;

				if (rel->oahash == 2) {
					rel_hsh = rel->r;
					rel_prb = rel->l;
				} else {
					assert (rel->oahash == 1);
					rel_hsh = rel->l;
					rel_prb = rel->r;
				}
				if (rel_is_ref(rel_hsh))
					need_all = true;
				rel_hsh = rel_buildhash(v, rel_hsh, &iprj, list_empty(eq_exps));
			   	rel_hsh->flag = (!list_empty(other) || list_empty(rel->attr))?op_join:op_semi;
				rel_hsh->single = rel->single;
			} else {
				found_exps_cmp_hsh = rel_hsh->attr;
				found_exps_prj_hsh = rel_hsh->exps;
			}
			rel_prb = rel_probehash(v, rel_prb, &pprj);
			if (rel->oahash == 2) {
				rel->l = rel_prb;
				rel->r = rel_hsh;
			} else {
				rel->l = rel_hsh;
				rel->r = rel_prb;
			}

			/* get full projection list from parent */
			assert(p);
			list *exps_cmp_hsh = NULL, *exps_cmp_prb = NULL;
			if (!list_empty(eq_exps)) {
				exps_cmp_hsh = sa_list(v->sql->sa);
				exps_cmp_prb = sa_list(v->sql->sa);
				find_cmp_exps(&exps_cmp_hsh, &exps_cmp_prb, eq_exps, rel_hsh, rel_prb);

				if (found_exps_cmp_hsh) { /* if not the same ?? */
					printf("# todo need to check hsh\n");
				} else {
					rel_hsh->attr = cross?NULL:exps_cmp_hsh;
				}
				rel_prb->attr = cross?NULL:exps_cmp_prb;
			}

			list *exps_hsh = sa_list(v->sql->sa), *exps_prb = sa_list(v->sql->sa);
			find_payload_exps(v->sql, &exps_hsh, &exps_prb, p->exps, rel_hsh, rel_prb, rel->attr);

			if (found_exps_prj_hsh) {
				printf("# todo need to check prj\n");
			} else if (!list_empty(exps_hsh) && !is_base(rel_hsh->op)) {
				rel_hsh->exps = exps_hsh;
			}
			rel_prb->exps = exps_prb;
			if (need_all && !is_base(rel_hsh->op)) /* add all exps */
				rel_hsh->exps = rel_projections(v->sql, rel_hsh->l, NULL, 1, 1);
			else if (!list_empty(other)) {
				if (!is_base(rel_hsh->op)) {
					sql_rel *l = rel_hsh->l;
					if (!is_project(l->op) && !is_base(l->op))
						l = rel_hsh->l = rel_project(v->sql->sa, l, rel_projections(v->sql, l, NULL, 1, 1));
					list *nl = sa_list(v->sql->sa);
					for(node *n = l->exps->h; n; n = n->next)
						append(nl, exp_ref(v->sql, n->data));
					rel_hsh->exps = !rel_hsh->exps?nl:list_distinct(list_merge(rel_hsh->exps, nl, NULL), (fcmp) exp_equal, NULL);
				}
			}

			if (!list_empty(other)) {
				if (!is_base(rel_prb->op)) {
					sql_rel *l = rel_prb->l;
					if (!is_project(l->op) && !is_base(l->op))
						l = rel_prb->l = rel_project(v->sql->sa, l, rel_projections(v->sql, l, NULL, 1, 1));
					list *nl = sa_list(v->sql->sa);
					for(node *n = l->exps->h; n; n = n->next)
						append(nl, exp_ref(v->sql, n->data));
					rel_prb->exps = !rel_prb->exps?nl:list_distinct(list_merge(rel_prb->exps, nl, NULL), (fcmp) exp_equal, NULL);
				}
			}
			if (iprj) {
				list *n = clean_exp_list(rel_hsh->attr, sa_list(v->sql->sa), iprj->l);
				n = list_merge(n, rel_hsh->exps, NULL);
				if (!list_empty(n))
					iprj->exps = n;
			}

			(void) rel_pipeline(v, rel_prb, false, 1);
			if (rel_hsh->op == op_buildhash)
				(void) rel_pipeline(v, rel_hsh, true, 1);

			rel->parallel = 1;
			if (pb)
				rel->spb = 1;
			res = SPB;
		} else {
			if (pb && is_outerjoin(rel->op))
				res = 0;
			/* For now we only try to partition in case of a equi-join.
			 * The other joins are too complex to handle. */
			else if (pb) { /* and rel->op == op_join */
				if (!rel->partition)
					res = _rel_partition(v->sql, rel);
				if (res) {
					int lres = rel_pipeline(v, rel->l, false, (rel->partition==1 && rel->spb)?pb:0);
					if (lres == EPB && pb)
						rel_dup(rel->l);
					int rres = rel_pipeline(v, rel->r, false, (rel->partition==2 && rel->spb)?pb:0);
					if (rres == EPB && pb)
						rel_dup(rel->r);
					if (pb)
						res = 0;
				}
				if (!res) {
					rel->spb = 1;
					res = SPB;
				}
			}
		}
	} else if (is_ddl(rel->op)) {
		if (rel->flag == ddl_output || rel->flag == ddl_create_seq || rel->flag == ddl_alter_seq || rel->flag == ddl_alter_table || rel->flag == ddl_create_table || rel->flag == ddl_create_view) {
			if (rel->l)
				res = rel_pipeline(v, rel->l, false, pb);
		} else if (rel->flag == ddl_list || rel->flag == ddl_exception) {
			if (rel->l)
				res = rel_pipeline(v, rel->l, false, pb);
			if (rel->r)
				res = rel_pipeline(v, rel->r, false, pb);
		}
	} else if (rel->op == op_table) {
		if ((IS_TABLE_PROD_FUNC(rel->flag) || rel->flag == TABLE_FROM_RELATION) && rel->l)
			res = rel_pipeline(v, rel->l, false, pb);
		sql_exp *op = rel->r;
        if (rel->flag != TRIGGER_WRAPPER && op) {
            sql_subfunc *f = op->f;
            if (/*f->func->lang == FUNC_LANG_INT &&*/ f->func->pipeline) {
				res = pb;
				if (pb) {
					f->pipeline = true;
					rel->parallel = 1;
					rel->spb = 1;
					res = SPB;
				} else {
					rel = rel_dup(rel);
					f->pipeline = true;
				}
            }
		}
	} else if (is_physical(rel->op)) {
		res = rel_pipeline(v, rel->l, false, pb);
	} else {
		assert(0);
	}
	v->parent = p;
	if (rel && v->opt >= 0)
        rel->opt = v->opt;
	if (rel_is_ref(rel))
		return 0;
	return res;
}


static sql_subfunc *
find_func( mvc *sql, char *name, list *exps )
{
	list * l = sa_list(sql->sa);
	node *n;

	for(n = exps->h; n; n = n->next)
		append(l, exp_subtype(n->data));
	return sql_bind_func_(sql, "sys", name, l, F_FUNC, false, true);

}

static sql_exp *
rel_find_aggr_exp(mvc *sql, sql_rel *rel, list *exps, sql_exp *e, char *name)
{
 	list *ea = e->l;
	sql_exp *a = NULL, *eae;
	node *n;

	(void)rel;
	if (list_length(ea) != 1)
		return NULL;
	eae = ea->h->data;
	if (eae->type != e_column)
		return NULL;
	for( n = exps->h; n; n = n->next) {
		a = n->data;

		if (a->type == e_aggr) {
			sql_subfunc *af = a->f;
			list *aa = a->l;

			/* TODO handle distinct and no-nil etc ! */
			if (strcmp(af->func->base.name, name) == 0 &&
				/* TODO handle count (has no args!!) */
			    aa && list_length(aa) == 1) {
				sql_exp *aae = aa->h->data;

				if (eae->type == e_column &&
				    ((!aae->l && !eae->l) ||
				    (aae->l && eae->l &&
				    strcmp(aae->l, eae->l) == 0)) &&
				    (aae->r && eae->r &&
				    strcmp(aae->r, eae->r) == 0))
					return exp_ref(sql, a);
			}
		}
	}
	return NULL;
}

static sql_exp *
find_aggr_exp(mvc *sql, list *exps, char *name)
{
	node *n;

	for( n = exps->h; n; n = n->next) {
		sql_exp *a = n->data;

		if (a->type == e_aggr) {
			sql_subfunc *af = a->f;

			if (strcmp(af->func->base.name, name) == 0)
				return exp_ref(sql, a);
		}
	}
	return NULL;
}

/* Filter out the 0's introducted by the parallel group by.
 * Must be done before using the values. */
static sql_rel *
rel_count_gt_zero(visitor *v, sql_rel *rel)
{
	mvc *sql = v->sql;
	if (is_groupby(rel->op) && !find_prop(rel->p, PROP_REMOTE)) {
		list *gbe = rel->r;

		if (!gbe || list_empty(gbe) || is_rewrite_gt_zero_used(rel->used))
			return rel;
		/* introduce select * from l where cnt > 0 */
		/* find count */
		if (list_empty(rel->exps)) /* no result expressions, just project the groupby expressions */
			rel->exps = rel_projections(sql, rel, NULL, 1, 1);
		list *exps = rel_projections(sql, rel, NULL, 1, 1);
		sql_exp *e = find_aggr_exp(sql, rel->exps, "count"), *ea = e;
		if (e && e->type == e_column)
			ea = exps_find_exp(rel->exps, e);
		if (!ea || !list_empty(ea->l)) {
			sql_subfunc *cf = sql_bind_func(sql, "sys", "count", sql_fetch_localtype(TYPE_void), NULL, F_AGGR, true, true);

			e = exp_aggr(sql->sa, NULL, cf, 0, 0, CARD_AGGR, 0);

			exp_label(sql->sa, e, ++sql->label);
			append(rel->exps, e);
			e = exp_ref(sql, e);
		}
		rel->used |= rewrite_gt_zero_used;
		e = exp_compare(sql->sa, e, exp_atom_lng(sql->sa, 0), cmp_notequal);
		if (rel_is_ref(rel)) {
			sql_rel *i = rel_create(v->sql->sa);
			*i = *rel;
			i->ref.refcnt = 1;
			i = rel_select(sql->sa, i, e);
			set_count_prop(v->sql->sa, i, get_rel_count(i->l));
			rel_dup(rel->l);
			rel = rel_inplace_project(v->sql->sa, rel, i, exps);
		} else {
			sql_rel *p = v->parent;
			if (p && is_select(p->op)) {
				append(p->exps, e);
				return rel;
			} else {
				rel = rel_select(sql->sa, rel, e);
			}
			set_count_prop(v->sql->sa, rel, get_rel_count(rel->l));
			rel = rel_project(sql->sa, rel, exps);
		}
		set_count_prop(v->sql->sa, rel, get_rel_count(rel->l));
	}
	return rel;
}


/* rewrite avg into sum/count */
static sql_rel *
rel_avg_rewrite(visitor *v, sql_rel *rel)
{
	mvc *sql = v->sql;
	if (is_groupby(rel->op) && rel->r /*&& rel->parallel*/) {
		list *pexps, *nexps = new_exp_list(sql->sa), *avgs = new_exp_list(sql->sa);
		list *aexps = new_exp_list(sql->sa); /* alias list */
		node *m, *n;

		if (mvc_debug_on(sql, 64)) /* disable rewriter with sql_debug=64 */
			return rel;

		/* Find all avg's */
		for (m = rel->exps->h; m; m = m->next) {
			sql_exp *e = m->data;

			if (e->type == e_aggr) {
				sql_subfunc *a = e->f;

				if (strcmp(a->func->base.name, "avg") == 0) {
					sql_subtype *rt = exp_subtype(e);
					sql_subtype *it = first_arg_subtype(e);
					if ((EC_APPNUM(rt->type->eclass) && !EC_APPNUM(it->type->eclass)) || /* always rewrite floating point average */
						(rt->type->localtype > it->type->localtype)) {	/* always rewrite if result type is large enough */
						append(avgs, e);
						continue;
					}
				}
			}
			/* alias for local aggr exp */
			if (e->type == e_column &&
			   (!list_find_exp(rel->r, e) &&
			    !rel_find_exp(rel->l, e)))
				append(aexps, e);
			else
				append(nexps, e);
		}
		if (!list_length(avgs))
			return rel;

		/* For each avg, find count and sum */
		for (m = avgs->h; m; m = m->next) {
			list *args;
			sql_exp *avg = m->data, *navg, *cond, *cnt_d;
			sql_exp *cnt = rel_find_aggr_exp(sql, rel, nexps, avg, "count");
			sql_exp *sum = rel_find_aggr_exp(sql, rel, nexps, avg, "sum");
			sql_subfunc *div, *ifthen, *cmp;
			list *l = avg->l;
			sql_subtype *avg_input_t = exp_subtype(l->h->data);

			/* create nsum/cnt exp */
			if (!cnt) {
				sql_subfunc *cf = sql_bind_func_(sql, "sys", "count", append(sa_list(sql->sa), avg_input_t), F_AGGR, false, true);
				sql_exp *e = exp_aggr(sql->sa, list_dup(avg->l, (fdup)NULL), cf, need_distinct(avg), need_no_nil(avg), avg->card, has_nil(avg));

				append(nexps, e);
				cnt = exp_ref(sql, e);
			}
			if (!sum) {
				sql_subfunc *sf = sql_bind_func_(sql, "sys", "sum", append(sa_list(sql->sa), avg_input_t), F_AGGR, false, true);
				sql_exp *e = exp_aggr(sql->sa, list_dup(avg->l, (fdup)NULL), sf, need_distinct(avg), need_no_nil(avg), avg->card, has_nil(avg));

				append(nexps, e);
				sum = exp_ref(sql, e);
			}
			cnt_d = cnt;

			sql_subtype *avg_t = exp_subtype(avg);
			sql_subtype *dbl_t = sql_fetch_localtype(TYPE_dbl);
			if (subtype_cmp(avg_t, dbl_t) == 0 || EC_INTERVAL(avg_t->type->eclass)) {
				/* check for count = 0 (or move into funcs) */
				args = new_exp_list(sql->sa);
				append(args, cnt);
				append(args, exp_atom_lng(sql->sa, 0));
				cmp = find_func(sql, "=", args);
				assert(cmp);
				cond = exp_op(sql->sa, args, cmp);

				args = new_exp_list(sql->sa);
				append(args, cond);
				append(args, exp_atom(sql->sa, atom_general(sql->sa, exp_subtype(cnt_d), NULL, 0)));
				/* TODO only ifthenelse if value column may have nil's*/
				append(args, cnt_d);
				ifthen = find_func(sql, "ifthenelse", args);
				assert(ifthen);
				cnt_d = exp_op(sql->sa, args, ifthen);

				if (subtype_cmp(avg_t, dbl_t) == 0) {
					cnt_d = exp_convert(sql, cnt, exp_subtype(cnt), dbl_t);
					sum = exp_convert(sql, sum, exp_subtype(sum), dbl_t);
				}

				args = new_exp_list(sql->sa);

				sql_subtype *st = exp_subtype(sum);
				sql_subtype *ct = exp_subtype(cnt_d);
				/* convert sum flt -> dbl */
				if (st->type->eclass == EC_FLT && ct->type->eclass == EC_FLT && st->type->localtype < ct->type->localtype) {
					sum = exp_convert(sql, sum, st, ct);
				} else if (st->type->eclass == EC_FLT) {
					if (ct->type != st->type) {
						sql_subtype *dbl_t = sql_fetch_localtype(TYPE_dbl);
						if (ct->type->eclass != EC_FLT || st->type == dbl_t->type)
							cnt_d = exp_convert(sql, cnt_d, exp_subtype(cnt_d), st);
					}
				}
				append(args, sum);
				append(args, cnt_d);
				div = find_func(sql, "sql_div", args);
				assert(div);
				navg = exp_op(sql->sa, args, div);
			} else {
				args = sa_list(sql->sa);
				append(args, sum);
				append(args, cnt_d);
				div = find_func(sql, "num_div", args);
				assert(div);
				navg = exp_op(sql->sa, args, div);
			}

			if (subtype_cmp(exp_subtype(avg), exp_subtype(navg)) != 0)
				navg = exp_convert(sql, navg, exp_subtype(navg), exp_subtype(avg));

			exp_prop_alias(sql->sa, navg, avg);
			assert(navg);
			m->data = navg;
		}
		pexps = new_exp_list(sql->sa);
		for (m = rel->exps->h, n = avgs->h; m; m = m->next) {
			sql_exp *e = m->data;

			if (e->type == e_aggr) {
				sql_subfunc *a = e->f;

				if (strcmp(a->func->base.name, "avg") == 0) {
					sql_subtype *rt = exp_subtype(e);
					sql_subtype *it = first_arg_subtype(e);
					if ((EC_APPNUM(rt->type->eclass) && !EC_APPNUM(it->type->eclass)) || /* always rewrite floating point average */
						(rt->type->localtype > it->type->localtype)) {	/* always rewrite if result type is large enough */
						sql_exp *avg = n->data;

						append(pexps, avg);
						n = n->next;
						continue;
					}
				}
			}
			/* alias for local aggr exp */
			if (e->type == e_column && !rel_find_exp(rel->l, e))
				append(pexps, e);
			else
				append(pexps, exp_ref(sql, e));
		}
		sql_rel *nrel = rel_groupby(sql, rel_dup(rel->l), rel->r);
		nrel->parallel = rel->parallel;
		nrel->partition = rel->partition;
		nrel->spb = rel->spb;
		set_count_prop(v->sql->sa, nrel, get_rel_count(rel));
		rel_destroy(v->sql, rel);
		nrel->exps = nexps;
		rel = rel_project(sql->sa, nrel, pexps);
		set_count_prop(v->sql->sa, rel, get_rel_count(rel->l));
		set_processed(rel);
		v->changes++;
	}
	return rel;
}

static sql_rel *
rel_simplify_project(visitor *v, sql_rel *rel)
{
	sql_rel *l = rel->l;
	if (v->depth > 1 &&
			is_simple_project(rel->op) && !need_distinct(rel) && !rel_is_ref(rel) && rel->l && rel->r &&
			is_simple_project(l->op) && !rel_is_ref(l) && !l->r &&
			v->parent && is_topn(v->parent->op)  &&
			list_check_prop_all(rel->exps, (prop_check_func)&exp_is_rename)) {

			if (list_length(rel->exps) != list_length(l->exps))
				return rel;
			for(node *n = rel->exps->h, *m = l->exps->h; n && m; n = n->next, m = m->next ) {
				sql_exp *oe = n->data;
				sql_exp *ie = m->data;
				if (!(exp_is_useless_rename(oe) && oe->nid == ie->alias.label) &&
				    !(exp_is_rename(oe) && oe->nid != oe->alias.label && oe->nid == ie->alias.label))
					return rel;
			}
			list *nexps = sa_list(v->sql->sa);
			for(node *n = rel->exps->h, *m = l->exps->h; n && m; n = n->next, m = m->next ) {
				sql_exp *oe = n->data;
				sql_exp *ie = m->data;

				if (exp_is_useless_rename(oe) && oe->nid == ie->alias.label)
					append(nexps, ie);
				else if (exp_is_rename(oe) && oe->nid != oe->alias.label && oe->nid == ie->alias.label) {
					if (!ie->nid || ie->nid != ie->alias.label) {
						exp_setalias(ie, oe->alias.label, exp_relname(oe), exp_name(oe));
						append(nexps, ie);
					} else
						append(nexps, oe);
				}
			}
			rel->l = NULL;
			l->exps = nexps;
			if (l->l) /* constants don't need order by */
				l->r = rel->r;
			rel_destroy(v->sql, rel);
			v->changes++;
			return l;
	}
	return rel;
}


static list * append_func_arguments(visitor *v, list *iexps, list *resultexps);

static list *
append_func_argument(visitor *v, sql_exp *ie, list *resultexps)
{
	while (ie->type == e_convert)
		ie = ie->l;

	if (ie->type == e_column) {
		append(resultexps, exp_copy(v->sql, ie));
	} else if (ie->type == e_func || ie->type == e_aggr) {
		resultexps = append_func_arguments(v, ie->l, resultexps);
	} else if (ie->type == e_cmp) {
		if (ie->flag == cmp_con || ie->flag == cmp_dis) {
			resultexps = append_func_arguments(v, ie->l, resultexps);
		} else if (ie->flag == cmp_in || ie->flag == cmp_notin) {
			resultexps = append_func_argument(v, ie->l, resultexps);
			resultexps = append_func_arguments(v, ie->r, resultexps);
		} else if (ie->flag == cmp_filter) {
			resultexps = append_func_arguments(v, ie->l, resultexps);
			resultexps = append_func_arguments(v, ie->r, resultexps);
		} else {
			resultexps = append_func_argument(v, ie->l, resultexps);
			resultexps = append_func_argument(v, ie->r, resultexps);
			if (ie->f)
				resultexps = append_func_argument(v, ie->f, resultexps);
		}
	}
	return resultexps;
}

static list *
append_func_arguments(visitor *v, list *iexps, list *resultexps)
{
	if (list_empty(iexps))
		return resultexps;
	for(node *m = iexps->h; m; m = m->next)
		resultexps = append_func_argument(v, m->data, resultexps);
	return resultexps;
}

static sql_rel *
rel_push_down_topn(visitor *v, sql_rel *rel)
{
	if (is_topn(rel->op) && !rel_is_ref(rel) && rel->l) {
		sql_rel *orderby = rel->l;

		if (is_simple_project(orderby->op) && !rel_is_ref(orderby) && orderby->l && orderby->r && exps_have_func(orderby->exps) && !exps_have_unsafe(orderby->exps, false, false)) {
			list *nexps = sa_list(v->sql->sa);
			list *pexps = sa_list(v->sql->sa);
			list *oexps = orderby->exps;
			list *obes = orderby->r;
			/* todo mark the expression we cannot push down */
			for(node *m = obes->h; m; m = m->next) {
				sql_exp *obe = m->data;

				assert(obe->type == e_column);
				sql_exp *ne = exps_bind_nid(oexps, obe->nid);
				if (ne && ne->type == e_func)
					return rel;
			}
			orderby->exps = pexps;
			for (node *n = oexps->h; n; n = n->next) {
				sql_exp *oe = n->data;
				if (oe->type != e_column && oe->type != e_atom) {
					nexps = append_func_argument(v, oe, nexps);
					append(pexps, oe);
				} else {
					assert (exp_is_useless_rename(oe) || (exp_is_rename(oe) && oe->nid != oe->alias.label));
					append(nexps, oe);
					append(pexps, exp_ref(v->sql, oe));
				}
			}
			sql_rel *norderby = rel_project(v->sql->sa, orderby->l, nexps);
			norderby->r = orderby->r;
			orderby->r = NULL;
			rel->l = norderby;
			orderby->l = rel;
			return orderby;
		}
	}
	return rel;
}

void
split_join_exps_pp(sql_rel *rel, list *joinable, list *not_joinable, bool anti)
{
	if (!list_empty(rel->exps)) {
		for (node *n = rel->exps->h; n; n = n->next) {
			sql_exp *e = n->data;

			/* we can handle thetajoins, rangejoins and filter joins (like) */
			/* ToDo how about atom expressions? */
			if (can_join_exp(rel, e, anti) && is_equi_exp_(e) && !exp_is_atom(e->r) && !exp_is_atom(e->l)) {
				append(joinable, e);
			} else {
				append(not_joinable, e);
			}
		}
	}
}

static sql_rel *
rel_add_project(visitor *v, sql_rel *rel)
{
	sql_rel *p = v->parent;
	if (!rel)
		return rel;

	v->parent = rel;
	if ((is_join(rel->op) || is_semi(rel->op) || is_select(rel->op)) && p && p->op != op_table) {
		list *exps = rel_projections(v->sql, rel, NULL, 1, 1);
		if (!rel_is_ref(rel))
			rel = rel_project(v->sql->sa, rel, exps);
		else
			rel = rel_inplace_project(v->sql->sa, rel, NULL, exps);
	}
	v->parent = p;
	return rel;
}

#define rewrite_physical_used      (1 << 6)
#define is_physical_done(X)        ((X & rewrite_physical_used) == rewrite_physical_used)

static sql_rel *
rel_rewrite_physical(visitor *v, sql_rel *rel)
{
	if (is_physical_done(rel->used))
		return rel;
	rel->used |= rewrite_physical_used;

	if (rel)
		rel = rel_add_orderby(v, rel);
	if (rel)
		rel = rel_avg_rewrite(v, rel);
	if (rel)
		rel = rel_simplify_project(v, rel);
	if (rel)
		rel = rel_push_down_topn(v, rel);
	if (rel) { /* split equi-join/select */
		rel = rel_count_gt_zero(v, rel);
		ATOMIC_TYPE oahash_enabled = (1U<<19);
		if (SQLrunning && (GDKdebug & oahash_enabled)) {
			if (rel)	/* Add a projection after each join, needed for limited number of columns in hash tables */
				rel = rel_add_project(v, rel);
		}
	}
	return rel;
}

sql_rel *
rel_physical(mvc *sql, sql_rel *rel)
{
	visitor v = { .sql = sql };

	do {
		v.changes = 0;
		rel = rel_visitor_bottomup(&v, rel, &rel_rewrite_physical);
	} while (v.changes);

	if (!rel)
		return NULL;
	v.changes = 0;
	global_props gp = (global_props) {.cnt = {0} };
	v.data = &gp;
	rel = rel_visitor_topdown(&v, rel, &rel_properties); /* collect relational tree properties */
	v.data = NULL;

	if (!sql->recursive) {
		ATOMIC_TYPE oahash_enabled = (1U<<19);
		if (!SQLrunning || !(GDKdebug & oahash_enabled) || gp.complex_modify || gp.cnt[op_except] || gp.cnt[op_inter]) {
			(void)rel_partition(sql, rel);
		} else {
			rel = rel_dce(&v, NULL, rel);
			if (v.opt >= 0)
				v.opt = rel->opt+1;
			(void)rel_pipeline(&v, rel, true, 0);
		}
	}

	rel = rel_exp_visitor_topdown(&v, rel, &exp_timezone, true);

#ifdef HAVE_HGE
	if (rel && sql->no_int128) {
		sql_rel *r = rel;
		if (is_topn(r->op))
				r = r->l;
		if (r && is_project(r->op) && !list_empty(r->exps)) {
			for (node *n = r->exps->h; n; n = n->next) {
				sql_exp *e = n->data;

				if (exp_subtype(e)->type->localtype == TYPE_hge) /* down cast */
					e = n->data = exp_convert(sql, e, exp_subtype(e), sql_fetch_localtype(TYPE_lng));
			}
		}
	}
#endif
	return rel;
}
