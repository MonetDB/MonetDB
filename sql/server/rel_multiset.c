
#include "monetdb_config.h"
#include "rel_multiset.h"
#include "rel_exp.h"
#include "rel_rel.h"
#include "rel_basetable.h"
#include "rel_updates.h"
#include "rel_rewriter.h"

/* TODO move into unnest code */
static bool
has_multiset(list *exps)
{
	bool needed = false;
	if (list_empty(exps))
		return needed;
	for(node *n = exps->h; n && !needed; n = n->next) {
		sql_exp *e = n->data;
		sql_subtype *t = exp_subtype(e);

		needed = (t && t->multiset);
		if (!needed && t && t->type->composite && is_nested(e))
			needed = has_multiset(e->f);
	}
	return needed;
}

static sql_rel *
ms_add_join_exps(visitor *v, sql_rel *rel, list *exps)
{
	if (list_empty(exps))
		return rel;
	sql_rel *r = rel->r;
	while (is_select(r->op))
		r = r->l;
	prop *p = r->p;
	if (!p || p->kind != PROP_UNNEST)
		return sql_error(v->sql, 02, SQLSTATE(42000) "UNNEST: joining of "
				"array column without an UNNEST");
	sql_exp *re = p->value.pval;

	for(node *n = exps->h; n; n = n->next) {
		sql_exp *e = n->data;
		sql_subtype *t = exp_subtype(e);
		if (exp_match(e, re) && t->multiset) {
			v->changes++;
			sql_exp *le = exp_ref(v->sql, e);
			list *rexps = rel_projections(v->sql, rel->r, NULL, 0, 1);
			sql_exp *re = exps_bind_column(rexps, MSID_NAME, NULL, NULL, 0);
			if (le && re) {
				re = exp_ref(v->sql, re);
				e = exp_compare(v->sql->sa, le, re, cmp_equal);
				rel->exps = sa_list_append(v->sql->sa, rel->exps, e);
			}
			return rel;
		} else if (t->type->composite) {
			/* sofar only handles one level */
			sql_rel *res = ms_add_join_exps(v, rel, e->f);
			if (res)
				return res;
		}
	}
	return NULL;
}

/* TODO handle composite/multset (ie deep nested cases) too */
static sql_rel *
fm_join(visitor *v, sql_rel *rel)
{
	if (is_dependent(rel) && list_length(rel->exps) <= 1) {
		if (list_length(rel->exps) == 1) {
			sql_exp *e = rel->exps->h->data;
			if (!exp_is_true(e))
				return rel;
		}
		list *exps = rel_projections(v->sql, rel->l, NULL, 0, 1);
		bool needed = has_multiset(exps);

		if (needed)
			return ms_add_join_exps(v, rel, exps);
	}
	return rel;
}

static sql_rel *
flatten_multiset(visitor *v, sql_rel *rel)
{
	(void)v;
	switch(rel->op) {
	case op_join:
	case op_left:
		return fm_join(v, rel);
	default:
		return rel;
	}
	return rel;
}

sql_rel *
rel_multiset(mvc *sql, sql_rel *rel)
{
	visitor v = { .sql = sql };

	rel = rel_visitor_bottomup(&v, rel, &flatten_multiset);
	return rel;
}
