
#include "monetdb_config.h"
#include "rel_multiset.h"
#include "rel_exp.h"
#include "rel_rel.h"
#include "rel_basetable.h"
#include "rel_updates.h"
#include "rel_rewriter.h"

extern void _rel_print(mvc *sql, sql_rel *cur);

/* TODO handle composite/multset (ie deep nested cases) too */
static sql_rel *
fm_join(visitor *v, sql_rel *rel)
{
	if (list_empty(rel->exps) && is_dependent(rel)) {
		bool needed = false;
		sql_rel *l = rel->l;
		sql_rel *r = rel->r;
		if (0 && r && is_basetable(r->op)) {
			sql_table *t = r->l;
		   	if (t->base.name[0] == '%')
				return l;
		}
		list *exps = rel_projections(v->sql, l, NULL, 0, 1);
		for(node *n = exps->h; n && !needed; n = n->next) {
			sql_subtype *t = exp_subtype(n->data);
			needed = (t && t->multiset);
		}
		if (needed) {
			for(node *n = exps->h; n; n = n->next) {
				sql_exp *e = n->data;
				sql_subtype *t = exp_subtype(e);
				if (t->multiset) {
					v->changes++;
					sql_exp *le = exp_ref(v->sql, e);
					list *rexps = rel_projections(v->sql, r, NULL, 0, 1);
					sql_exp *re = exps_bind_column(rexps, "id", NULL, NULL, 0);
					if (le && re) {
						re = exp_ref(v->sql, re);
						e = exp_compare(v->sql->sa, le, re, cmp_equal);
						rel->exps = sa_list_append(v->sql->sa, rel->exps, e);
					}
					return rel;
				}
			}
		}
	}
	return rel;
}

static sql_rel *
flatten_multiset(visitor *v, sql_rel *rel)
{
	(void)v;
	switch(rel->op) {
	case op_join:
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
