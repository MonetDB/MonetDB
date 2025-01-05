
#include "monetdb_config.h"
#include "rel_multiset.h"
#include "rel_exp.h"
#include "rel_rel.h"
#include "rel_basetable.h"
#include "rel_updates.h"

static sql_rel *
fm_insert(visitor *v, sql_rel *rel)
{
	sql_rel *bt = rel->l;
	if (is_basetable(bt->op)) {
		sql_table *t = bt->l;

		bool needed = false;
		for(node *n = ol_first_node(t->columns); n && !needed; n = n->next) {
			sql_column *c = n->data;
			needed = c->type.multiset;
		}
		if (needed) {
			sql_rel *cur = NULL;
			sql_rel *ins = rel->r;
			assert(is_project(ins->op) || is_base(ins->op));
			list *exps = ins->exps;
			list *btexps = sa_list(v->sql->sa);
			/* do insert per multiset and once for base table */
			for(node *n = ol_first_node(t->columns), *m = exps->h; n && m; n = n->next) {
				sql_column *c = n->data;
				if (c->type.multiset) {
					list *nexps = sa_list(v->sql->sa);
					append(btexps, exp_ref(v->sql, m->data)); /* rowid */
					m = m->next;
					/* find fields and msid,nr from right handside */
					for(node *f = c->type.type->d.fields->h; f; f = f->next, m = m->next)
						append(nexps, exp_ref(v->sql, m->data));
					append(nexps, exp_ref(v->sql, m->data));
					m = m->next;
					if (c->type.multiset == MS_ARRAY) {
						append(nexps, exp_ref(v->sql, m->data));
						m = m->next;
					}
					sql_table *t = mvc_bind_table(v->sql, c->t->s, c->storage_type);
					if (!t) {
						/* should not happen */
						(void) sql_error(v->sql, 10, SQLSTATE(42000) "Could not find table: %s.%s", c->t->s->base.name, c->storage_type);
						return NULL;
					}
					sql_rel *st = rel_basetable(v->sql, t, a_create(v->sql->sa, t->base.name));
					sql_rel *i = rel_insert(v->sql, st, rel_project(v->sql->sa, rel_dup(rel->r), nexps));
					if (cur)
						cur = rel_list(v->sql->sa, cur, i);
					else
						cur = i;
				} else {
					append(btexps, exp_ref(v->sql, m->data));
				}
			}
			rel->r = rel_project(v->sql->sa, rel->r, btexps);
			cur = rel_list(v->sql->sa, cur, rel);
			return cur;
		}
	}
	return rel;
}

static sql_rel *
fm_project(visitor *v, sql_rel *rel)
{
	if (!rel->l && rel->exps) { /* check for type multiset */
		bool needed = false;
		for(node *n = rel->exps->h; n && !needed; n = n->next) {
			sql_subtype *t = exp_subtype(n->data);
			needed = (t && t->multiset);
		}
		if (needed) {
			//sql_subtype *oidtype = sql_bind_localtype("oid");
			sql_subtype *inttype = sql_bind_localtype("int");
			list *nexps = sa_list(v->sql->sa);
			list *fexps = sa_list(v->sql->sa);
			for(node *n = rel->exps->h; n; n = n->next) {
				sql_exp *e = n->data;
				sql_subtype *t = exp_subtype(e);
				if (t->multiset) {
					e = exp_ref(v->sql, e);
					sql_alias *cn = a_create(v->sql->sa, e->alias.name);
					sql_exp *mse = exp_column(v->sql->sa, cn, "rowid", inttype, 1,1, 1, 1);
					mse->alias.label = -(++v->sql->label);
					mse->nid = e->alias.label;
					append(nexps, mse);
					for(node *f = t->type->d.fields->h; f; f = f->next) {
						sql_arg *field = f->data;
						mse = exp_column(v->sql->sa, cn, field->name, &field->type, 1,1, 1, 1);
						mse->alias.label = -(++v->sql->label);
						mse->nid = e->alias.label;
						append(nexps, mse);
					}
					mse = exp_column(v->sql->sa, cn, "multisetid", inttype, 1,1, 1, 1);
					mse->alias.label = -(++v->sql->label);
					mse->nid = e->alias.label;
					append(nexps, mse);
					if (t->multiset == MS_ARRAY) {
						mse = exp_column(v->sql->sa, cn, "multisetnr", inttype, 1,1, 1, 1);
						mse->alias.label = -(++v->sql->label);
						mse->nid = e->alias.label;
						append(nexps, mse);
					}
				} else {
					append(nexps, e);
				}
				append(fexps, exp_ref(v->sql, e));
			}
			sql_rel *nrel = rel_project(v->sql->sa, NULL, rel->exps);
			list *tl = append(sa_list(v->sql->sa), exp_subtype(fexps->h->data));//exp_types(v->sql->sa, fexps);
			list *rl = exp_types(v->sql->sa, nexps);
			sql_subfunc *msf = sql_bind_func_(v->sql, NULL, "multiset", tl, F_UNION, true, true, false);
			msf->res = rl;
			sql_exp *e = exp_op(v->sql->sa, fexps, msf);
			rel = rel_table_func(v->sql->sa, nrel, e, nexps, TABLE_PROD_FUNC);
			v->changes++;
		}
	}
	return rel;
}

static sql_rel *
flatten_multiset(visitor *v, sql_rel *rel)
{
	(void)v;
	switch(rel->op) {
	case op_project:
		return fm_project(v, rel);
	case op_insert:
		return fm_insert(v, rel);
	default:
		//printf("todo %d\n", rel->op);
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
