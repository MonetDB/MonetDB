
#include "monetdb_config.h"
#include "rel_multiset.h"
#include "rel_exp.h"
#include "rel_rel.h"
#include "rel_basetable.h"
#include "rel_updates.h"
#include "rel_rewriter.h"

extern void _rel_print(mvc *sql, sql_rel *cur);

#if 0
static sql_rel *
fm_basetable(visitor *v, sql_rel *rel)
{
	if (v->parent && is_modify(v->parent->op))
		return rel;

	sql_table *t = rel->l;

	bool needed = false;
	for(node *n = ol_first_node(t->columns); n && !needed; n = n->next) {
		sql_column *c = n->data;
		needed = c->type.multiset;
	}
	if (needed) {
		list *exps = rel_projections(v->sql, rel, NULL, 0, 1);
		for(node *n = ol_first_node(t->columns); n; n = n->next) {
			sql_column *c = n->data;

			if (c->type.multiset) {
				sql_table *t = mvc_bind_table(v->sql, c->t->s, c->storage_type);
				if (!t) {
					/* should not happen */
					(void) sql_error(v->sql, 10, SQLSTATE(42000) "Could not find table: %s.%s", c->t->s->base.name, c->storage_type);
					return NULL;
				}
				sql_rel *st = rel_basetable(v->sql, t, a_create(v->sql->sa, t->base.name));
				rel = rel_crossproduct(v->sql->sa, rel, st, op_join /* todo add special case ? */);

				sql_exp *le = exps_bind_column(exps, c->base.name, NULL, NULL, 0);
				list *rexps = rel_projections(v->sql, st, NULL, 0, 1);
				sql_exp *re = exps_bind_column(rexps, "id", NULL, NULL, 0);
				if (le && re) {
					sql_exp *e = exp_compare(v->sql->sa, le, re, cmp_equal);
					rel->exps = sa_list_append(v->sql->sa, rel->exps, e);
				}
			}
		}
	}
	return rel;
}

static sql_rel *
fm_table(visitor *v, sql_rel *rel)
{
	if (IS_TABLE_PROD_FUNC(rel->flag)) {
		sql_exp *op = rel->r;
		sql_subfunc *f = op->f;
		if (f->func->private && !f->func->s && strcmp(f->func->base.name, "unnest") == 0) {
			sql_rel *p = v->parent;
			if (p && is_join(p->op) && is_dependent(p)) { /* expect p above */
				list *args = op->l;
				sql_exp *e = args->h->data;
				sql_column *c = exp_find_column(p, e, -1);
				if (c) {
					sql_table *st = mvc_bind_table(v->sql, c->t->s, c->storage_type);
					if (!st)
						return sql_error(v->sql, ERR_NOTFOUND, SQLSTATE(42000) "SELECT: unnest multiset table '%s' missing", c->storage_type);
					sql_rel *bst = rel_basetable(v->sql, st, a_create(v->sql->sa, c->storage_type));
					/* add mapping */
					_rel_print(v->sql, bst);
					return bst;
				}
			}
		}
	}
	return rel;
}
#endif

static node *
fm_insert_ms(visitor *v, node *m, sql_subtype *t, sql_table *pt, char *name, list *btexps, list *niexps, sql_rel **cur, sql_rel *ins)
{
	assert (t->multiset);

	sql_subfunc *next_val = sql_find_func(v->sql, "sys", "next_value_for", 3, F_FUNC, false, NULL);
	list *args = sa_list(v->sql->sa);
	sql_exp *rowid = m->data;

	/* give old rowid unique label !*/
	sql_alias a = rowid->alias;
	exp_setalias(rowid, ++v->sql->label, rowid->alias.parent, "oldid");
	rowid = exp_ref(v->sql, rowid);

	append(args, rowid);
	append(args, exp_atom_clob(v->sql->sa, "sys"));
	append(args, exp_atom_clob(v->sql->sa, name));
	sql_exp *nrowid = exp_op(v->sql->sa, args, next_val), *e;
	sql_subtype *inttype = sql_bind_localtype("int");
	nrowid = exp_convert(v->sql, nrowid, exp_subtype(nrowid), inttype);
	//exp_prop_alias(v->sql->sa, nrowid, rowid);
	nrowid->alias = a;
	append(niexps, nrowid);
	nrowid = exp_ref(v->sql, nrowid);

	sql_table *subtable = mvc_bind_table(v->sql, pt->s, name);
	if (!subtable) {
		/* should not happen */
		(void) sql_error(v->sql, 10, SQLSTATE(42000) "Could not find table: %s.%s", pt->s->base.name, name);
		return NULL;
	}

	list *nexps = sa_list(v->sql->sa);
	append(btexps, nrowid);
	m = m->next;
	if (t->type->composite) {
		for(node *n = ol_first_node(subtable->columns), *o = t->type->d.fields->h; n && m && o; n = n->next, o = o->next) {
			sql_column *c = n->data;
			if (c->type.multiset) {
				m = fm_insert_ms(v, m, &c->type, subtable, c->storage_type, nexps, niexps, cur, ins);
			} else {
				sql_exp *e = exp_ref(v->sql, m->data);
				append(niexps, e = exp_ref(v->sql, m->data));
				append(nexps, exp_ref(v->sql, e));
				m = m->next;
			}
		}
	} else {
		sql_exp *e = exp_ref(v->sql, m->data);
		append(niexps, e = exp_ref(v->sql, m->data));
		append(nexps, exp_ref(v->sql, e));
		m = m->next;
	}

	sql_subfunc *renumber = sql_find_func(v->sql, "sys", "renumber", 3, F_FUNC, false, NULL);
	args = sa_list(v->sql->sa);
	sql_exp *msid = exp_ref(v->sql, m->data);
	append(args, msid);
	append(args, rowid);
	append(args, nrowid);
	sql_exp *nmsid = exp_op(v->sql->sa, args, renumber);
	exp_prop_alias(v->sql->sa, nmsid, msid);
	append(niexps, nmsid);

	append(nexps, exp_ref(v->sql, m->data));
	m = m->next;
	if (t->multiset == MS_ARRAY) {
		append(niexps, e = exp_ref(v->sql, m->data));
		append(nexps, exp_ref(v->sql, e));
		m = m->next;
	}
	sql_rel *st = rel_basetable(v->sql, subtable, a_create(v->sql->sa, subtable->base.name));
	sql_rel *i = rel_insert(v->sql, st, rel_project(v->sql->sa, rel_dup(ins), nexps));
	if (*cur)
		*cur = rel_list(v->sql->sa, *cur, i);
	else
		*cur = i;
	return m;
}

static sql_rel *
fm_insert(visitor *v, sql_rel *rel)
{
	sql_rel *bt = rel->l;
	if (is_basetable(bt->op)) {
		sql_table *t = bt->l;

		bool needed = t->multiset;
		if (needed) {
			sql_rel *cur = NULL;
			sql_rel *ins = rel->r;
			assert(is_project(ins->op) || is_base(ins->op));
			list *exps = ins->exps;
			list *btexps = sa_list(v->sql->sa);
			list *niexps = sa_list(v->sql->sa); /* inject extra project, such that referencing becomes easier */
			/* do insert per multiset and once for base table */
			rel->r = ins = rel_project(v->sql->sa, ins, niexps);
			for(node *n = ol_first_node(t->columns), *m = exps->h; n && m; n = n->next) {
				sql_column *c = n->data;
				if (c->type.multiset) {
					m = fm_insert_ms(v, m, &c->type, t, c->storage_type, btexps, niexps, &cur, ins);
				} else {
					sql_exp *e = exp_ref(v->sql, m->data);
					append(niexps, e);
					append(btexps, exp_ref(v->sql, e));
					m = m->next;
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

static void
fm_project_ms(visitor *v, sql_exp *e, sql_subtype *t, sql_alias *cn, list *nexps)
{
	//sql_subtype *oidtype = sql_bind_localtype("oid");
	sql_subtype *inttype = sql_bind_localtype("int");
	int label = v->sql->label;
	v->sql->label += 2 + (t->multiset == MS_ARRAY) + list_length(t->type->d.fields);
	e = exp_ref(v->sql, e);
	sql_exp *mse = exp_column(v->sql->sa, cn, "rowid", inttype, 1,1, 1, 1);
	mse->alias.label = (++label);
	mse->nid = mse->alias.label;
	append(nexps, mse);
	if (t->type->composite) {
		for(node *f = t->type->d.fields->h; f; f = f->next) {
			sql_arg *field = f->data;

			if (field->type.multiset) {
				sql_alias *nn = a_create(v->sql->sa, field->name);
				nn->parent = cn;
				fm_project_ms(v, e, &field->type, nn, nexps);
			} else {
				mse = exp_column(v->sql->sa, cn, field->name, &field->type, 1,1, 1, 1);
				mse->alias.label = (++label);
				mse->nid = mse->alias.label;
				append(nexps, mse);
			}
		}
	} else {
		sql_subtype lt = *t;
		lt.multiset = MS_VALUE;
		mse = exp_column(v->sql->sa, cn, "elements", &lt, 1,1, 1, 1);
		mse->alias.label = (++label);
		mse->nid = mse->alias.label;
		append(nexps, mse);
	}
	mse = exp_column(v->sql->sa, cn, "multisetid", inttype, 1,1, 1, 1);
	mse->alias.label = (++label);
	mse->nid = mse->alias.label;
	append(nexps, mse);
	if (t->multiset == MS_ARRAY) {
		mse = exp_column(v->sql->sa, cn, "multisetnr", inttype, 1,1, 1, 1);
		mse->alias.label = (++label);
		mse->nid = mse->alias.label;
		append(nexps, mse);
	}
}

static sql_rel *
fm_project(visitor *v, sql_rel *rel)
{
	sql_rel *l = rel->l;

	if ((!l || (l && rel->card == CARD_ATOM && is_project(l->op))) && rel->exps) { /* check for type multiset */
		bool needed = false;
		for(node *n = rel->exps->h; n; n = n->next) {
			sql_exp *e = n->data;
			sql_subtype *t = exp_subtype(e);
			needed = (t && t->multiset);
			if (needed && is_intern(e)) {
				needed = false;
				break;
			}
		}
		for(node *n = rel->exps->h; n && !needed; n = n->next) {
			sql_subtype *t = exp_subtype(n->data);
			needed = (t && t->multiset);
		}
		if (needed) {
			list *nexps = sa_list(v->sql->sa);
			list *fexps = sa_list(v->sql->sa);
			for(node *n = rel->exps->h; n; n = n->next) {
				sql_exp *e = n->data;
				sql_subtype *t = exp_subtype(e);
				if (t->multiset) {
					sql_alias *cn = a_create(v->sql->sa, e->alias.name);
					fm_project_ms(v, e, t, cn, nexps);
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
	} else if (rel->l && rel->exps) { /* check for type multiset, expand the column list for the content columns */
		bool needed = false;
		for(node *n = rel->exps->h; n && !needed; n = n->next) {
			sql_exp *e = n->data;
			sql_subtype *t = exp_subtype(e);
			needed = (t && t->multiset);
			if (needed && is_intern(e)) {
				needed = false;
				break;
			}
		}
		if (needed) {
			sql_rel *l = rel->l;
			list *nexps = sa_list(v->sql->sa);
			list *exps = l->exps;
			if (!is_project(l->op))
				exps = rel_projections(v->sql, l, NULL, 0, 1);
			for(node *n = rel->exps->h; n; n = n->next) {
				sql_exp *e = n->data;
				sql_subtype *t = exp_subtype(e);
				if (t->multiset) {
					sql_alias *cn = a_create(v->sql->sa, e->alias.name);
					sql_exp *rowid = exps_bind_column(exps, exp_name(e), NULL, NULL, 0);
					if (!rowid)
						rowid = exps_bind_column2(exps, cn, "rowid", NULL);
					rowid = exp_ref(v->sql, rowid);
					append(nexps, rowid);
					if (t->type->composite) {
						for(node *f = t->type->d.fields->h; f; f = f->next) {
							sql_arg *field = f->data;
							sql_exp *mse = exps_bind_column2(exps, cn, field->name, NULL);
							mse = exp_ref(v->sql, mse);
							append(nexps, mse);
						}
					} else {
						sql_exp *mse = exps_bind_column2(exps, cn, "elements", NULL);
						mse = exp_ref(v->sql, mse);
						append(nexps, mse);
					}
					sql_exp *msid = exps_bind_column2(exps, cn, "id", NULL);
					if (!msid)
						msid = exps_bind_column2(exps, cn, "multisetid", NULL);
					msid = exp_ref(v->sql, msid);
					append(nexps, msid);
					if (t->multiset == MS_ARRAY) {
						sql_exp *msnr = exps_bind_column2(exps, cn, "nr", NULL);
						if (!msnr)
							msnr = exps_bind_column2(exps, cn, "multisetnr", NULL);
						msnr = exp_ref(v->sql, msnr);
						append(nexps, msnr);
					}
				} else {
					append(nexps, e);
				}
			}
			rel = rel_project(v->sql->sa, rel->l, nexps);
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
	case op_join:
		return fm_join(v, rel);
	case op_insert:
		return fm_insert(v, rel);
#if 0
	case op_basetable:
		return fm_basetable(v, rel);
	case op_table:
		return fm_table(v, rel);
#endif
	//case op_truncate: ie also truncate multiset table and restart sequence number
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
