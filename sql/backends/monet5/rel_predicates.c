/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0.  If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 *
 * Copyright 1997 - July 2008 CWI, August 2008 - 2021 MonetDB B.V.
 */

#include "monetdb_config.h"

#include "rel_predicates.h"
#include "rel_rel.h"
#include "rel_exp.h"
#include "mal_backend.h"

#if 0
static void
pl_print(mvc *m, list *pls)
{
	if (list_empty(pls))
		return;
	for (node *n = pls->h; n; n = n->next) {
		pl *p = n->data;
		if (p->r) {
			printf("# %s %s %s.%s.%s %s %s\n",
				p->f?atom2string(m->pa, p->f):"NULL",
				compare_func(range2lcompare(p->cmp), 0),
				p->c->t->s?p->c->t->s->base.name:"",
				p->c->t->base.name,
				p->c->base.name,
				compare_func(range2rcompare(p->cmp), 0),
				atom2string(m->pa, p->r));
		} else
			printf("# %s.%s.%s %s %s %s\n",
				p->c->t->s?p->c->t->s->base.name:"",
				p->c->t->base.name,
				p->c->base.name,
				p->f?compare_func(p->cmp, 0):"all" ,
				p->f?atom2string(m->pa, p->f):"",
				p->r?atom2string(m->pa, p->r):"");
	}
}
#endif

static sql_column *
bt_find_column( sql_rel *rel, char *tname, char *name)
{
	if (!rel || !rel->exps || !rel->l)
		return NULL;
	sql_exp *ne = NULL;
	sql_table *t = rel->l;
	if ((ne = exps_bind_column2(rel->exps, tname, name, NULL)) != NULL)
		return find_sql_column(t, ne->r);
	return NULL;
}

static sql_column *
exp_find_column( sql_rel *rel, sql_exp *exp)
{
	if (exp->type == e_column)
		return bt_find_column(is_basetable(rel->op)?rel:rel->l, exp->l, exp->r);
	if (exp->type == e_convert)
		return exp_find_column( rel, exp->l);
	return NULL;
}

static list *
add_predicate(sql_allocator *sa, list *l, pl *pred)
{
	if (!l)
		l = sa_list(sa);
	list_append(l, pred);
	return l;
}

static sql_rel *
rel_find_predicates(visitor *v, sql_rel *rel)
{
	bool needall = false;

	if (is_basetable(rel->op)) {
		sql_table *t = rel->l;

		if (!t || !rel->exps || isNew(t) || t->persistence == SQL_DECLARED_TABLE)
			return rel;
		sql_rel *parent = v->parent;

		/* select with basetable */
		if (is_select(parent->op)) {
			/* add predicates */
			for (node *n = parent->exps->h; n && !needall; n = n->next) {
				sql_exp *e = n->data, *r = e->r, *r2 = e->f;
				sql_column *c = NULL;

				if (!is_compare(e->type) || !is_theta_exp(e->flag) || r->type != e_atom || !r->l || (r2 && (r2->type != e_atom || !r2->l)) || is_symmetric(e) || !(c = exp_find_column(rel, e->l))) {
					needall = true;
				} else {
					pl *p = SA_ZNEW(v->sql->pa, pl);
					p->c = c;
					p->cmp = e->flag;
					p->anti = is_anti(e);
					p->semantics = is_semantics(e);
					p->r = atom_dup(v->sql->pa, r->l);
					if (r2)
						p->f = atom_dup(v->sql->pa, r2->l);
					v->sql->session->tr->predicates = add_predicate(v->sql->pa, v->sql->session->tr->predicates, p);
					*(int*)v->data = 1;
				}
			}
		}

		if (!is_select(parent->op) || needall) {
			/* any other case, add all predicates */
			sql_table *t = rel->l;

			if (!t || !rel->exps)
				return rel;
			for (node *n = rel->exps->h; n; n = n->next) {
				sql_exp *e = n->data;
				if (!is_intern(e)) {
					pl *p = SA_ZNEW(v->sql->pa, pl);
					p->c = find_sql_column(t, e->r);
					assert(p->c);
					v->sql->session->tr->predicates = add_predicate(v->sql->pa, v->sql->session->tr->predicates, p);
					*(int*)v->data = 1;
				}
			}
		}
	}
	return rel;
}

void
rel_predicates(backend *be, sql_rel *rel)
{
	if (be->mvc->session->level < tr_serializable)
		return ;
	int changes = 0;
	visitor v = { .sql = be->mvc, .data = &changes };
	rel = rel_visitor_topdown(&v, rel, &rel_find_predicates);
#if 0
	if (changes)
		pl_print(be->mvc, be->mvc->session->tr->predicates);
#endif
}

