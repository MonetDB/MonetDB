

#include "monetdb_config.h"

#include "rel_bin.h"
#include "rel_rel.h"
#include "rel_exp.h"
/*
#include "rel_basetable.h"
#include "rel_psm.h"
#include "rel_prop.h"
#include "rel_unnest.h"
#include "rel_optimizer.h"
*/
#include "rel_predicates.h"


typedef struct pl {
	sql_column *c;
	comp_type cmp;
	atom *l;
	atom *r;
} pl;

static void
pl_print(mvc *m, list *pls)
{
	if (list_empty(pls))
		return;
	for (node *n = pls->h; n; n = n->next) {
		pl *p = n->data;
		if (p->r) {
			printf("# %s %s %s.%s.%s %s %s\n",
				p->l?atom2string(m->pa, p->l):"NULL",
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
				p->l?compare_func(p->cmp, 0):"all" ,
				p->l?atom2string(m->pa, p->l):"",
				p->r?atom2string(m->pa, p->r):"");
	}
}

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
	/* TODO handle simple converts */
	if (exp->type == e_column)
		return bt_find_column(rel->l, exp->l, exp->r);
	if (exp->type == e_convert)
		return exp_find_column( rel, exp->l);
	return NULL;
}

static list *
Append(sql_allocator *sa, list *l, void *d)
{
	if (!l)
		l = sa_list(sa);
	append(l,d);
	return l;
}

static sql_rel *
rel_find_predicates(visitor *v, sql_rel *rel)
{
	int needall = 0;

	/* select with basetable */
	if (is_select(rel->op)) {
		sql_rel *bt = rel->l;
		if (bt && is_basetable(bt->op)) {
			/* add predicates */
			for (node *n = rel->exps->h; n && !needall; n = n->next) {
				sql_exp *e = n->data;
				if (!is_intern(e) && e->type == e_cmp) {
					sql_column *c = exp_find_column( rel, e->l);
					if (c) {
						sql_exp *r = e->r;
						sql_exp *r2 = e->f;
						pl *p = SA_NEW(v->sql->pa, pl);
						p->c = c;
						p->cmp = e->flag;
						if (is_anti(e))
							p->cmp = swap_compare(p->cmp);
						p->l = p->r = NULL;
						if (r && r->type == e_atom && r->l)
							p->l = atom_dup(v->sql->pa, r->l);
						if (r2 && r2->type == e_atom && r2->l)
							p->r = atom_dup(v->sql->pa, r2->l);
						v->sql->session->tr->predicates = Append(v->sql->pa, v->sql->session->tr->predicates, p);
						*(int*)v->data = 1;
					} else {
						needall = 1;
					}
				}
			}
		}
	}
	/* project with basetable */
	if (is_simple_project(rel->op) || needall /* ie handle select like a project, ie all columns of the basetable */) {
		sql_rel *bt = rel->l;
		if (bt && is_basetable(bt->op)) {
			sql_table *t = bt->l;
			/* add predicates */
			if (!t || !bt->exps)
				return rel;
			for (node *n = bt->exps->h; n; n = n->next) {
				sql_exp *e = n->data;
				if (!is_intern(e)) {
					pl *p = SA_NEW(v->sql->pa, pl);
					p->c = find_sql_column(t, e->r);
					p->cmp = 0;
					p->l = p->r = NULL;
					v->sql->session->tr->predicates = Append(v->sql->pa, v->sql->session->tr->predicates, p);
					*(int*)v->data = 1;
				}
			}
		}
	}
	/* group by with basetable */
	if (is_groupby(rel->op)) {
		sql_rel *bt = rel->l;
		/* for now same as project above */
		if (bt && is_basetable(bt->op)) {
			sql_table *t = bt->l;
			/* add predicates */
			if (!t || !bt->exps)
				return rel;
			for (node *n = bt->exps->h; n; n = n->next) {
				sql_exp *e = n->data;
				if (!is_intern(e)) {
					pl *p = SA_NEW(v->sql->pa, pl);
					p->c = find_sql_column(t, e->r);
					p->cmp = 0;
					p->l = p->r = NULL;
					v->sql->session->tr->predicates = Append(v->sql->pa, v->sql->session->tr->predicates, p);
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
	if (changes)
		pl_print(be->mvc, be->mvc->session->tr->predicates);
}

