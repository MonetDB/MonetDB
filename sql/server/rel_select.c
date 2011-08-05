/*
 * The contents of this file are subject to the MonetDB Public License
 * Version 1.1 (the "License"); you may not use this file except in
 * compliance with the License. You may obtain a copy of the License at
 * http://www.monetdb.org/Legal/MonetDBLicense
 *
 * Software distributed under the License is distributed on an "AS IS"
 * basis, WITHOUT WARRANTY OF ANY KIND, either express or implied. See the
 * License for the specific language governing rights and limitations
 * under the License.
 *
 * The Original Code is the MonetDB Database System.
 *
 * The Initial Developer of the Original Code is CWI.
 * Portions created by CWI are Copyright (C) 1997-July 2008 CWI.
 * Copyright August 2008-2011 MonetDB B.V.
 * All Rights Reserved.
 */


#include "monetdb_config.h"
#include "rel_select.h"
#include "sql_semantic.h"	/* TODO this dependency should be removed, move
				   the dependent code into sql_mvc */
#include "sql_privileges.h"
#include "sql_env.h"
#include "rel_exp.h"
#include "rel_xml.h"
#include "rel_dump.h"
#include "rel_prop.h"
#include "rel_schema.h"
#include "rel_sequence.h"

#define rel_groupby_gbe(sa,r,e) rel_groupby(sa, r, append(new_exp_list(sa), e))
#define ERR_AMBIGUOUS		050000

sql_rel *
rel_dup(sql_rel *r)
{
	sql_ref_inc(&r->ref);
	return r;
}

static void
rel_destroy_(sql_rel *rel)
{
	if (!rel)
		return;
	if (is_join(rel->op) ||
	    is_semi(rel->op) ||
	    is_select(rel->op) ||
	    is_set(rel->op) ||
	    rel->op == op_topn) {
		if (rel->l)
			rel_destroy(rel->l);
		if (rel->r)
			rel_destroy(rel->r);
	} else if (is_project(rel->op)) {
		if (rel->l)
			rel_destroy(rel->l);
	} else if (is_modify(rel->op)) {
		if (rel->r)
			rel_destroy(rel->r);
	}
}

void
rel_destroy(sql_rel *rel)
{
	if (!rel)
		return;
	if (sql_ref_dec(&rel->ref) > 0)
		return;
	rel_destroy_(rel);
	/*_DELETE(rel); TODO pass back to the rel list ! */
}

sql_rel*
rel_create( sql_allocator *sa )
{
	sql_rel *r = SA_NEW(sa, sql_rel);

	sql_ref_init(&r->ref);
	r->l = r->r = NULL;
	r->exps = NULL;
	r->nrcols = 0;
	r->flag = 0;
	r->card = CARD_ATOM;
	r->processed = 0;
	r->subquery = 0;
	r->p = NULL;
	return r;
}

static void
rel_setsubquery(sql_rel*r)
{
	if (rel_is_ref(r))
		return;
	if (r->l && !is_base(r->op))
		rel_setsubquery(r->l);
	if (r->r && is_join(r->op))
		rel_setsubquery(r->r);
	set_subquery(r);
}

static int
rel_issubquery(sql_rel*r)
{
	if (!r->subquery) {
		if (is_select(r->op))
			return rel_issubquery(r->l);
	}	
	return r->subquery;
}

/* we don't name relations directly, but sometimes we need the relation
   name. So we look it up in the first expression

   we should clean up (remove) this function.
 */
char *
rel_name( sql_rel *r )
{
	if (!is_project(r->op) && !is_base(r->op) && r->l)
		return rel_name(r->l);
	if (r->exps && list_length(r->exps)) {
		sql_exp *e = r->exps->h->data;
		if (e->rname)
			return e->rname;
		if (e->type == e_column)
			return e->l;
	}
	return NULL;
}

sql_rel *
rel_label( mvc *sql, sql_rel *r)
{
	int nr = ++sql->label;
	char name[16], *nme;

	nme = number2name(name, 16, nr);
	if (!is_project(r->op)) {
		r = rel_project(sql->sa, r, rel_projections(sql, r, NULL, 1, 1));
		set_processed(r);
	}
	if (is_project(r->op) && r->exps) {
		node *ne = r->exps->h;

		for (; ne; ne = ne->next)
			exp_setname(sql->sa, ne->data, nme, NULL );
	}
	/* op_projects can have a order by list */
	if (r->op == op_project && r->r) {
		list *exps = r->r;
		node *ne = exps->h;

		for (; ne; ne = ne->next)
			exp_setname(sql->sa, ne->data, nme, NULL );
	}
	return r;
}

static sql_exp *
exp_alias_or_copy( mvc *sql, char *tname, char *cname, sql_rel *orel, sql_exp *old, int settname)
{
	if (settname && !tname)
		tname = old->rname;

	if (settname && !tname && old->type == e_column)
		tname = old->l;

	if (!cname) {
		char name[16], *nme;
		nme = number2name(name, 16, ++sql->label);

		exp_setname(sql->sa, old, nme, nme);
		return exp_column(sql->sa, nme, nme, exp_subtype(old), orel->card, has_nil(old), is_intern(old));
	} else if (cname && !old->name) {
		exp_setname(sql->sa, old, tname, cname);
	}
	return exp_column(sql->sa, tname, cname, exp_subtype(old), orel->card, has_nil(old), is_intern(old));
}

/* return all expressions, with table name == tname */
static list *
rel_table_projections( mvc *sql, sql_rel *rel, char *tname )
{
	list *exps;

	if (!rel)
		return NULL;

	if (!tname) {
		if (is_project(rel->op) && rel->l)
			return rel_projections(sql, rel->l, NULL, 1, 0);
		else	
			return NULL;
	}

	switch(rel->op) {
	case op_join:
	case op_left:
	case op_right:
	case op_full:
		exps = rel_table_projections( sql, rel->l, tname);
		if (exps)
			return exps;
		return rel_table_projections( sql, rel->r, tname);
	case op_semi:
	case op_anti:
	case op_select:
		return rel_table_projections( sql, rel->l, tname);

	case op_topn:
	case op_groupby:
	case op_union:
	case op_except:
	case op_inter:
	case op_project:
		if (!is_processed(rel))
			return rel_table_projections( sql, rel->l, tname);
	case op_table:
	case op_basetable:
		if (rel->exps) {
			node *en;

			exps = new_exp_list(sql->sa);
			for (en = rel->exps->h; en; en = en->next) {
				sql_exp *e = en->data;
				/* first check alias */
				if (!is_intern(e) && e->rname && strcmp(e->rname, tname) == 0)
					append(exps, exp_alias_or_copy(sql, tname, exp_name(e), rel, e, 1));
				if (!is_intern(e) && !e->rname && e->l && strcmp(e->l, tname) == 0)
					append(exps, exp_alias_or_copy(sql, tname, exp_name(e), rel, e, 1));
			}
			if (exps && list_length(exps))
				return exps;
		}
	default:
		return NULL;
	}
}

/* find the path to the relation containing the base of the expression
	(e_column), in most cases this means go down the join tree and
	find the base column.
 */
static int
rel_bind_path_(sql_rel *rel, sql_exp *e, list *path )
{
	int found = 0;

	switch (rel->op) {
	case op_join:
	case op_left:
	case op_right:
	case op_full:
		/* first right (possible subquery) */
		found = rel_bind_path_(rel->r, e, path);
		if (!found)
			found = rel_bind_path_(rel->l, e, path);
		break;
	case op_semi:
	case op_anti:

	case op_select:
	case op_topn:
		found = rel_bind_path_(rel->l, e, path);
		break;

	case op_union:
	case op_inter:
	case op_except:
		if (!rel->exps) {
			found = rel_bind_path_(rel->l, e, path);
			assert(0);
			break;
		}
	case op_groupby:
	case op_project:
	case op_table:
	case op_basetable:
		if (!rel->exps)
			break;
		if (!found && e->l && exps_bind_column2(rel->exps, e->l, e->r))
			found = 1;
		if (!found && !e->l && exps_bind_column(rel->exps, e->r, NULL))
			found = 1;
		break;
	case op_insert:
	case op_update:
	case op_delete:
		break;
	case op_ddl:
		break;
	}
	if (found)
		list_prepend(path, rel);
	return found;
}

static list *
rel_bind_path(sql_rel *rel, sql_exp *e )
{
	list *path = new_rel_list();

	if (e->type == e_convert)
		e = e->l;
	if (e->type == e_column) {
		if (rel) {
			if (!rel_bind_path_(rel, e, path)) {
				/* something is wrong */
				list_destroy(path);
				return NULL;
			}
		}
		return path;
	}
	/* default the top relation */
	append(path, rel);
	return path;
}


list *
rel_projections(mvc *sql, sql_rel *rel, char *tname, int settname, int intern )
{
	list *rexps, *exps ;

	if (is_subquery(rel) && is_project(rel->op))
		return list_create(NULL);

	switch(rel->op) {
	case op_join:
	case op_left:
	case op_right:
	case op_full:
		exps = rel_projections(sql, rel->l, tname, settname, intern );
		rexps = rel_projections(sql, rel->r, tname, settname, intern );
		exps = list_merge( exps, rexps, (fdup)NULL);
		return exps;
	case op_groupby:
	case op_project:
	case op_table:
	case op_basetable:
	case op_ddl:

	case op_union:
	case op_except:
	case op_inter:
		if (rel->exps) {
			node *en;

			exps = new_exp_list(sql->sa);
			for (en = rel->exps->h; en; en = en->next) {
				sql_exp *e = en->data;
				if (intern || !is_intern(e))
					append(exps, exp_alias_or_copy(sql, tname, exp_name(e), rel, e, settname));
			}
			return exps;
		}

		exps = rel_projections(sql, rel->l, tname, settname, intern );
		if (exps) {
			node *en;
			for (en = exps->h; en; en = en->next) {
				sql_exp *e = en->data;
				e->card = rel->card;
			}
		}
		return exps;
	case op_semi:
	case op_anti:

	case op_select:
	case op_topn:
		return rel_projections(sql, rel->l, tname, settname, intern );
	default:
		return NULL;
	}
}

sql_rel *
rel_copy( sql_allocator *sa, sql_rel *i )
{
	sql_rel *rel = rel_create(sa);

	rel->l = NULL;
	rel->r = NULL;
	rel->card = i->card;

	switch(i->op) {
	case op_basetable:
		rel->l = i->l;
		break;
	case op_table:
		rel->l = i->l;
		break;
	case op_groupby:
		rel->l = rel_copy(sa, i->l);
		if (i->r)
			rel->r = (i->r)?list_dup(i->r, (fdup)NULL):NULL;
		break;
	case op_join:
	case op_left:
	case op_right:
	case op_full:
	case op_semi:
	case op_anti:
	case op_project:
	case op_select:
	default:
		if (i->l)
			rel->l = rel_copy(sa, i->l);
		if (i->r)
			rel->r = rel_copy(sa, i->r);
		break;
	}
	rel->op = i->op;
	rel->exps = (i->exps)?list_dup(i->exps, (fdup)NULL):NULL;
	return rel;
}

sql_rel *
_rel_basetable(sql_allocator *sa, sql_table *t, char *atname)
{
	node *cn;
	sql_rel *rel = rel_create(sa);
	char *tname = t->base.name;

	assert(atname);
	rel->l = t;
	rel->r = NULL;
	rel->op = op_basetable;
	rel->exps = new_exp_list(sa);

	for (cn = t->columns.set->h; cn; cn = cn->next) {
		sql_column *c = cn->data;

		append(rel->exps, exp_alias(sa, atname, c->base.name, tname, c->base.name, &c->type, CARD_MULTI, c->null, 0));
	}
	append(rel->exps, exp_alias(sa, atname, "%TID%", tname, "%TID%", sql_bind_localtype("oid"), CARD_MULTI, 0, 1));

	if (t->idxs.set) {
		for (cn = t->idxs.set->h; cn; cn = cn->next) {
			sql_idx *i = cn->data;
			sql_subtype *t = sql_bind_localtype("wrd"); /* hash "wrd" */
			char *iname = sa_strconcat( sa, "%", i->base.name);

			if (i->type == join_idx)
				t = sql_bind_localtype("oid"); 
			/* index names are prefixed, to make them independent */
			append(rel->exps, exp_alias(sa, atname, iname, tname, iname, t, CARD_MULTI, 0, 1));
		}
	}

	rel->card = CARD_MULTI;
	rel->nrcols = list_length(t->columns.set);
	return rel;
}

sql_rel *
rel_basetable(mvc *sql, sql_table *t, char *tname)
{
#if 0
	if (isMergeTable(t)) {
		/* instantiate merge tabel */
		sql_rel *rel = NULL;

		if (sql->emode == m_deps) {
			rel = _rel_basetable(sql->sa, t, tname);
		} else {
			node *n;

			if (list_empty(t->tables.set)) 
				rel = _rel_basetable(sql->sa, t, tname);
			if (t->tables.set) {
				for (n = t->tables.set->h; n; n = n->next) {
					sql_table *pt = n->data;
					sql_rel *prel = rel_basetable(sql, pt, tname);
					if (rel) { 
						rel = rel_setop(sql->sa, rel, prel, op_union);
						rel->exps = rel_projections(sql, rel, NULL, 1, 1);
					} else {
						rel = prel;
					}
				}
			}
		}
		return rel;
	}
#endif
	return _rel_basetable(sql->sa, t, tname);
}

sql_rel *
rel_table_func(sql_allocator *sa, sql_rel *l, sql_exp *f, list *exps)
{
	sql_rel *rel = rel_create(sa);

	rel->l = l;
	rel->r = f;
	rel->op = op_table;
	rel->exps = exps;
	rel->card = CARD_MULTI;
	rel->nrcols = list_length(exps);
	return rel;
}

sql_rel *
rel_recursive_func(sql_allocator *sa, list *exps)
{
	sql_rel *rel = rel_create(sa);

	rel->l = NULL;
	rel->r = NULL;
	rel->op = op_table;
	rel->exps = exps;
	rel->card = CARD_MULTI;
	if (exps)
		rel->nrcols = list_length(exps);
	return rel;
}

sql_rel *
rel_setop(sql_allocator *sa, sql_rel *l, sql_rel *r, operator_type setop)
{
	sql_rel *rel = rel_create(sa);

	rel->l = l;
	rel->r = r;
	rel->op = setop;
	rel->exps = NULL;
	rel->card = CARD_MULTI;
	if (l && r)
		rel->nrcols = l->nrcols + r->nrcols;
	return rel;
}

sql_rel *
rel_crossproduct(sql_allocator *sa, sql_rel *l, sql_rel *r, operator_type join)
{
	sql_rel *rel = rel_create(sa);

	rel->l = l;
	rel->r = r;
	rel->op = join;
	rel->exps = NULL;
	rel->card = CARD_MULTI;
	rel->nrcols = l->nrcols + r->nrcols;
	return rel;
}

void
rel_join_add_exp( sql_allocator *sa, sql_rel *rel, sql_exp *e)
{
	assert(is_join(rel->op) || is_semi(rel->op) || is_select(rel->op));

	if (!rel->exps)
		rel->exps = new_exp_list(sa);
	append(rel->exps, e);
	if (e->card > rel->card)
		rel->card = e->card;
}

static void
rel_join_add_exps( sql_allocator *sa, sql_rel *rel, list *exps)
{
	node *n;
	for(n = exps->h; n; n = n->next)
		rel_join_add_exp(sa, rel, n->data);
}

sql_rel *
rel_project(sql_allocator *sa, sql_rel *l, list *e)
{
	sql_rel *rel = rel_create(sa);

	rel->l = l;
	rel->r = NULL;
	rel->op = op_project;
	rel->exps = e;
	rel->card = CARD_MULTI;
	if (l) {
		rel->card = l->card;
		rel->nrcols = l->nrcols;
		assert (exps_card(rel->exps) <= rel->card);
	}
	return rel;
}

static sql_rel*
rel_project_exp(sql_allocator *sa, sql_exp *e)
{
	sql_rel *rel = rel_project(sa, NULL, append(new_exp_list(sa), e));

	set_processed(rel);
	return rel;
}

static sql_rel *
rel_distinct(sql_rel *l)
{
	if (l->card >= CARD_AGGR) /* in case of CARD_AGGR, we could
	                             do better, ie check the group by
	                             list etc */
		set_distinct(l);
	return l;
}


static sql_exp * exps_match(sql_exp *m, sql_exp *e);

static int
explists_match(list *m, list *e)
{
	node *nm,*ne;

	if (!m || !e)
		return (m==e);
	if (list_length(m) != list_length(e))
		return 0;
	for (nm = m->h, ne = e->h; nm && ne; nm = nm->next, ne = ne->next) {
		if (!exps_match(nm->data, ne->data))
			return 0;
	}
	return 1;
}

static sql_exp *
exps_match(sql_exp *m, sql_exp *e)
{
	if (m->type != e->type)
		return NULL;
	switch (m->type) {
	case e_column:
		if (strcmp(m->r, e->r) == 0) {
			if (m->l && e->l && (strcmp(m->l, e->l) == 0))
				return m;
			else if (!m->l && !e->l)
				return m;
		}
		break;
	case e_aggr:
		if (m->f == e->f && explists_match(m->l, e->l))
			return m;
		break;
	default:
		return NULL;
	}
	return NULL;
}

static sql_exp *
exps_find_match_exp(list *l, sql_exp *e)
{
	node *n;
	if (!l || !list_length(l))
		return NULL;

	for (n = l->h; n; n = n->next){
		sql_exp *m = n->data;
		if (exps_match(m,e))
			return m;
	}
	return NULL;
}

sql_exp *
rel_groupby_add_aggr(mvc *sql, sql_rel *rel, sql_exp *e)
{
	sql_exp *m = NULL, *ne;
	char name[16], *nme = NULL;
	char *tname = NULL;

	if ((m=exps_find_match_exp(rel->exps, e)) == NULL) {
		if (!e->name) {
			nme = number2name(name, 16, ++sql->label);
			exp_setname(sql->sa, e, NULL, nme);
		}
		append(rel->exps, e);
		m = e;
	}
	if (e->type == e_column)
		tname = e->l;
	ne = exp_column(sql->sa, tname, m->name, exp_subtype(m),
			rel->card, has_nil(m), is_intern(m));
	exp_setname(sql->sa, ne, NULL, e->name);
	return ne;
}

void
rel_project_add_exp( mvc *sql, sql_rel *rel, sql_exp *e)
{
	assert(is_project(rel->op));

	if (rel->op == op_project) {
		if (!rel->exps)
			rel->exps = new_exp_list(sql->sa);
		append(rel->exps, e);
	} else if (rel->op == op_groupby) {
		(void) rel_groupby_add_aggr(sql, rel, e);
	}
}

static sql_rel*
rel_parent( sql_rel *rel )
{
	if (is_project(rel->op) || rel->op == op_topn) {
		sql_rel *l = rel->l;
		if (is_project(l->op))
			return l;
	}
	return rel;
}

static sql_exp *
rel_lastexp(mvc *sql, sql_rel *rel )
{
	sql_exp *e;

	if (!is_processed(rel))
		rel = rel_parent(rel);
	assert(list_length(rel->exps));
	if (rel->op == op_project)
		return exp_alias_or_copy(sql, NULL, NULL, rel, rel->exps->t->data, 1);
	assert(is_project(rel->op));
	e = rel->exps->t->data;
	return exp_column(sql->sa, e->rname, e->name, exp_subtype(e), e->card, has_nil(e), is_intern(e));
}

static sql_exp *
rel_find_lastexp(sql_rel *rel )
{
	if (!is_processed(rel))
		rel = rel_parent(rel);
	assert(list_length(rel->exps));
	return rel->exps->t->data;
}

void
rel_select_add_exp(sql_rel *l, sql_exp *e)
{
	assert(l->op == op_select);
	append(l->exps, e);
}

sql_rel *
rel_select(sql_allocator *sa, sql_rel *l, sql_exp *e)
{
	sql_rel *rel;
	
	if (l && l->op == op_select && !rel_is_ref(l)) { /* refine old select */
		if (e)
			rel_select_add_exp(l, e);
		return l;
	}
	rel = rel_create(sa);
	rel->l = l;
	rel->r = NULL;
	rel->op = op_select;
	rel->exps = new_exp_list(sa);
	if (e)
		append(rel->exps, e);
	rel->card = CARD_ATOM; /* no relation */
	if (l) {
		rel->card = l->card;
		rel->nrcols = l->nrcols;
	}
	return rel;
}

sql_rel *
rel_select_copy(sql_allocator *sa, sql_rel *l, list *exps)
{
	sql_rel *rel = rel_create(sa);
	
	rel->l = l;
	rel->r = NULL;
	rel->op = op_select;
	rel->exps = list_dup(exps, (fdup)NULL);
	rel->card = CARD_ATOM; /* no relation */
	if (l) {
		rel->card = l->card;
		rel->nrcols = l->nrcols;
	}
	return rel;
}

static sql_rel*
rel_project2groupby(mvc *sql, sql_rel *g)
{
	if (g->op == op_project) {
		node *en;

		g->card = CARD_ATOM; /* no groupby expressions */
		g->op = op_groupby;
		g->r = new_exp_list(sql->sa); /* add empty groupby column list */
		
		for (en = g->exps->h; en; en = en->next) {
			sql_exp *e = en->data;

			if (e->card > g->card) {
				if (e->type == e_column && e->r) {
					return sql_error(sql, 02, "cannot use non GROUP BY column '%s' in query results without an aggregate function", (char *) e->r);
				} else {
					return sql_error(sql, 02, "cannot use non GROUP BY column in query results without an aggregate function");
				}
			}
		}
		return rel_project(sql->sa, g, rel_projections(sql, g, NULL, 1, 1));
	}
	return NULL;
}

sql_rel *
rel_groupby(sql_allocator *sa, sql_rel *l, list *groupbyexps )
{
	sql_rel *rel = rel_create(sa);
	list *aggrs = new_exp_list(sa);
	node *en;

	rel->card = CARD_ATOM;
	if (groupbyexps) {
		rel->card = CARD_AGGR;
		for (en = groupbyexps->h; en; en = en->next) {
			sql_exp *e = en->data, *ne;

			/* after the group by the cardinality reduces */
			e->card = rel->card;
			ne = exp_column(sa, exp_relname(e), exp_name(e), exp_subtype(e), exp_card(e), has_nil(e), 0);
			append(aggrs, ne);
		}
	}
	rel->l = l;
	rel->r = groupbyexps;
	rel->exps = aggrs;
	rel->nrcols = l->nrcols;
	rel->op = op_groupby;
	return rel;
}

sql_rel *
rel_orderby(mvc *sql, sql_rel *l, list *orderbyexps )
{
	if (l->op != op_project) {
		sql_rel *rel = rel_create(sql->sa);

		rel->l = l;
		rel->r = orderbyexps;
		rel->op = op_project;	
		rel->exps = rel_projections(sql, rel, NULL, 1, 0);
		rel->card = l->card;
		rel->nrcols = l->nrcols;
		return rel;
	}
	assert(l->op == op_project && !l->r);
	l->r = orderbyexps;
	return l;
}

sql_rel *
rel_topn(sql_allocator *sa, sql_rel *l, list *exps )
{
	sql_rel *rel = rel_create(sa);

	rel->l = l;
	rel->r = NULL;
	rel->op = op_topn;	
	rel->exps = exps;
	rel->card = l->card;
	rel->nrcols = l->nrcols;
	return rel;
}

static char * rel_get_name( sql_rel *rel )
{
	switch(rel->op) {
	case op_table:
		if (rel->r) 
			return exp_name(rel->r);
		return NULL;
	case op_basetable:
		return rel->r;
	default:
		if (rel->l)
			return rel_get_name(rel->l);
	}
	return NULL;
}

/* ls is the left expression of the select, rs is a simple atom, e is the
   select expression.
 */
sql_rel *
rel_push_select(sql_allocator *sa, sql_rel *rel, sql_exp *ls, sql_exp *e)
{
	list *l = rel_bind_path(rel, ls);
	node *n;
	sql_rel *lrel = NULL, *p = NULL;

	if (!l) {
		/* expression has no clear parent relation, so filter current
		   with it */
		return rel_select(sa, rel, e);
	}

	for (n = l->h; n; n = n->next ) {
		lrel = n->data;

		if (rel_is_ref(lrel))
			break;

		/* push down as long as the operators allow this */
		if (!is_select(lrel->op) &&
		    !(is_semi(lrel->op) && !rel_is_ref(lrel->l)) &&
		    lrel->op != op_join &&
		    lrel->op != op_left)
			break;
		/* pushing through left head of a left join is allowed */
		if (lrel->op == op_left && (
					!n->next || lrel->l != n->next->data))
			break;
		p = lrel;
	}
	list_destroy(l);
	if (!lrel) 
		return NULL;
	if (p && p->op == op_select && !rel_is_ref(p)) { /* refine old select */
		rel_select_add_exp(p, e);
	} else {
		sql_rel *n = rel_select(sa, lrel, e);

		if (p && p != lrel) {
			assert(p->op == op_join || p->op == op_left || is_semi(p->op));
			if (p->l == lrel) {
				assert(p->l != n);
				p->l = n;
			} else {
				assert(p->op == op_join && p->r == lrel);
				assert(p->r != n);
				p->r = n;
			}
		} else {
			if (rel != lrel)
				assert(0);
			rel = n;
		}
	}
	return rel;
}


/* ls and rs are the left and right expression of the join, e is the
   join expression.
 */
sql_rel *
rel_push_join(sql_allocator *sa, sql_rel *rel, sql_exp *ls, sql_exp *rs, sql_exp *e)
{
	list *l = rel_bind_path(rel, ls);
	list *r = rel_bind_path(rel, rs);
	node *ln, *rn;
	sql_rel *lrel = NULL, *rrel = NULL, *p = NULL;

	if (!l || !r) {
		if (l)
			list_destroy(l);
		if (r)
			list_destroy(r);
		return NULL;
	}

	p = rel;
	for (ln = l->h, rn = r->h; ln && rn; ln = ln->next, rn = rn->next ) {
		lrel = ln->data;
		rrel = rn->data;
		
		if (rel_is_ref(lrel) || rel_is_ref(rrel))
			break;

		/* push down as long as the operators allow this
			and the relation is equal.
		 */
		if (lrel != rrel ||
				(!is_select(lrel->op) &&
				 !(is_semi(lrel->op) && !rel_is_ref(lrel->l)) &&
				 lrel->op != op_join &&
				 lrel->op != op_left))
			break;
		/* pushing through left head of a left join is allowed */
		if (lrel->op == op_left && (
					!ln->next || lrel->l != ln->next->data))
			break;
		p = lrel;
	}
	list_destroy(l);
	list_destroy(r);
	if (!lrel || !rrel)
		return NULL;

	/* filter on columns of this relation */
	if ((lrel == rrel && lrel->op != op_join) || rel_is_ref(p)) {
		if (lrel->op == op_select && !rel_is_ref(lrel)) {
			rel_select_add_exp(lrel, e);
		} else if (p && p->op == op_select && !rel_is_ref(p)) {
			rel_select_add_exp(p, e);
		} else {
			sql_rel *n = rel_select(sa, lrel, e);

			if (p && p != lrel) {
				if (p->l == lrel)
					p->l = n;
				else
					p->r = n;
			} else {
				rel = n;
			}
		}
		return rel;
	}

	rel_join_add_exp( sa, p, e);
	return rel;
}

/* forward refs */
static sql_rel * rel_setquery(mvc *sql, sql_rel *rel, symbol *sq);
static sql_rel * rel_joinquery(mvc *sql, sql_rel *rel, symbol *sq);
static sql_rel * rel_crossquery(mvc *sql, sql_rel *rel, symbol *q);
static sql_rel * rel_unionjoinquery(mvc *sql, sql_rel *rel, symbol *sq);

void
rel_add_intern(mvc *sql, sql_rel *rel)
{
	if (rel->op == op_project && rel->l && rel->exps && !need_distinct(rel)) {
		list *prjs = rel_projections(sql, rel->l, NULL, 1, 1);
		node *n;
	
		for(n=prjs->h; n; n = n->next) {
			sql_exp *e = n->data;

			if (is_intern(e)) {
				append(rel->exps, e);
				n->data = NULL;
			}
		}
	}
}

static sql_rel *
rel_table_optname(mvc *sql, sql_rel *sq, symbol *optname)
{
	(void)sql;
	if (optname && optname->token == SQL_NAME) {
		dlist *columnrefs = NULL;
		char *tname = optname->data.lval->h->data.sval;

		columnrefs = optname->data.lval->h->next->data.lval;
		if (columnrefs && sq->exps) {
			dnode *d = columnrefs->h;
			node *ne = sq->exps->h;

			for (; d && ne; d = d->next, ne = ne->next)
				exp_setname(sql->sa, ne->data, tname, d->data.sval );
		}
		if (!columnrefs && sq->exps) {
			node *ne = sq->exps->h;

			for (; ne; ne = ne->next)
				exp_setname(sql->sa, ne->data, tname, NULL );
		}
	}
	rel_add_intern(sql, sq);
	return sq;
}

static sql_rel *
rel_subquery_optname(mvc *sql, sql_rel *rel, symbol *query)
{
	SelectNode *sn = (SelectNode *) query;
	exp_kind ek = {type_value, card_relation, TRUE};
	sql_rel *sq = rel_subquery(sql, rel, query, ek);

	assert(query->token == SQL_SELECT);
	if (!sq)
		return NULL;

	return rel_table_optname(sql, sq, sn->name);
}

static sql_rel *
query_exp_optname(mvc *sql, sql_rel *r, symbol *q)
{
	switch (q->token) {
	case SQL_UNION:
	case SQL_EXCEPT:
	case SQL_INTERSECT:
	{
		sql_rel *tq = rel_setquery(sql, r, q);

		if (!tq)
			return NULL;
		return rel_table_optname(sql, tq, q->data.lval->t->data.sym);
	}
	case SQL_JOIN:
	{
		sql_rel *tq = rel_joinquery(sql, r, q);

		if (!tq)
			return NULL;
		return rel_table_optname(sql, tq, q->data.lval->t->data.sym);
	}
	case SQL_CROSS:
	{
		sql_rel *tq = rel_crossquery(sql, r, q);

		if (!tq)
			return NULL;
		return rel_table_optname(sql, tq, q->data.lval->t->data.sym);
	}
	case SQL_UNIONJOIN:
	{
		sql_rel *tq = rel_unionjoinquery(sql, r, q);

		if (!tq)
			return NULL;
		return rel_table_optname(sql, tq, q->data.lval->t->data.sym);
	}
	default:
		(void) sql_error(sql, 02, "case %d %s\n", q->token, token2string(q->token));
	}
	return NULL;
}

static sql_rel *
rel_bind_column_(mvc *sql, sql_rel **p, sql_rel *rel, char *cname )
{
	int ambiguous = 0;
	sql_rel *l = NULL, *r = NULL;
	switch(rel->op) {
	case op_join:
	case op_left:
	case op_right:
	case op_full: {
		sql_rel *right = rel->r;

		*p = rel;
		r = rel_bind_column_(sql, p, rel->r, cname);

		if (!r || !rel_issubquery(right)) {
			*p = rel;
			l = rel_bind_column_(sql, p, rel->l, cname);
			if (l && r && !rel_issubquery(r)) {
				(void) sql_error(sql, ERR_AMBIGUOUS, "SELECT: identifier '%s' ambiguous", cname);
				return NULL;
			}
		}
		if (sql->session->status == -ERR_AMBIGUOUS) 
			return NULL;
		if (l && !r)
			return l;
		return r;
	}
	case op_union:
	case op_except:
	case op_inter:
	case op_groupby:
	case op_project:
	case op_table:
	case op_basetable:
		if (rel->exps && exps_bind_column(rel->exps, cname, &ambiguous))
			return rel;
		if (ambiguous) {
			(void) sql_error(sql, ERR_AMBIGUOUS, "SELECT: identifier '%s' ambiguous", cname);
			return NULL;
		}
		*p = rel;
		if (is_processed(rel))
			return NULL;
		if (rel->l && !(is_base(rel->op)))
			return rel_bind_column_(sql, p, rel->l, cname );
		break;
	case op_semi:
	case op_anti:

	case op_select:
	case op_topn:
		*p = rel;
		return rel_bind_column_(sql, p, rel->l, cname);
	default:
		return NULL;
	}
	return NULL;
}

#if 0
static sql_exp *
rel_find_column( mvc *sql, sql_rel *rel, char *cname )
{
	sql_rel *p = NULL;

	if (!rel || (rel = rel_bind_column_(sql, &p, rel, cname)) == NULL)
		return NULL;

	if (is_project(rel->op) || rel->op == op_table) {
		if (rel->exps) {
			sql_exp *e = exps_bind_column(rel->exps, cname, NULL);
			if (e)
				return e;
		}
	}
	return NULL;
}
#endif

sql_exp *
rel_bind_column( mvc *sql, sql_rel *rel, char *cname, int f )
{
	sql_rel *p = NULL;

	if (f == sql_sel && rel && is_project(rel->op) && !is_processed(rel))
		rel = rel->l;

	if (!rel || (rel = rel_bind_column_(sql, &p, rel, cname)) == NULL)
		return NULL;

	if ((is_project(rel->op) || is_base(rel->op)) && rel->exps) {
		sql_exp *e = exps_bind_column(rel->exps, cname, NULL);
		if (e)
			return exp_alias_or_copy(sql, e->rname, cname, rel, e, 1);
	}
	return NULL;
}

sql_exp *
rel_bind_column2( mvc *sql, sql_rel *rel, char *tname, char *cname, int f )
{
	if (f == sql_sel && rel && is_project(rel->op) && !is_processed(rel))
		rel = rel->l;

	if (!rel)
		return NULL;

	if (rel->exps) {
		sql_exp *e = exps_bind_column2(rel->exps, tname, cname);
		if (e)
			return exp_alias_or_copy(sql, tname, cname, rel, e, 1);
	}
	if (is_project(rel->op) && rel->l) {
		if (!is_processed(rel))
			return rel_bind_column2(sql, rel->l, tname, cname, f);
	} else if (is_join(rel->op)) {
		sql_exp *e = rel_bind_column2(sql, rel->l, tname, cname, f);
		if (!e)
			e = rel_bind_column2(sql, rel->r, tname, cname, f);
		return e;
	} else if (is_set(rel->op) ||
		   is_sort(rel) ||
		   is_semi(rel->op) ||
		   is_select(rel->op)) {
		if (rel->l)
			return rel_bind_column2(sql, rel->l, tname, cname, f);
	}
	return NULL;
}

static sql_rel *
rel_named_table_function(mvc *sql, sql_rel *rel, symbol *query)
{
	list *exps = NULL;
	sql_subtype *st = NULL;
	node *m;
	exp_kind ek = {type_value, card_relation, TRUE};
	sql_exp *e = rel_value_exp(sql, &rel, query->data.lval->h->data.sym, sql_from, ek);
	char *tname = NULL;
	if (!e)
		return NULL;

	if (query->data.lval->h->next->data.sym)
		tname = query->data.lval->h->next->data.sym->data.lval->h->data.sval;

	/* column or table function */
	st = exp_subtype(e);
	if (!st->comp_type) {
		(void) sql_error(sql, 02, "SELECT: '%s' does not return a table", exp_func_name(e));
		return NULL;
	}

	/* for each column add table.column name */
	exps = new_exp_list(sql->sa);
	for (m = st->comp_type->columns.set->h; m; m = m->next) {
		sql_column *c = m->data;
		append(exps, exp_column(sql->sa, tname, c->base.name, &c->type, CARD_MULTI, c->null, 0));
	}
	return rel_table_func(sql->sa, rel, e, exps);
}

static sql_exp *
rel_op_(mvc *sql, sql_schema *s, char *fname, exp_kind ek)
{
	sql_subfunc *f = NULL;

	f = sql_bind_func(sql->sa, s, fname, NULL, NULL);
	if (f && 
	   ((ek.card == card_relation && f->res.comp_type) || 
	    (ek.card == card_none && !f->res.type) || 
	    (ek.card != card_none && ek.card != card_relation && f->res.type && !f->res.comp_type))) {
		return exp_op(sql->sa, NULL, f);
	} else {
		return sql_error(sql, 02,
			"SELECT: no such operator '%s'", fname);
	}
}

/* special class of table returning function, but with a table input as well */
static sql_rel *
rel_named_table_operator(mvc *sql, sql_rel *rel, symbol *query)
{
	exp_kind ek = {type_value, card_relation, TRUE};
	sql_rel *sq = rel_subquery(sql, rel, query->data.lval->h->data.sym, ek);
	sql_table *t;
	sql_func *f;
	sql_subfunc *sf;
	sql_exp *e;
	list *exps;
	dlist *column_spec = NULL;
	node *m;
	int nr = ++sql->label;
	char *tname = NULL, name[16], *nme;

	nme = number2name(name, 16, nr);

	if (!sq)
		return NULL;
	
	if (query->data.lval->h->next->data.sym) {
		tname = query->data.lval->h->next->data.sym->data.lval->h->data.sval;
		column_spec = query->data.lval->h->next->data.sym->data.lval->h->next->data.lval;
	}

	/* TODO niels this needs a cleanup, shouldn't be needed anymore */
	if ((t = mvc_create_table_as_subquery(sql, sq, sql->session->schema, tname, column_spec, tt_stream, CA_COMMIT)) == NULL) {
		rel_destroy(sq);
		return NULL;
	}
	backend_create_table_function(sql, nme, sq, t);

	f = SA_NEW(sql->sa, sql_func);
	base_init(sql->sa, &f->base, store_next_oid(), TR_OLD, nme);
	f->mod = sa_strdup(sql->sa, "user");
	f->imp = sa_strdup(sql->sa, nme);
	f->ops = NULL;
	f->res = *sql_bind_localtype("bat");
	f->res.comp_type = t;
	f->nr = 0;
	f->sql = 0;
	f->aggr = 0;
	f->side_effect = 0;
	f->fix_scale = 0;
	f->s = NULL;
	sf = SA_NEW(sql->sa, sql_subfunc);
	sf->func = f;
	sf->res = f->res;
	e = exp_op(sql->sa, NULL, sf);
	exps = new_exp_list(sql->sa);
	for (m = t->columns.set->h; m; m = m->next) {
		sql_column *c = m->data;
		append(exps, exp_column(sql->sa, tname, c->base.name, &c->type, CARD_MULTI, c->null, 0));
	}
	return rel_table_func(sql->sa, sq, e, exps);
}

static sql_rel *
rel_values( mvc *sql, symbol *tableref)
{
	sql_rel *r = NULL;
	dlist *rowlist = tableref->data.lval;
	symbol *optname = rowlist->t->data.sym;
	dnode *o;
	node *m;
	list *exps = list_new(sql->sa); 

	exp_kind ek = {type_value, card_value, TRUE};
	if (!rowlist->h)
		r = rel_project(sql->sa, NULL, NULL);

	/* last element in the list is the table_name */
	for (o = rowlist->h; o->next; o = o->next) {
		dlist *values = o->data.lval;

		if (r && list_length(r->exps) != dlist_length(values)) {
			return sql_error(sql, 02, "VALUES: number of values doesn't match");
		} else {
			dnode *n;

			if (list_empty(exps)) {
				for (n = values->h; n; n = n->next) {
					sql_exp *vals = exp_values(sql->sa, list_new(sql->sa));
					list_append(exps, vals);
					exp_label(sql->sa, vals, ++sql->label);
				}
			}
			for (n = values->h, m = exps->h; n && m; 
					n = n->next, m = m->next) {
				sql_exp *vals = m->data;
				list *vals_list = vals->f;
				sql_exp *e = rel_value_exp(sql, NULL, n->data.sym, sql_sel, ek);
				if (!e) 
					return NULL;
				list_append(vals_list, e);
			}
		}
	}
	/* loop to check types */
	for (m = exps->h; m; m = m->next) {
		node *n;
		sql_exp *vals = m->data;
		list *vals_list = vals->f;
		list *nexps = list_new(sql->sa);

		/* first get super type */
		vals->tpe = *exp_subtype(vals_list->h->data);

		for (n = vals_list->h; n; n = n->next) {
			sql_exp *e = n->data;
			sql_subtype super;

			supertype(&super, &vals->tpe, exp_subtype(e));
			vals->tpe = super;
		}
		for (n = vals_list->h; n; n = n->next) {
			sql_exp *e = n->data;
			
			e = rel_check_type(sql, &vals->tpe, e, type_equal);
			if (!e)
				return NULL;
			append(nexps, e); 
		}
		vals->f = nexps;
	}
	r = rel_project(sql->sa, NULL, exps);
	set_processed(r);
	rel_table_optname(sql, r, optname);
	return r;
}

static sql_rel *
table_ref(mvc *sql, sql_rel *rel, symbol *tableref)
{
	char *tname = NULL;
	sql_table *t = NULL;

	(void)rel;
	if (tableref->token == SQL_NAME) {
		sql_rel *temp_table = NULL;
		char *sname = qname_schema(tableref->data.lval->h->data.lval);
		sql_schema *s = NULL;
		tname = qname_table(tableref->data.lval->h->data.lval);

		if (sname && !(s=mvc_bind_schema(sql,sname)))
			return sql_error(sql, 02, "SELECT: no such schema '%s'", sname);
		/* TODO: search path */
		if (!t && !sname) {
			sql_subtype *tpe;
			if ((tpe = stack_find_type(sql, tname)) != NULL &&
				tpe->comp_type) {
				temp_table = stack_find_rel_var(sql, tname);
				t = tpe->comp_type;
			} else {
				temp_table = stack_find_rel_view(sql, tname);
			}
			if (temp_table)
				rel_dup(temp_table);
		}
		if (!t && !temp_table) {
			if (!s)
				s = cur_schema(sql);
			t = mvc_bind_table(sql, s, tname);
			if (!t && !sname) {
				s = tmp_schema(sql);
				t = mvc_bind_table(sql, s, tname);
			}
		}
		if (!t && !temp_table) {
			return sql_error(sql, 02, "SELECT: no such table '%s'", tname);
		} else if (!temp_table && !table_privs(sql, t, PRIV_SELECT)) {
			return sql_error(sql, 02, "SELECT: access denied for %s to table '%s.%s'", stack_get_string(sql, "current_user"), s->base.name, tname);
		}
		if (tableref->data.lval->h->next->data.sym) {	/* AS */
			tname = tableref->data.lval->h->next->data.sym->data.lval->h->data.sval;
		}
		if (temp_table && !t) {
			node *n;
			list *exps = rel_projections(sql, temp_table, NULL, 1, 1);

			temp_table = rel_project(sql->sa, temp_table, exps);
			set_processed(temp_table);
			for (n = exps->h; n; n = n->next)
				exp_setname(sql->sa, n->data, tname, NULL);
			return temp_table;
		} else if (isView(t) /*&& sql->emode != m_instantiate */) {
			/* instantiate base view */
			node *n,*m;
			sql_rel *rel;

			if (sql->emode == m_deps)
				rel = rel_basetable(sql, t, tname);
			else
				rel = rel_parse(sql, t->query, m_instantiate);

			if (!rel)
				return rel;

			/* Direct renaming of rel_parse relation */
			if (sql->emode != m_deps) {
				assert(is_project(rel->op));
				for (n = t->columns.set->h, m = rel->exps->h; n && m; n = n->next, m = m->next) {
					sql_column *c = n->data;
					sql_exp *e = m->data;
	
					exp_setname(sql->sa, e, tname, c->base.name);
				}
			}
			return rel;
		}
		return rel_basetable(sql, t, tname);
	} else if (tableref->token == SQL_VALUES) {
		return rel_values(sql, tableref);
	} else if (tableref->token == SQL_TABLE) {
		return rel_named_table_function(sql, rel, tableref);
	} else if (tableref->token == SQL_TABLE_OPERATOR) {
		return rel_named_table_operator(sql, rel, tableref);
	} else if (tableref->token == SQL_SELECT) {
		return rel_subquery_optname(sql, rel, tableref);
	} else {
		return query_exp_optname(sql, rel, tableref);
	}
}

static sql_exp *
rel_var_ref(mvc *sql, char *name, int at)
{
	if (stack_find_var(sql, name)) {
		sql_subtype *tpe = stack_find_type(sql, name);
		int frame = stack_find_frame(sql, name);

		return exp_param(sql->sa, name, tpe, frame);
	} else if (at) {
		return sql_error(sql, 02, "SELECT: '@""%s' unknown", name);
	} else {
		return sql_error(sql, 02, "SELECT: identifier '%s' unknown", name);
	}
}

static sql_exp *
exps_get_exp(list *exps, int nth)
{
	node *n = NULL;
	int i = 0;

	if (exps)
		for (n=exps->h, i=1; n && i<nth; n=n->next, i++)
			;
	if (n && i == nth)
		return n->data;
	return NULL;
}

static sql_exp *
rel_column_ref(mvc *sql, sql_rel **rel, symbol *column_r, int f)
{
	sql_exp *exp = NULL;
	dlist *l = column_r->data.lval;

	assert(column_r->token == SQL_COLUMN &&
		(column_r->type == type_list || column_r->type == type_int));

	if (dlist_length(l) == 1 && l->h->type == type_int) {
		int nr = l->h->data.i_val;
		atom *a;
		assert(l->h->type == type_int);
		if ((a = sql_bind_arg(sql, nr)) != NULL)
			return exp_atom_ref(sql->sa, nr, atom_type(a));
		return NULL;
	} else if (dlist_length(l) == 1) {
		char *name = l->h->data.sval;
		sql_arg *a = sql_bind_param(sql, name);
		int var = stack_find_var(sql, name);
		
		if (rel && *rel)
			exp = rel_bind_column(sql, *rel, name, f);
		if (exp) {
			if (var || a)
				return sql_error(sql, ERR_AMBIGUOUS, "SELECT: identifier '%s' ambiguous", name);
		} else if (a) {
			if (var)
				return sql_error(sql, ERR_AMBIGUOUS, "SELECT: identifier '%s' ambiguous", name);
			exp = exp_param(sql->sa, a->name, &a->type, 0);
		}
		if (!exp && var)
			return rel_var_ref(sql, name, 0);
		if (!exp && !var)
			return sql_error(sql, 02, "SELECT: identifier '%s' unknown", name);
		
	} else if (dlist_length(l) == 2) {
		char *tname = l->h->data.sval;
		char *cname = l->h->next->data.sval;

		if (rel && *rel)
			exp = rel_bind_column2(sql, *rel, tname, cname, f);

		/* some views are just in the stack,
		   like before and after updates views */
		if (!exp) {
			sql_rel *v = stack_find_rel_view(sql, tname);

			if (v) {
				rel_dup(v);
				if (*rel)
					*rel = rel_crossproduct(sql->sa, *rel, v, op_join);
				else
					*rel = v;
				exp = rel_bind_column(sql, *rel, cname, f);
			}
		}
		if (!exp)
			return sql_error(sql, 02, "SELECT: no such column '%s.%s'", tname, cname);
	} else if (dlist_length(l) >= 3) {
		return sql_error(sql, 02, "TODO: column names of level >= 3");
	}
	return exp;
}

static lng
scale2value(int scale)
{
	lng val = 1;

	if (scale < 0)
		scale = -scale;
	for (; scale; scale--) {
		val = val * 10;
	}
	return val;
}

static sql_exp *
exp_fix_scale(mvc *sql, sql_subtype *ct, sql_exp *e, int both, int always)
{
	sql_subtype *et = exp_subtype(e);

	if (ct->type->scale == SCALE_FIX && et->type->scale == SCALE_FIX) {
		int scale_diff = ((int) ct->scale - (int) et->scale);

		if (scale_diff) {
			sql_subtype *it = sql_bind_localtype(et->type->base.name);
			sql_subfunc *c = NULL;

			if (scale_diff < 0) {
				if (!both)
					return e;
				c = sql_bind_func(sql->sa, sql->session->schema, "scale_down", et, it);
			} else {
				c = sql_bind_func(sql->sa, sql->session->schema, "scale_up", et, it);
			}
			if (c) {
				lng val = scale2value(scale_diff);
				atom *a = atom_int(sql->sa, it, val);

				c->res.scale = (et->scale + scale_diff);
				return exp_binop(sql->sa, e, exp_atom(sql->sa, a), c);
			}
		}
	} else if (always && et->scale) {	/* scale down */
		int scale_diff = -(int) et->scale;
		sql_subtype *it = sql_bind_localtype(et->type->base.name);
		sql_subfunc *c = sql_bind_func(sql->sa, sql->session->schema, "scale_down", et, it);

		if (c) {
			lng val = scale2value(scale_diff);
			atom *a = atom_int(sql->sa, it, val);

			c->res.scale = 0;
			return exp_binop(sql->sa, e, exp_atom(sql->sa, a), c);
		} else {
			printf("scale_down missing (%s)\n", et->type->base.name);
		}
	}
	return e;
}

static int
rel_set_type_param(mvc *sql, sql_subtype *type, sql_exp *param)
{
	if (!type || !param || param->type != e_atom)
		return -1;

	if (set_type_param(sql, type, param->flag) == 0) {
		param->tpe = *type;
		return 0;
	}
	return -1;
}

/* try to do an in-place conversion
 *
 * in-place conversion is only possible if the exp is a variable.
 * This is only done to be able to map more cached queries onto the same
 * interface.
 */

static void
convert_arg(mvc *sql, int nr, sql_subtype *rt)
{
	atom *a = sql_bind_arg(sql, nr);

	if (atom_null(a)) {
		if (a->data.vtype != rt->type->localtype) {
			ptr p;

			a->data.vtype = rt->type->localtype;
			p = ATOMnilptr(a->data.vtype);
			VALset(&a->data, a->data.vtype, p);
		}
	}
	a->tpe = *rt;
}

static sql_exp *
exp_convert_inplace(mvc *sql, sql_subtype *t, sql_exp *exp)
{
	atom *a;

	/* exclude named variables */
	if (exp->type != e_atom || exp->l /* atoms */ || exp->r /* named */ ||
		(t->scale && t->type->eclass != EC_FLT))
		return NULL;

	a = sql_bind_arg(sql, exp->flag);
	if (a && atom_cast(a, t)) {
		convert_arg(sql, exp->flag, t);
		exp->tpe = *t;
		return exp;
	}
	return NULL;
}

sql_exp *
rel_check_type(mvc *sql, sql_subtype *t, sql_exp *exp, int tpe)
{
	int err = 0;
	sql_exp* nexp = NULL;
	sql_subtype *fromtype = exp_subtype(exp);
	
	if ((!fromtype || !fromtype->type) && rel_set_type_param(sql, t, exp) == 0)
		return exp;

	/* first try cheap internal (in-place) conversions ! */
	if ((nexp = exp_convert_inplace(sql, t, exp)) != NULL)
		return nexp;

	if (fromtype && subtype_cmp(t, fromtype) != 0) {
		int c = sql_type_convert(fromtype->type->eclass, t->type->eclass);
		if (!c ||
		   (c == 2 && tpe == type_set) || (c == 3 && tpe != type_cast)){
			err = 1;
		} else {
			exp = exp_convert(sql->sa, exp, fromtype, t);
		}
	}
	if (err) {
		sql_exp *res = sql_error(
			sql, 03,
			"types %s(%d,%d) and %s(%d,%d) are not equal%s%s%s",
			fromtype->type->sqlname,
			fromtype->digits,
			fromtype->scale,
			t->type->sqlname,
			t->digits,
			t->scale,
			(exp->type == e_column ? " for column '" : ""),
			(exp->type == e_column ? exp->name : ""),
			(exp->type == e_column ? "'" : "")
		);
		return res;
	}
	return exp;
}

static sql_exp *
exp_sum_scales(mvc *sql, sql_subfunc *f, sql_exp *l, sql_exp *r)
{
	if (strcmp(f->func->imp, "*") == 0 &&
			f->func->res.type->scale == SCALE_FIX)
	{
		sql_subtype t;
		sql_subtype *lt = exp_subtype(l);
		sql_subtype *rt = exp_subtype(r);

		f->res.scale = lt->scale + rt->scale;
		f->res.digits = lt->digits + rt->digits;

		/* HACK alert: digits should be less then max */
		if (f->res.type->radix == 10 && f->res.digits > 19)
			f->res.digits = 19;
		if (f->res.type->radix == 2 && f->res.digits > 53)
			f->res.digits = 53;

		/* sum of digits may mean we need a bigger result type
		 * as the function don't support this we need to
		 * make bigger input types!
		 */

		/* numeric types are fixed length */
		if (f->res.type->eclass == EC_NUM) {
			sql_find_numeric(&t, f->res.type->localtype, f->res.digits);
		} else {
			sql_find_subtype(&t, f->res.type->sqlname, f->res.digits, f->res.scale);
		}
		if (type_cmp(t.type, f->res.type) != 0) {
			/* do we need to convert to the a larger localtype
			   int * int may not fit in an int, so we need to
			   convert to lng * int.
			 */
			sql_subtype nlt;

			sql_init_subtype(&nlt, t.type, f->res.digits, lt->scale);
			l = rel_check_type( sql, &nlt, l, type_equal );
		}
		f->res = t;
	}
	return l;
}

static sql_exp *
exp_scale_algebra(mvc *sql, sql_subfunc *f, sql_exp *l, sql_exp *r)
{
	sql_subtype *lt = exp_subtype(l);
	sql_subtype *rt = exp_subtype(r);

	if (lt->type->scale == SCALE_FIX && rt->scale &&
		strcmp(f->func->imp, "/") == 0) {
		int scale, digits, digL, scaleL;
		sql_subtype nlt;

		/* scale fixing may require a larger type ! */
		scaleL = (lt->scale < 3) ? 3 : lt->scale;
		scale = scaleL;
		scaleL += rt->scale;
		digL = lt->digits + (scaleL - lt->scale);
		digits = (digL > (int)rt->digits) ? digL : (int)rt->digits;

		/* HACK alert: digits should be less then max */
		if (f->res.type->radix == 10 && digits > 19)
			digits = 19;
		if (f->res.type->radix == 2 && digits > 53)
			digits = 53;

		sql_find_subtype(&nlt, lt->type->sqlname, digL, scaleL);
		l = rel_check_type( sql, &nlt, l, type_equal );

		sql_find_subtype(&f->res, lt->type->sqlname, digits, scale);
	}
	return l;
}

int
rel_convert_types(mvc *sql, sql_exp **L, sql_exp **R, int scale_fixing, int tpe)
{
	sql_exp *ls = *L;
	sql_exp *rs = *R;
	sql_subtype *lt = exp_subtype(ls);
	sql_subtype *rt = exp_subtype(rs);

	if (!rt && !lt) {
		sql_error(sql, 01, "Cannot have a parameter (?) on both sides of an expression");
		return -1;
	}
	if (rt && (!lt || !lt->type))
		 return rel_set_type_param(sql, rt, ls);
	if (lt && (!rt || !rt->type))
		 return rel_set_type_param(sql, lt, rs);

	if (rt && lt) {
		sql_subtype *i = lt;
		sql_subtype *r = rt;

		if (subtype_cmp(lt, rt) != 0) {
			sql_subtype super;

			supertype(&super, r, i);
			if (scale_fixing) {
				/* convert ls to super type */
				ls = rel_check_type(sql, &super, ls, tpe);
				/* convert rs to super type */
				rs = rel_check_type(sql, &super, rs, tpe);
			} else {
				/* convert ls to super type */
				super.scale = lt->scale;
				ls = rel_check_type(sql, &super, ls, tpe);
				/* convert rs to super type */
				super.scale = rt->scale;
				rs = rel_check_type(sql, &super, rs, tpe);
			}
		}
		*L = ls;
		*R = rs;
		if (!ls || !rs) {
			return -1;
		}
		return 0;
	}
	return -1;
}

static comp_type
compare_str2type( char *compare_op)
{
	comp_type type = cmp_equal;

	if (compare_op[0] == '=') {
		type = cmp_equal;
	} else if (compare_op[0] == '<') {
		type = cmp_lt;
		if (compare_op[1] != '\0') {
			if (compare_op[1] == '>')
				type = cmp_notequal;
			else if (compare_op[1] == '=')
				type = cmp_lte;
		}
	} else if (compare_op[0] == '>') {
		type = cmp_gt;
		if (compare_op[1] != '\0')
			if (compare_op[1] == '=')
				type = cmp_gte;
	} else if (compare_op[0] == 'l') {
		type = cmp_like;
	} else if (compare_op[0] == 'i') {
		type = cmp_ilike;
	} else if (compare_op[0] == 'n') {
		if (strcmp(compare_op, "not_like") == 0) {
			type = cmp_notlike;
		} else {
			type = cmp_notilike;
		}
	}
	return type;
}

static sql_rel *
rel_compare_exp_(mvc *sql, sql_rel *rel, sql_exp *ls, sql_exp *rs, sql_exp *rs2, int type, int anti )
{
	sql_exp *L = ls, *R = rs, *e = NULL;

	if (rel_convert_types(sql, &ls, &rs, 1, type_equal) < 0 ||
	   (rs2 && rel_convert_types(sql, &ls, &rs2, 1, type_equal) < 0)) 
		return NULL;
	if (!rs2 && type != cmp_like && type != cmp_notlike &&
			type != cmp_ilike && type != cmp_notilike)
	{
		if (ls->card < rs->card) {
			sql_exp *swap = ls;
	
			ls = rs;
			rs = swap;

			swap = L;
			L = R;
			R = swap;

			type = (int)swap_compare((comp_type)type);
		}
		e = exp_compare(sql->sa, ls, rs, type);
	} else {
		e = exp_compare2(sql->sa, ls, rs, rs2, type);
	}
	if (anti)
		set_anti(e);

	/* atom or row => select */
	if (ls->card > rel->card) {
		if (ls->name)
			return sql_error(sql, 02, "SELECT: cannot use non GROUP BY column '%s' in query results without an aggregate function", ls->name);
		else
			return sql_error(sql, 02, "SELECT: cannot use non GROUP BY column in query results without an aggregate function");
	}
	if (rs->card > rel->card || (rs2 && rs2->card > rel->card)) {
		if (rs->name)
			return sql_error(sql, 02, "SELECT: cannot use non GROUP BY column '%s' in query results without an aggregate function", rs->name);
		else
			return sql_error(sql, 02, "SELECT: cannot use non GROUP BY column in query results without an aggregate function");
	}
	if (rs->card <= CARD_ATOM && exp_is_atom(rs) && 
	   (!rs2 || (rs2->card <= CARD_ATOM && exp_is_atom(rs2)))) {
		if (ls->card == rs->card && !rs2)  /* bin compare op */
			return rel_select(sql->sa, rel, e);

		/* push select into the given relation */
		return rel_push_select(sql->sa, rel, L, e);
	} else { /* join */
		if (is_semi(rel->op)) {
			rel_join_add_exp(sql->sa, rel, e);
			return rel;
		}
		return rel_push_join(sql->sa, rel, L, R, e);
	}
}

static sql_rel *
rel_compare_exp(mvc *sql, sql_rel *rel, sql_exp *ls, sql_exp *rs,
		char *compare_op, sql_exp *esc, int reduce)
{
	comp_type type = cmp_equal;

	if (!ls || !rs)
		return NULL;

	if (!rel || !reduce) {
		sql_exp *e;

		if (rel_convert_types(sql, &ls, &rs, 1, type_equal) < 0) 
			return NULL;
		e = rel_binop_(sql, ls, rs, NULL, compare_op, card_value);

		if (!e)
			return NULL;
		if (!reduce) {
			if (rel->op == op_project) {
				append(rel->exps, e);
			} else {
				list *exps = new_exp_list(sql->sa);

				append(exps, e);
				return rel_project(sql->sa, rel, exps);
			}
		} else {
			return rel_select(sql->sa, rel, e);
		}
	}
	type = compare_str2type(compare_op);
	return rel_compare_exp_(sql, rel, ls, rs, esc, type, 0);
}

static sql_rel *
rel_compare(mvc *sql, sql_rel *rel, symbol *lo, symbol *ro,
		char *compare_op, int f, exp_kind k)
{
	sql_exp *rs = 0, *ls;
	exp_kind ek = {type_value, card_column, FALSE};

	if (lo->token == SQL_SELECT) { /* swap subquery to the right hand side */
		symbol *tmp = lo;

		lo = ro;
		ro = tmp;

		if (compare_op[0] == '>')
			compare_op[0] = '<';
		else if (compare_op[0] == '<')
			compare_op[0] = '>';
	}

	ls = rel_value_exp(sql, &rel, lo, f, ek);
	if (!ls)
		return NULL;
	if (ro->token != SQL_SELECT) {
		rs = rel_value_exp(sql, &rel, ro, f, ek);
	} else {
		/* first try without current relation, too see if there
		   are correlations with the outer relation */
		sql_rel *r = rel_subquery(sql, NULL, ro, ek);

		if (!r && sql->session->status != -ERR_AMBIGUOUS) {
			/* reset error */
			sql->session->status = 0;
			sql->errstr[0] = 0;
			r = rel_subquery(sql, rel, ro, ek);

			/* get inner queries result value, ie
			   get last expression of r */
			if (r) {
				sql_rel *inner = r;

				/* subqueries may result in semijoins */
				if (is_semi(inner->op))
					inner = inner->r;
				rs = rel_lastexp(sql, inner);
				rel = r;
			}
		} else if (r) {
			rel_setsubquery(r);
			rs = rel_lastexp(sql, r);
			if (r->card > CARD_ATOM) {
				sql_subaggr *zero_or_one = sql_bind_aggr(sql->sa, sql->session->schema, "zero_or_one", exp_subtype(rs));

				rs = exp_aggr1(sql->sa, rs, zero_or_one, 0, 0, CARD_ATOM, 0);
			}
			rel = rel_crossproduct(sql->sa, rel, r, op_semi);
		}
	}
	if (!rs) 
		return NULL;
	return rel_compare_exp(sql, rel, ls, rs, compare_op, NULL, k.reduce);
}

static sql_rel *
rel_or(mvc *sql, sql_rel *l, sql_rel *r, int f)
{
	sql_rel *rel;

	(void)f;
	l = rel_project(sql->sa, l, rel_projections(sql, l, NULL, 1, 1));
	r = rel_project(sql->sa, r, rel_projections(sql, r, NULL, 1, 1));
	set_processed(l);
	set_processed(r);
	rel = rel_setop(sql->sa, l, r, op_union);
	rel->exps = rel_projections(sql, rel, NULL, 1, 1);
	rel = rel_distinct(rel);
	if (exps_card(l->exps) <= CARD_AGGR &&
	    exps_card(r->exps) <= CARD_AGGR)
	{
		rel->card = exps_card(l->exps);
		exps_fix_card( rel->exps, rel->card);
	}
	return rel;
}

sql_exp *
rel_logical_value_exp(mvc *sql, sql_rel **rel, symbol *sc, int f)
{
	exp_kind ek = {type_value, card_column, FALSE};

	if (!sc)
		return NULL;

	if (THRhighwater())
		return sql_error(sql, 10, "SELECT: too many nested operators");

	switch (sc->token) {
	case SQL_OR:
	case SQL_AND:
	{
		symbol *lo = sc->data.lval->h->data.sym;
		symbol *ro = sc->data.lval->h->next->data.sym;

		sql_exp *ls = rel_logical_value_exp(sql, rel, lo, f);
		sql_exp *rs = rel_logical_value_exp(sql, rel, ro, f);

		if (!ls || !rs)
			return NULL;
		if (sc->token == SQL_OR)
			return rel_binop_(sql, ls, rs, NULL, "or", card_value);
		else
			return rel_binop_(sql, ls, rs, NULL, "and", card_value);
	}
	case SQL_COMPARE:
	{
		symbol *lo = sc->data.lval->h->data.sym;
		symbol *ro = sc->data.lval->h->next->next->data.sym;
		char *compare_op = sc->data.lval->h->next->data.sval;

		/* currently we don't handle the (universal and existential)
		   quantifiers (all and any/some) */

		sql_exp *rs = NULL, *ls = rel_value_exp(sql, rel, lo, f, ek);

		if (!ls)
			return NULL;

		if (ro->token != SQL_SELECT) {
			rs = rel_value_exp(sql, rel, ro, f, ek);
			if (!rs)
				return NULL;
			if (rel_convert_types(sql, &ls, &rs, 1, type_equal) < 0)
				return NULL;
			return rel_binop_(sql, ls, rs, NULL, compare_op, card_value);
		} else {
			/* first try without current relation, too see if there
			are correlations with the outer relation */
			sql_rel *r = rel_subquery(sql, NULL, ro, ek);
	
			/* correlation, ie return new relation */
			if (!r && sql->session->status != -ERR_AMBIGUOUS) {
			

				sql_exp *e;

				/* reset error */
				sql->session->status = 0;
				sql->errstr[0] = 0;
				if (!r)
					r = rel_subquery(sql, *rel, ro, ek);

				/* get inner queries result value, ie
				   get last expression of r */
				if (r) {
					rs = rel_lastexp(sql, r);
					*rel = r;
					e = exp_compare(sql->sa, ls, rs, compare_str2type(compare_op));
					if (f != sql_sel)
						return e;
			
					/* For selection we need to convert back into Boolean */
					ls = rel_unop_(sql, ls, NULL, "isnull", card_value);
					rs = exp_atom_bool(sql->sa, 0);
					return rel_binop_(sql, ls, rs, NULL, "=", card_value);
				}
			} else if (r && f != sql_sel) {
				sql_rel *l = *rel;
				rel_setsubquery(r);
				rs = rel_lastexp(sql, r);
				if (r->card > CARD_ATOM) {
					sql_subaggr *zero_or_one = sql_bind_aggr(sql->sa, sql->session->schema, "zero_or_one", exp_subtype(rs));

					rs = exp_aggr1(sql->sa, rs, zero_or_one, 0, 0, CARD_ATOM, 0);
				}
				*rel = rel_crossproduct(sql->sa, l, r, op_join);
			} else if (r) {
				sql_rel *l = *rel;
				list *exps = rel_projections(sql, l, NULL, 0, 1);

				rel_project_add_exp(sql, l, ls);
				rel_setsubquery(r);
				rs = rel_lastexp(sql, r);
				if (r->card > CARD_ATOM) {
					sql_subaggr *zero_or_one = sql_bind_aggr(sql->sa, sql->session->schema, "zero_or_one", exp_subtype(rs));

					rs = exp_aggr1(sql->sa, rs, zero_or_one, 0, 0, CARD_ATOM, 0);
				}
				*rel = rel_crossproduct(sql->sa, l, r, op_join);
				*rel = rel_project(sql->sa, *rel, exps);
			}
			if (!rs) 
				return NULL;
			if (rel_convert_types(sql, &ls, &rs, 1, type_equal) < 0) 
				return NULL;
			return rel_binop_(sql, ls, rs, NULL, compare_op, card_value);
		}
	}
	/* Set Member ship */
	case SQL_IN:
	case SQL_NOT_IN:
	{
		dlist *dl = sc->data.lval;
		symbol *lo = dl->h->data.sym;
		dnode *n = dl->h->next;
		sql_exp *l = rel_value_exp(sql, rel, lo, f, ek), *r = NULL;
		sql_rel *left = NULL, *right = NULL, *p = NULL;
		int needproj = 0, vals_only = 1;
		list *vals = NULL;

		if (!l)
			return NULL;

		ek.card = card_set;

		if (!left)
			left = *rel;

		if (!left || (!left->l && f == sql_sel)) {
			needproj = (left != NULL);
			left = rel_project_exp(sql->sa, l);
		}

		if (n->type == type_list) {
			sql_subtype *st = exp_subtype(l);

			vals = new_exp_list(sql->sa);
			n = n->data.lval->h;
			for (; n; n = n->next) {
				symbol *sval = n->data.sym;
				/* without correlation first */
				sql_rel *z = NULL, *rl;

				r = rel_value_exp(sql, &z, sval, f, ek);
				if (!r || !(r=rel_check_type(sql, st, r, type_equal))) {
					rel_destroy(right);
					return NULL;
				}
				if (z) {
					vals_only = 0;
					rl = z;
				} else {
					list_append(vals, r);
					rl = rel_project_exp(sql->sa, r);
				}
				if (right) {
					rl = rel_setop(sql->sa, right, rl, op_union);
					rl->exps = rel_projections(sql, rl, NULL, 0, 1);
					set_processed(rl);
				}
				right = rl;
			}
			if (right->processed)
				right = rel_label(sql, right);
			right = rel_distinct(right);
		} else {
			return sql_error(sql, 02, "IN: missing inner query");
		}
		if (right) {
			sql_exp *e = NULL;
		
			if (vals_only) {
				node *n;

				rel_destroy(right);
				for(n=vals->h; n; n = n->next) {
					sql_exp *r = n->data, *ne;

					if (sc->token == SQL_NOT_IN)
						ne = rel_binop_(sql, l, r, NULL, "<>", card_value);
					else
						ne = rel_binop_(sql, l, r, NULL, "=", card_value);
					if (!e) {
						e = ne;
					} else if (sc->token == SQL_NOT_IN) {
						e = rel_binop_(sql, e, ne, NULL, "and", card_value);
					} else {
						e = rel_binop_(sql, e, ne, NULL, "or", card_value);
					}
				}
				return e;
			}
			r = rel_lastexp(sql, right);
			if (rel_convert_types(sql, &l, &r, 1, type_equal) < 0) 
				return NULL;
			left = rel_crossproduct(sql->sa, left, right, op_join);
			if (p) {
				rel_destroy(p->l);
				p->l = left;
			}
			left->op = op_left;
			e = exp_compare(sql->sa, l, r, cmp_equal );
			rel_join_add_exp(sql->sa, left, e);
			if (!p) {
				if (*rel && needproj)
					left = *rel = rel_project(sql->sa, left, NULL);
				else
					*rel = left;
			}
			if (sc->token == SQL_NOT_IN)
				e = rel_binop_(sql, l, r, NULL, "<>", card_value);
			else
				e = rel_binop_(sql, l, r, NULL, "=", card_value);
			return e;
		}
		return NULL;
	}
	case SQL_LIKE:
	case SQL_NOT_LIKE:
	{
		symbol *lo = sc->data.lval->h->data.sym;
		symbol *ro = sc->data.lval->h->next->data.sym;
		int insensitive = sc->data.lval->h->next->next->data.i_val;
		sql_subtype *st = sql_bind_localtype("str");
		sql_exp *le = rel_value_exp(sql, rel, lo, f, ek);
		sql_exp *re, *ee = NULL;

		if (!le)
			return NULL;

		if (!exp_subtype(le)) 
			return sql_error(sql, 02, "SELECT: parameter not allowed on "
					"left hand side of LIKE operator");

		lo = ro->data.lval->h->data.sym;
		/* like uses a single string pattern */
		ek.card = card_value;
		re = rel_value_exp(sql, rel, lo, f, ek);
		if (!re)
			return NULL;
		if (!exp_subtype(re)) {
			if (rel_set_type_param(sql, st, re) == -1) 
				return sql_error(sql, 02, "LIKE: wrong type, should be string");
		} else if ((re = rel_check_type(sql, st, re, type_equal)) == NULL) {
			return sql_error(sql, 02, "LIKE: wrong type, should be string");
		}
		/* Do we need to escape ? */
		if (dlist_length(ro->data.lval) == 2) {
			char *escape = ro->data.lval->h->next->data.sval;
			ee = exp_atom(sql->sa, atom_string(sql->sa, st, sa_strdup(sql->sa, escape)));
		}
		if (sc->token == SQL_LIKE) {
			char *like = insensitive ? "ilike" : "like";
			if (ee)
				return rel_nop_(sql, le, re, ee, NULL, NULL, like, card_value);
			return rel_binop_(sql, le, re, NULL, like, card_value);
		} else {
			char *like = insensitive ? "ilike" : "like";
			if (ee)
				le = rel_nop_(sql, le, re, ee, NULL, NULL, like, card_value);
			else
				le = rel_binop_(sql, le, re, NULL, like, card_value);
			return rel_unop_(sql, le, NULL, "not", card_value);
		}
	}
	case SQL_BETWEEN:
	case SQL_NOT_BETWEEN:
	{
		symbol *lo = sc->data.lval->h->data.sym;
		int symmetric = sc->data.lval->h->next->data.i_val;
		symbol *ro1 = sc->data.lval->h->next->next->data.sym;
		symbol *ro2 = sc->data.lval->h->next->next->next->data.sym;
		sql_exp *le = rel_value_exp(sql, rel, lo, f, ek);
		sql_exp *re1 = rel_value_exp(sql, rel, ro1, f, ek);
		sql_exp *re2 = rel_value_exp(sql, rel, ro2, f, ek);
		sql_exp *e1 = NULL, *e2 = NULL;

		assert(sc->data.lval->h->next->type == type_int);
		if (!le || !re1 || !re2) 
			return NULL;
		
		if (rel_convert_types(sql, &le, &re1, 1, type_equal) < 0 ||
				rel_convert_types(sql, &le, &re2, 1, type_equal) < 0)
			return NULL;

		if (!re1 || !re2) 
			return NULL;

		if (symmetric) {
			sql_exp *tmp = NULL;
			sql_subfunc *min = sql_bind_func(sql->sa, sql->session->schema, "sql_min", exp_subtype(re1), exp_subtype(re2));
			sql_subfunc *max = sql_bind_func(sql->sa, sql->session->schema, "sql_max", exp_subtype(re1), exp_subtype(re2));

			if (!min || !max) {
				return sql_error(sql, 02, "min or max operator on types %s %s missing", exp_subtype(re1)->type->sqlname, exp_subtype(re2)->type->sqlname);
			}
			tmp = exp_binop(sql->sa, re1, re2, min);
			re2 = exp_binop(sql->sa, re1, re2, max);
			re1 = tmp;
		}

		if (sc->token == SQL_NOT_BETWEEN) {
			e1 = rel_binop_(sql, le, re1, NULL, "<", card_value);
			e2 = rel_binop_(sql, le, re2, NULL, ">", card_value);
		} else {
			e1 = rel_binop_(sql, le, re1, NULL, ">=", card_value);
			e2 = rel_binop_(sql, le, re2, NULL, "<=", card_value);
		}
		if (!e1 || !e2)
			return NULL;
		if (sc->token == SQL_NOT_BETWEEN) {
			return rel_binop_(sql, e1, e2, NULL, "or", card_value);
		} else {
			return rel_binop_(sql, e1, e2, NULL, "and", card_value);
		}
	}
	case SQL_IS_NULL:
	case SQL_IS_NOT_NULL:
	/* is (NOT) NULL */
	{
		sql_exp *le = rel_value_exp(sql, rel, sc->data.sym, f, ek);

		if (!le)
			return NULL;
		le = rel_unop_(sql, le, NULL, "isnull", card_value);
		if (sc->token != SQL_IS_NULL)
			le = rel_unop_(sql, le, NULL, "not", card_value);
		return le;
	}
	case SQL_NOT: {
		sql_exp *le = rel_logical_value_exp(sql, rel, sc->data.sym, f);

		if (!le)
			return le;
		return rel_unop_(sql, le, NULL, "not", card_value);
	}
	case SQL_ATOM: {
		/* TRUE or FALSE */
		AtomNode *an = (AtomNode *) sc;

		if (!an || !an->a) {
			assert(0);
			return exp_atom(sql->sa, atom_general(sql->sa, sql_bind_localtype("str"), NULL));
		} else {
			return exp_atom(sql->sa, atom_dup(sql->sa, an->a));
		}
	}
	case SQL_COLUMN:
		return rel_column_ref(sql, rel, sc, f);
	case SQL_UNION:
	case SQL_EXCEPT:
	case SQL_INTERSECT: {
		*rel = rel_setquery(sql, *rel, sc);
		if (*rel)
			return rel_lastexp(sql, *rel);
		return NULL;
	}
	default: {
		sql_exp *re, *le = rel_value_exp(sql, rel, sc, f, ek);

		if (!le)
			return NULL;
		re = exp_atom_bool(sql->sa, 1);
		if (rel_convert_types(sql, &le, &re, 1, type_equal) < 0) 
			return NULL;
		return rel_binop_(sql, le, re, NULL, "=", 0);
	}
	}
}

static sql_rel *
rel_add_identity(mvc *sql, sql_rel *rel, sql_exp **exp)
{
	list *exps = rel_projections(sql, rel, NULL, 1, 1);
	sql_exp *e;

	if (list_length(exps) == 0) {
		*exp = NULL;
		return rel;
	}
	rel = rel_project(sql->sa, rel, rel_projections(sql, rel, NULL, 1, 1));
	e = rel_unop_(sql, rel->exps->h->data, NULL, "identity", card_value);
	set_intern(e);
	rel_project_add_exp(sql, rel, e);
	*exp = exp_label(sql->sa, e, ++sql->label);
	return rel;
}

sql_rel *
rel_logical_exp(mvc *sql, sql_rel *rel, symbol *sc, int f)
{
	exp_kind ek = {type_value, card_column, TRUE};

	if (!sc)
		return NULL;

	if (THRhighwater())
		return sql_error(sql, 10, "SELECT: too many nested operators");

	switch (sc->token) {
	case SQL_OR:
	{
		symbol *lo = sc->data.lval->h->data.sym;
		symbol *ro = sc->data.lval->h->next->data.sym;

		sql_rel *lr, *rr;

		if (!rel)
			return NULL;
		lr = rel;
		rr = rel_dup(lr);

		lr = rel_logical_exp(sql, lr, lo, f);
		rr = rel_logical_exp(sql, rr, ro, f);

		if (!lr || !rr)
			return NULL;
		return rel_or(sql, lr, rr, f);
	}
	case SQL_AND:
	{
		symbol *lo = sc->data.lval->h->data.sym;
		symbol *ro = sc->data.lval->h->next->data.sym;

		rel = rel_logical_exp(sql, rel, lo, f);
		if (!rel)
			return NULL;
		return rel_logical_exp(sql, rel, ro, f);
	}
	case SQL_COMPARE:
	{
		symbol *lo = sc->data.lval->h->data.sym;
		symbol *ro = sc->data.lval->h->next->next->data.sym;
		char *compare_op = sc->data.lval->h->next->data.sval;
		/* currently we don't handle the (universal and existential)
		   quantifiers (all and any/some) */
		return rel_compare(sql, rel, lo, ro, compare_op, f, ek);
	}
		break;
	/* Set Member ship */
	case SQL_IN:
	case SQL_NOT_IN:
	{
		dlist *dl = sc->data.lval;
		symbol *lo = dl->h->data.sym;
		dnode *n = dl->h->next;
		sql_rel *left = NULL, *right = NULL, *outer = rel;
		sql_exp *l = rel_value_exp(sql, &left, lo, f, ek), *e, *r = NULL;
		list *vals = NULL;
		int correlated = 0, vals_only = 1;
		int l_is_value = 1, r_is_rel = 0;

		if (!l && sql->session->status != -ERR_AMBIGUOUS) {
			l_is_value = 0;
			/* reset error */
			left = rel;
			sql->session->status = 0;
			sql->errstr[0] = 0;
			l = rel_value_exp(sql, &left, lo, f, ek);
		} else if (!left) {
			left = rel_project_exp(sql->sa, exp_label(sql->sa, l, ++sql->label));
		}
		if (!l)
			return NULL;

		ek.card = card_set;

		/* first remove the NULLs */
		if (sc->token == SQL_NOT_IN &&
		    l->card != CARD_ATOM && has_nil(l)) {
			e = rel_unop_(sql, l, NULL, "isnull", card_value);
			e = exp_compare(sql->sa, e, exp_atom_bool(sql->sa, 0), cmp_equal);
			if (!is_select(rel->op) && !rel_is_ref(rel))
				left = rel = rel_select(sql->sa, rel, e);
			else
				rel_select_add_exp(rel, e);
		}

		/* list of values or subqueries */
		if (n->type == type_list) {
			sql_subtype *st = exp_subtype(l);

			vals = new_exp_list(sql->sa);
			n = n->data.lval->h;
			for (; n; n = n->next) {
				symbol *sval = n->data.sym;
				/* without correlation first */
				sql_rel *z = NULL;
				sql_rel *rl;

				r = rel_value_exp(sql, &z, sval, f, ek);
				if (z)
					r_is_rel = 1;
				if (!r && sql->session->status != -ERR_AMBIGUOUS) {
					/* reset error */
					sql->session->status = 0;
					sql->errstr[0] = 0;

					/* TODO remove null checking (not needed in correlated case because of the semi/anti join) ! */
					rel = left = rel_dup(left);
					r = rel_value_exp(sql, &rel, sval, f, ek);
					if (r && !is_project(rel->op)) {
						rel = rel_project(sql->sa, rel, NULL);
						rel_project_add_exp(sql, rel, r);
					}
					z = rel;
					correlated = 1;
				}
				if (!r || !(r=rel_check_type(sql, st, r, type_equal))) {
					rel_destroy(right);
					return NULL;
				}
				if (z) {
					vals_only = 0;
					rl = z;
				} else {
					list_append(vals, r);
					rl = rel_project_exp(sql->sa, exp_label(sql->sa, r, ++sql->label));
				}
				if (right) {
					rl = rel_setop(sql->sa, right, rl, op_union);
					rl->exps = rel_projections(sql, rl, NULL, 0, 1);
					set_processed(rl);
				}
				right = rl;
			}
			if (!correlated) {
				if (right->processed)
					right = rel_label(sql, right);
				right = rel_distinct(right);
			}
		} else {
			return sql_error(sql, 02, "IN: missing inner query");
		}

		if (right) {
			if (vals_only && !correlated) {
				list *nvals = list_new(sql->sa);
				sql_exp *e = NULL;
				node *n;

				rel_destroy(right);
				for(n=vals->h; n; n = n->next) {
					sql_exp *r = n->data;

					r = rel_check_type(sql, exp_subtype(l), r, type_equal);
					if (!r)
						return NULL;
					append(nvals, r);
				}

				if (sc->token == SQL_NOT_IN) {
					e = exp_in(sql->sa, l, nvals, cmp_notin);
				} else {
					e = exp_in(sql->sa, l, nvals, cmp_in);
				}
				rel = rel_select(sql->sa, rel, e);
				return rel;
			}
			r = rel_lastexp(sql, right);
			rel = rel_crossproduct(sql->sa, left, right, op_join);
			if (rel_convert_types(sql, &l, &r, 1, type_equal) < 0) 
				return NULL;
			e = exp_compare(sql->sa, l, r, cmp_equal );
			rel_join_add_exp(sql->sa, rel, e);
			if (correlated || l_is_value || r_is_rel) {
				rel->op = (sc->token == SQL_IN)?op_semi:op_anti;
			} else if (sc->token == SQL_NOT_IN) {
				rel->op = op_left;
				e = rel_unop_(sql, r, NULL, "isnull", card_value);
				r = exp_atom_bool(sql->sa, 1);
				e = exp_compare(sql->sa,  e, r, cmp_equal);
				rel = rel_select(sql->sa, rel, e);
			}
			if (l_is_value)
				rel = rel_crossproduct(sql->sa, outer, rel, op_join);
			rel = rel_project(sql->sa, rel, rel_projections(sql, outer, NULL, 1, 1));
			set_processed(rel);
			return rel;
		}
		return right;
	}
	case SQL_EXISTS:
	case SQL_NOT_EXISTS:
	{
		symbol *lo = sc->data.sym;
		sql_rel *r;
		sql_exp *e = NULL;
		list *exps = NULL;

		ek.card = card_set;
		r = rel_subquery(sql, NULL, lo, ek);
		if (!r && rel && sql->session->status != -ERR_AMBIGUOUS) { /* correlation */
			sql_exp *le, *re;
			/* reset error */
			sql->session->status = 0;
			sql->errstr[0] = '\0';

			/* We don't have a natural anti/semi join but versions
			   which require (join) expression. So we should
			   generate row id's which we use in the anti/semi
			   join expression.
			 */
			exps = rel_projections(sql, rel, NULL, 1, 1);
			rel = rel_add_identity(sql, rel, &e);
			set_processed(rel);

			r = rel_subquery(sql, rel_dup(rel), lo, ek);
			if (!r)
				return NULL;

			r = rel_label(sql, r);

			/* look up the identity columns and label these */
			le = rel_bind_column(sql, rel, e->name, f);
			re = rel_bind_column(sql, r, e->name, f);

			if (!le || !re)
				return NULL;
			e = exp_compare(sql->sa, le, re, cmp_equal);
		}
		if (!r || !rel)
			return NULL;
		r = rel = rel_crossproduct(sql->sa, rel, r, op_join);
		if (sc->token == SQL_EXISTS) {
			r->op = op_semi;
		} else {	
			r->op = op_anti;
		}
		if (e)
			rel->exps = append(new_exp_list(sql->sa), e);
		if (exps)
			rel = rel_project(sql->sa, rel, exps);
		return rel;
	}
	case SQL_LIKE:
	case SQL_NOT_LIKE:
	{
		symbol *lo = sc->data.lval->h->data.sym;
		symbol *ro = sc->data.lval->h->next->data.sym;
		int insensitive = sc->data.lval->h->next->next->data.i_val;
		sql_subtype *st = sql_bind_localtype("str");
		sql_exp *le = rel_value_exp(sql, &rel, lo, f, ek);
		sql_exp *re, *ee = NULL;

		if (!le)
			return NULL;

		if (!exp_subtype(le)) 
			return sql_error(sql, 02, "SELECT: parameter not allowed on "
					"left hand side of LIKE operator");

		/* Do we need to escape ? */
		if (dlist_length(ro->data.lval) == 2) {
			char *escape = ro->data.lval->h->next->data.sval;
			ee = exp_atom(sql->sa, atom_string(sql->sa, st, sa_strdup(sql->sa, escape)));
		}
		ro = ro->data.lval->h->data.sym;
		re = rel_value_exp(sql, &rel, ro, f, ek);
		if (!re)
			return NULL;
		if (!exp_subtype(re)) {
			if (rel_set_type_param(sql, st, re) == -1) 
				return sql_error(sql, 02, "LIKE: wrong type, should be string");
		} else if ((re = rel_check_type(sql, st, re, type_equal)) == NULL) {
			return sql_error(sql, 02, "LIKE: wrong type, should be string");
		}
		if ((le = rel_check_type(sql, st, le, type_equal)) == NULL) 
			return sql_error(sql, 02, "LIKE: wrong type, should be string");
		if (sc->token == SQL_LIKE)
			return rel_compare_exp(sql, rel, le, re,
					(insensitive ? "ilike" : "like"), ee, TRUE);
		return rel_compare_exp(sql, rel, le, re,
				(insensitive ? "not_ilike" : "not_like"), ee, TRUE);
	}
	case SQL_BETWEEN:
	case SQL_NOT_BETWEEN:
	{
		symbol *lo = sc->data.lval->h->data.sym;
		int symmetric = sc->data.lval->h->next->data.i_val;
		symbol *ro1 = sc->data.lval->h->next->next->data.sym;
		symbol *ro2 = sc->data.lval->h->next->next->next->data.sym;
		sql_exp *le = rel_value_exp(sql, &rel, lo, f, ek);
		sql_exp *re1 = rel_value_exp(sql, &rel, ro1, f, ek);
		sql_exp *re2 = rel_value_exp(sql, &rel, ro2, f, ek);

		assert(sc->data.lval->h->next->type == type_int);
		if (!le || !re1 || !re2) 
			return NULL;

		if (rel_convert_types(sql, &le, &re1, 1, type_equal) < 0 ||
		    rel_convert_types(sql, &le, &re2, 1, type_equal) < 0)
			return NULL;

		if (!re1 || !re2) 
			return NULL;

		if (symmetric) {
			sql_exp *tmp = NULL;
			sql_subfunc *min = sql_bind_func(sql->sa, sql->session->schema, "sql_min", exp_subtype(re1), exp_subtype(re2));
			sql_subfunc *max = sql_bind_func(sql->sa, sql->session->schema, "sql_max", exp_subtype(re1), exp_subtype(re2));

			if (!min || !max) {
				return sql_error(sql, 02, "min or max operator on types %s %s missing", exp_subtype(re1)->type->sqlname, exp_subtype(re2)->type->sqlname);
			}
			tmp = exp_binop(sql->sa, re1, re2, min);
			re2 = exp_binop(sql->sa, re1, re2, max);
			re1 = tmp;
		}

		if (le->card == CARD_ATOM) {
			sql_exp *e1, *e2;
			if (sc->token == SQL_NOT_BETWEEN) {
				e1 = rel_binop_(sql, le, re1, NULL, "<", card_value);
				e2 = rel_binop_(sql, le, re2, NULL, ">", card_value);
			} else {
				e1 = rel_binop_(sql, le, re1, NULL, ">=", card_value);
				e2 = rel_binop_(sql, le, re2, NULL, "<=", card_value);
			}
			if (!e1 || !e2)
				return NULL;
			if (sc->token == SQL_NOT_BETWEEN) {
				e1 = rel_binop_(sql, e1, e2, NULL, "or", card_value);
			} else {
				e1 = rel_binop_(sql, e1, e2, NULL, "and", card_value);
			}
			e2 = exp_atom_bool(sql->sa, 1);
			rel = rel_select(sql->sa, rel, exp_compare(sql->sa,  e1, e2, cmp_equal));
		} else if (sc->token == SQL_NOT_BETWEEN) {
			rel = rel_compare_exp_(sql, rel, le, re1, re2, 3, 1);
		} else {
			rel = rel_compare_exp_(sql, rel, le, re1, re2, 3, 0);
		}
		return rel;
	}
	case SQL_IS_NULL:
	case SQL_IS_NOT_NULL:
	/* is (NOT) NULL */
	{
		sql_exp *re, *le = rel_value_exp(sql, &rel, sc->data.sym, f, ek);

		if (!le)
			return NULL;
		le = rel_unop_(sql, le, NULL, "isnull", card_value);
		if (sc->token == SQL_IS_NULL)
			re = exp_atom_bool(sql->sa, 1);
		else
			re = exp_atom_bool(sql->sa, 0);
		le = exp_compare(sql->sa, le, re, cmp_equal);
		return rel_select(sql->sa, rel, le);
	}
	case SQL_NOT: {
		sql_exp *re, *le = rel_value_exp(sql, &rel, sc->data.sym, f, ek);

		if (!le)
			return NULL;
		le = rel_unop_(sql, le, NULL, "not", card_value);
		re = exp_atom_bool(sql->sa, 1);
		le = exp_compare(sql->sa, le, re, cmp_equal);
		return rel_select(sql->sa, rel, le);
	}
	case SQL_ATOM: {
		/* TRUE or FALSE */
		AtomNode *an = (AtomNode *) sc;
		sql_exp *e = exp_atom(sql->sa, atom_dup(sql->sa, an->a));
		return rel_select(sql->sa, rel, e);
	}
	case SQL_COLUMN: {
		sql_rel *or = rel;
		sql_exp *e = rel_column_ref(sql, &rel, sc, f);

		if (e) {
			sql_subtype bt;

			sql_find_subtype(&bt, "boolean", 0, 0);
			e = rel_check_type(sql, &bt, e, type_equal);
		}
		if (!e || or != rel)
			return NULL;
		return rel_select(sql->sa, rel, e);
	}
	case SQL_UNION:
	case SQL_EXCEPT:
	case SQL_INTERSECT:
		return rel_setquery(sql, rel, sc);
	default: {
		sql_exp *re, *le = rel_value_exp(sql, &rel, sc, f, ek);

		if (!le)
			return NULL;
		re = exp_atom_bool(sql->sa, 1);
		if (rel_convert_types(sql, &le, &re, 1, type_equal) < 0) 
			return NULL;
		le = exp_compare(sql->sa, le, re, cmp_equal);
		return rel_select(sql->sa, rel, le);
	}
	}
	/* never reached, as all switch cases have a `return` */
}

static sql_exp *
rel_op(mvc *sql, symbol *se, exp_kind ek )
{
	dnode *l = se->data.lval->h;
	char *fname = qname_fname(l->data.lval);
	char *sname = qname_schema(l->data.lval);
	sql_schema *s = sql->session->schema;

	if (sname)
		s = mvc_bind_schema(sql, sname);
	return rel_op_(sql, s, fname, ek);
}

sql_exp *
rel_unop_(mvc *sql, sql_exp *e, sql_schema *s, char *fname, int card)
{
	sql_subfunc *f = NULL;
	sql_subtype *t = NULL;

	if (!s)
		s = sql->session->schema;
	t = exp_subtype(e);
	f = sql_bind_func(sql->sa, s, fname, t, NULL);
	/* try to find the function without a type, and convert
	 * the value to the type needed by this function!
	 */
	if (!f &&
	   (f = sql_find_func(sql->sa, s, fname, 1)) != NULL &&
	   ((card == card_relation && f->res.comp_type) || 
	    (card == card_none && !f->res.type) || 
	    (card != card_none && card != card_relation && f->res.type && !f->res.comp_type))) {
		sql_arg *a = f->func->ops->h->data;

		e = rel_check_type(sql, &a->type, e, type_equal);
		if (!e) 
			f = NULL;
	}
	if (f &&
	   ((card == card_relation && f->res.comp_type) || 
	    (card == card_none && !f->res.type) || 
	    (card != card_none && card != card_relation && f->res.type && !f->res.comp_type))) {
		if (f->func->res.scale == INOUT) {
			f->res.digits = t->digits;
			f->res.scale = t->scale;
		}
		return exp_unop(sql->sa, e, f);
	} else if (e) {
		char *type = exp_subtype(e)->type->sqlname;

		return sql_error(sql, 02, "SELECT: no such unary operator '%s(%s)'", fname, type);
	}
	return NULL;
}

static sql_exp * _rel_aggr(mvc *sql, sql_rel **rel, int distinct, char *aggrstr, symbol *sym, int f);

static sql_exp *
rel_unop(mvc *sql, sql_rel **rel, symbol *se, int fs, exp_kind ek)
{
	dnode *l = se->data.lval->h;
	char *fname = qname_fname(l->data.lval);
	char *sname = qname_schema(l->data.lval);
	sql_schema *s = sql->session->schema;
	exp_kind iek = {type_value, card_column, FALSE};
	sql_exp *e = rel_value_exp(sql, rel, l->next->data.sym, fs, iek);
	sql_subfunc *f = NULL;
	sql_subtype *t = NULL;

	if (!e)
		return NULL;
	if (sname)
		s = mvc_bind_schema(sql, sname);
	if (!s)
		s = sql->session->schema;
	t = exp_subtype(e);
	f = sql_bind_func(sql->sa, s, fname, t, NULL);
	if (f && f->func->aggr)
		return _rel_aggr(sql, rel, 0, fname, l->next->data.sym, fs);
	return rel_unop_(sql, e, s, fname, ek.card);
}


sql_exp *
rel_binop_(mvc *sql, sql_exp *l, sql_exp *r, sql_schema *s,
		char *fname, int card)
{
	sql_exp *res = NULL;
	sql_subtype *t1, *t2;
	sql_subfunc *f = NULL;

	t1 = exp_subtype(l);
	t2 = exp_subtype(r);

	if (!s)
		s = sql->session->schema;

	/* handle param's early */
	if ((!t1 || !t2) && rel_convert_types(sql, &l, &r, 1/*fix scale*/, type_equal) >= 0) {
		t1 = exp_subtype(l);
		t2 = exp_subtype(r);
	}
	if (!t1 || !t2)
		return sql_error(sql, 01, "Cannot have a parameter (?) on both sides of an expression");
		
	f = sql_bind_func(sql->sa, s, fname, t1, t2);
	if (!f && is_commutative(fname)) {
		f = sql_bind_func(sql->sa, s, fname, t2, t1);
		if (f && (card == card_relation || !f->res.comp_type)) {
			sql_subtype *tmp = t1;
			t1 = t2;	
			t2 = tmp;
			res = l;		
			l = r;
			r = res;
		}
	}
	if (f && 
	   ((card == card_relation && f->res.comp_type) || 
	    (card == card_none && !f->res.type) || 
	    (card != card_none && card != card_relation && f->res.type && !f->res.comp_type))) {
		if (f->func->fix_scale == SCALE_FIX) {
			l = exp_fix_scale(sql, t2, l, 0, 0);
			r = exp_fix_scale(sql, t1, r, 0, 0);
		} else if (f->func->fix_scale == SCALE_DIV) {
			l = exp_scale_algebra(sql, f, l, r);
		} else if (f->func->fix_scale == SCALE_MUL) {
			l = exp_sum_scales(sql, f, l, r);
		} else if (f->func->fix_scale == DIGITS_ADD) {
			f->res.digits = t1->digits + t2->digits;
		}
		return exp_binop(sql->sa, l, r, f);
	} else {
		sql_exp *ol = l;
		sql_exp *or = r;

		if (!EC_NUMBER(t1->type->eclass) &&
		   (f = sql_bind_member(sql->sa, s, fname, t1, 2)) != NULL &&
	   	   ((card == card_relation && f->res.comp_type) || 
	     	    (card == card_none && !f->res.type) || 
	    	    (card != card_none && card != card_relation && f->res.type && !f->res.comp_type))) {
			/* try finding function based on first argument */
			node *m = f->func->ops->h;
			sql_arg *a = m->data;

			l = rel_check_type(sql, &a->type, l, type_equal);
			a = m->next->data;
			r = rel_check_type(sql, &a->type, r, type_equal);
			if (l && r) 
				return exp_binop(sql->sa, l, r, f);
		}
		/* reset error */
		sql->session->status = 0;
		sql->errstr[0] = '\0';

		l = ol;
		r = or;
		/* try finding function based on both arguments */
		if (rel_convert_types(sql, &l, &r, 1/*fix scale*/, type_equal) >= 0){
			/* try operators */
			t1 = exp_subtype(l);
			t2 = exp_subtype(r);
			f = sql_bind_func(sql->sa, s, fname, t1, t2);
			if (f && 
	   	   	   ((card == card_relation && f->res.comp_type) || 
	     	    	    (card == card_none && !f->res.type) || 
	    	    	    (card != card_none && card != card_relation && f->res.type && !f->res.comp_type))) {
				if (f->func->fix_scale == SCALE_FIX) {
					l = exp_fix_scale(sql, t2, l, 0, 0);
					r = exp_fix_scale(sql, t1, r, 0, 0);
				} else if (f->func->fix_scale == SCALE_DIV) {
					l = exp_scale_algebra(sql, f, l, r);
				} else if (f->func->fix_scale == SCALE_MUL) {
					l = exp_sum_scales(sql, f, l, r);
				} else if (f->func->fix_scale == DIGITS_ADD) {
					f->res.digits = t1->digits + t2->digits;
				}
				return exp_binop(sql->sa, l, r, f);
			}
		}
		/* reset error */
		sql->session->status = 0;
		sql->errstr[0] = '\0';

		l = ol;
		r = or;
		t1 = exp_subtype(l);
		(void) exp_subtype(r);

		if ((f = sql_bind_member(sql->sa, s, fname, t1, 2)) != NULL &&
	   	   ((card == card_relation && f->res.comp_type) || 
	     	   (card == card_none && !f->res.type) || 
	    	   (card != card_none && card != card_relation && f->res.type && !f->res.comp_type))) {
			/* try finding function based on first argument */
			node *m = f->func->ops->h;
			sql_arg *a = m->data;

			l = rel_check_type(sql, &a->type, l, type_equal);
			a = m->next->data;
			r = rel_check_type(sql, &a->type, r, type_equal);
			if (l && r) 
				return exp_binop(sql->sa, l, r, f);
		}
		/* reset error */
		sql->session->status = 0;
		sql->errstr[0] = '\0';

		l = ol;
		r = or;
		/* everything failed, fall back to bind on function name only */
		if ((f = sql_find_func(sql->sa, s, fname, 2)) != NULL &&
	   	   ((card == card_relation && f->res.comp_type) || 
	     	   (card == card_none && !f->res.type) || 
	    	   (card != card_none && card != card_relation && f->res.type && !f->res.comp_type))) {
			node *m = f->func->ops->h;
			sql_arg *a = m->data;

			l = rel_check_type(sql, &a->type, l, type_equal);
			a = m->next->data;
			r = rel_check_type(sql, &a->type, r, type_equal);
			if (l && r)
				return exp_binop(sql->sa, l, r, f);
		}
	}
	if (r && l)
		res = sql_error(sql, 02, "SELECT: no such binary operator '%s(%s,%s)'",
				fname,
				exp_subtype(l)->type->sqlname,
				exp_subtype(r)->type->sqlname);
	return res;
}

#define SQLMAXDEPTH ((THREAD_STACK_SIZE/4096))

static sql_exp *
rel_binop(mvc *sql, sql_rel **rel, symbol *se, int f, exp_kind ek)
{
	dnode *dl = se->data.lval->h;
	sql_exp *l, *r;
	char *fname = qname_fname(dl->data.lval);
	char *sname = qname_schema(dl->data.lval);
	sql_schema *s = sql->session->schema;
	exp_kind iek = {type_value, card_column, FALSE};

	l = rel_value_exp(sql, rel, dl->next->data.sym, f, iek);
	r = rel_value_exp(sql, rel, dl->next->next->data.sym, f, iek);

	if (!l || !r) 
		return NULL;

	if (sname)
		s = mvc_bind_schema(sql, sname);
	return rel_binop_(sql, l, r, s, fname, ek.card);
}

sql_exp *
rel_nop_(mvc *sql, sql_exp *a1, sql_exp *a2, sql_exp *a3, sql_exp *a4, sql_schema *s, char *fname, int card)
{
	list *tl = list_create(NULL);
	sql_subfunc *f = NULL;

	append(tl, exp_subtype(a1));
	append(tl, exp_subtype(a2));
	append(tl, exp_subtype(a3));
	if (a4)
		append(tl, exp_subtype(a4));

	if (!s)
		s = sql->session->schema;
	f = sql_bind_func_(sql->sa, s, fname, tl);
	list_destroy(tl);
	if (!f || (card == card_relation || f->res.comp_type))
		return sql_error(sql, 02, "SELECT: no such operator '%s'", fname);
	if (!a4)
		return exp_op3(sql->sa, a1,a2,a3,f);
	return exp_op4(sql->sa, a1,a2,a3,a4,f);
}

static sql_exp *
rel_nop(mvc *sql, sql_rel **rel, symbol *se, int fs, exp_kind ek)
{
	int nr_args = 0;
	dnode *l = se->data.lval->h;
	dnode *ops = l->next->data.lval->h;
	list *exps = new_exp_list(sql->sa);
	list *tl = list_create(NULL);
	sql_subfunc *f = NULL;
	sql_subtype *obj_type = NULL;
	char *fname = qname_fname(l->data.lval);
	char *sname = qname_schema(l->data.lval);
	sql_schema *s = sql->session->schema;
	exp_kind iek = {type_value, card_column, FALSE};
	int table_func = (ek.card == card_relation);

	for (; ops; ops = ops->next, nr_args++) {
		sql_exp *e = rel_value_exp(sql, rel, ops->data.sym, fs, iek);
		sql_subtype *tpe;

		if (!e) 
			return NULL;
		append(exps, e);
		tpe = exp_subtype(e);
		if (!nr_args)
			obj_type = tpe;
		append(tl, tpe);
	}
	if (sname)
		s = mvc_bind_schema(sql, sname);
	f = sql_bind_func_(sql->sa, s, fname, tl);
	list_destroy(tl);
	if (f) {
		return exp_op(sql->sa, exps, f);
	} else if (((f = sql_bind_member(sql->sa, s, fname, obj_type, nr_args)) != NULL ||
		   (f = sql_find_func(sql->sa, s, fname, nr_args)) != NULL) &&
		   (table_func || !f->res.comp_type)) {
		node *n, *m;
		list *nexps = new_exp_list(sql->sa);

		for (n = exps->h, m = f->func->ops->h; n && m;
				  n = n->next, m = m->next) {
			sql_arg *a = m->data;
			sql_exp *e = n->data;

			if (a->type.type->eclass == EC_ANY) {
				sql_subtype *st = &e->tpe;
				sql_init_subtype(&a->type, st->type, st->digits, st->scale);
			}
			e = rel_check_type(sql, &a->type, e, type_equal);
			if (!e) {
				nexps = NULL;
				break;
			}
			append(nexps, e);
		}
		if (nexps) 
			return exp_op(sql->sa, nexps, f);
	}
	return sql_error(sql, 02, "SELECT: no such operator '%s'", fname);
}

static sql_exp *
flatten_exps( mvc *sql, list *exps )
{
	node *n;
	sql_exp *e = NULL;

	for (n=exps->h; n; n=n->next) {
		sql_exp *c = n->data, *ne = NULL;
	
		switch(c->type)  {
		case e_cmp:
			if (!c->f) {
				char *cmp = compare_func((comp_type)c->flag);
				ne = rel_binop_(sql, c->l, c->r, NULL, cmp, card_value);
				if (!e)
					e = ne;
				else
					e = rel_binop_(sql, e, ne, NULL, "sql_and", card_value);
			}
			break;
		default:
			assert(0);
		}
	}
	return e;
}

static sql_exp *
_rel_aggr(mvc *sql, sql_rel **rel, int distinct, char *aggrstr, symbol *sym, int f)
{
	sql_subaggr *a = NULL;
	int no_nil = 0;
	sql_exp *e = NULL, *exp = NULL;
	sql_rel *groupby = *rel;

	if (!groupby) {
		char *uaggrstr = malloc(strlen(aggrstr) + 1);
		e = sql_error(sql, 02, "%s: missing group by",
				toUpperCopy(uaggrstr, aggrstr));
		free(uaggrstr);
		return e;
	}

	if (f == sql_having && is_select(groupby->op))
		groupby = groupby->l;

	if (groupby->l && groupby->op == op_project) {
		sql_rel *r = groupby->l;	
		if (r->op == op_groupby) {
			groupby = r;
		} else if (r->op == op_select && r->l) {
			/* a having after a groupby */
			r = r->l;
			if (r->op == op_groupby)
				groupby = r;
		}
	}

	if (groupby->op != op_groupby)		/* implicit groupby */
		*rel = rel_project2groupby(sql, groupby);
	if (!*rel) {
		rel_destroy(groupby);
		return NULL;
	}

	if (f == sql_where) {
		char *uaggrstr = malloc(strlen(aggrstr) + 1);
		e = sql_error(sql, 02, "%s: not allowed in WHERE clause",
				toUpperCopy(uaggrstr, aggrstr));
		free(uaggrstr);
		return e;
	}
	
	if (!sym) {	/* count(*) case */

		if (strcmp(aggrstr, "count") != 0) {
			char *uaggrstr = malloc(strlen(aggrstr) + 1);
			e = sql_error(sql, 02, "%s: unable to perform '%s(*)'",
					toUpperCopy(uaggrstr, aggrstr), aggrstr);
			free(uaggrstr);
			return e;
		}
		a = sql_bind_aggr(sql->sa, sql->session->schema, aggrstr, NULL);
		/* add aggr expression to the groupby, and return a
			column expression */

		/* for correlated selections, we need to count on the
		   join expression */
		if (groupby->r && exps_intern(groupby->r)) {
			sql_rel *i = groupby->l;

			if (i->exps) {
				e = flatten_exps(sql, i->exps);
				e = exp_aggr1(sql->sa, e, a, distinct, 1, groupby->card, 0);
				return e;
			}
		}
		e = exp_aggr(sql->sa, NULL, a, distinct, 0, groupby->card, 0);
		if (*rel == groupby && f == sql_sel) /* selection */
			return e;
		return rel_groupby_add_aggr(sql, groupby, e);
	} else {
		exp_kind ek = {type_value, card_column, FALSE};

		/* use cnt as nils shouldn't be counted */
		sql_rel *gr = groupby->l;

		no_nil = 1;
		e = rel_value_exp(sql, &gr, sym, f, ek);
		if (gr && e && is_project(gr->op) && !is_set(gr->op) && e->type != e_column) {
			rel_project_add_exp(sql, gr, e);
			e = exp_alias_or_copy(sql, exp_relname(e), exp_name(e), gr->l, e, 0);
		}
		groupby->l = gr;
	}

	if (!e)
		return NULL;
	a = sql_bind_aggr(sql->sa, sql->session->schema, aggrstr, exp_subtype(e));
	if (!a) { /* find aggr + convert */

		a = sql_find_aggr(sql->sa, sql->session->schema, aggrstr);
		if (a) {
			sql_arg *arg = a->aggr->ops->h->data;

			e = rel_check_type(sql, &arg->type, e, type_equal);
			if (!e)
				a = NULL;
		}
	}
	if (a) {
		/* type may have changed, ie. need to fix_scale */
		sql_subtype *t = exp_subtype(e);

		e = exp_aggr1(sql->sa, e, a, distinct, no_nil, groupby->card, has_nil(e));
		exp = e;
		if (*rel != groupby || f != sql_sel) /* selection */
			exp = rel_groupby_add_aggr(sql, groupby, exp);
		return exp_fix_scale(sql, t, exp, 1,
					(t->type->scale == SCALE_FIX));
	} else {
		char *type = "unknown";
		char *uaggrstr = malloc(strlen(aggrstr) + 1);

		if (e) 
			type = exp_subtype(e)->type->sqlname;

		e = sql_error(sql, 02, "%s: no such operator '%s(%s)'",
				toUpperCopy(uaggrstr, aggrstr), aggrstr, type);

		free(uaggrstr);
		return e;
	}
}

static sql_exp *
rel_aggr(mvc *sql, sql_rel **rel, symbol *se, int f)
{
	dlist *l = se->data.lval;
	int distinct = l->h->next->data.i_val;
	char *aggrstr = l->h->data.sval;

	assert(l->h->next->type == type_int);
	return _rel_aggr( sql, rel, distinct, aggrstr, l->h->next->next->data.sym, f);
}

static sql_exp *
rel_case(mvc *sql, sql_rel **rel, int token, symbol *opt_cond, dlist *when_search_list, symbol *opt_else, int f)
{
	sql_subtype *tpe = NULL;
	list *conds = new_exp_list(sql->sa);
	list *results = new_exp_list(sql->sa);
	dnode *dn = when_search_list->h;
	sql_subtype *restype = NULL, rtype;
	sql_exp *res = NULL, *else_exp = NULL;
	node *n, *m;
	exp_kind ek = {type_value, card_column, FALSE};

	if (dn) {
		sql_exp *cond = NULL, *result = NULL;

		/* NULLIF(e1,e2) == CASE WHEN e1=e2 THEN NULL ELSE e1 END */
		if (token == SQL_NULLIF) {
			sql_exp *e1, *e2;

			e1 = rel_value_exp(sql, rel, dn->data.sym, f, ek);
			e2 = rel_value_exp(sql, rel, dn->next->data.sym, f, ek);
			if (e1 && e2) {
				cond = rel_binop_(sql, e1, e2, NULL, "=", card_value);
				result = exp_atom(sql->sa, atom_general(sql->sa, exp_subtype(e1), NULL));
				else_exp = e1;	/* ELSE case */
			}
			/* COALESCE(e1,e2) == CASE WHEN e1
			   IS NOT NULL THEN e1 ELSE e2 END */
		} else if (token == SQL_COALESCE) {
			cond = rel_value_exp(sql, rel, dn->data.sym, f, ek);

			if (cond) {
				result = cond;
				cond = rel_unop_(sql, rel_unop_(sql, cond, NULL, "isnull", card_value), NULL, "not", card_value);
			}
		} else {
			dlist *when = dn->data.sym->data.lval;

			if (opt_cond) {
				sql_exp *l = rel_value_exp(sql, rel, opt_cond, f, ek);
				sql_exp *r = rel_value_exp(sql, rel, when->h->data.sym, f, ek);
				if (!l || !r || rel_convert_types(sql, &l, &r, 1, type_equal) < 0) 
					return NULL;
				cond = rel_binop_(sql, l, r, NULL, "=", card_value);
			} else {
				cond = rel_logical_value_exp(sql, rel, when->h->data.sym, sql_sel);
			}
			result = rel_value_exp(sql, rel, when->h->next->data.sym, f, ek);
		}
		if (!cond || !result) 
			return NULL;
		list_prepend(conds, cond);
		list_prepend(results, result);

		restype = exp_subtype(result);

		if (token == SQL_NULLIF)
			dn = NULL;
		else
			dn = dn->next;
	}
	if (!restype) 
		return sql_error(sql, 02, "result type missing");
	/* for COALESCE we skip the last (else part) */
	for (; dn && (token != SQL_COALESCE || dn->next); dn = dn->next) {
		sql_exp *cond = NULL, *result = NULL;

		if (token == SQL_COALESCE) {
			cond = rel_value_exp(sql, rel, dn->data.sym, f, ek);

			if (cond) {
				result = cond;
				cond = rel_unop_(sql, rel_unop_(sql, cond, NULL, "isnull", card_value), NULL, "not", card_value);
			}
		} else {
			dlist *when = dn->data.sym->data.lval;

			if (opt_cond) {
				sql_exp *l = rel_value_exp(sql, rel, opt_cond, f, ek);
				sql_exp *r = rel_value_exp(sql, rel, when->h->data.sym, f, ek);
				if (!l || !r || rel_convert_types(sql, &l, &r, 1, type_equal) < 0) 
					return NULL;
				cond = rel_binop_(sql, l, r, NULL, "=", card_value);
			} else {
				cond = rel_logical_value_exp(sql, rel, when->h->data.sym, sql_sel);
			}
			result = rel_value_exp(sql, rel, when->h->next->data.sym, sql_sel, ek);
		}
		if (!cond || !result) 
			return NULL;
		list_prepend(conds, cond);
		list_prepend(results, result);

		tpe = exp_subtype(result);
		if (!tpe) 
			return sql_error(sql, 02, "result type missing");
		supertype(&rtype, restype, tpe);
		if (!tpe) 
			return sql_error(sql, 02, "result types %s,%s of case are not compatible", restype->type->sqlname, tpe->type->sqlname);
		restype = &rtype;
	}
	if (opt_else || else_exp) {
		sql_exp *result = else_exp;

		if (!result && !(result = rel_value_exp(sql, rel, opt_else, f, ek))) 
			return NULL;

		tpe = exp_subtype(result);
		if (tpe && restype) {
			supertype(&rtype, restype, tpe);
			tpe = &rtype;
		}
		restype = tpe;

		if (!result || !(result = rel_check_type(sql, restype, result, type_equal))) 
			return NULL;
		res = result;

		if (!res) 
			return NULL;
	} else {
		sql_exp *a = exp_atom(sql->sa, atom_general(sql->sa, restype, NULL));

		res = a;
	}

	for (n = conds->h, m = results->h; n && m; n = n->next, m = m->next) {
		sql_exp *cond = n->data;
		sql_exp *result = m->data;

		if (!(result = rel_check_type(sql, restype, result, type_equal))) 
			return NULL;

		/* remove any null's in the condition */
		if (has_nil(cond)) {
			sql_exp *condnil = rel_unop_(sql, cond, NULL, "isnull", card_value);
			cond = rel_nop_(sql, condnil, exp_atom_bool(sql->sa, 0), cond, NULL, NULL, "ifthenelse", card_value);
		}
		res = rel_nop_(sql, cond, result, res, NULL, NULL, "ifthenelse", card_value);
		if (!res) 
			return NULL;
		/* ugh overwrite res type */
		((sql_subfunc*)res->f)->res = *restype;
	}
	return res;
}

static sql_exp *
rel_case_exp(mvc *sql, sql_rel **rel, symbol *se, int f)
{
	dlist *l = se->data.lval;

	if (se->token == SQL_COALESCE) {
		symbol *opt_else = l->t->data.sym;

		return rel_case(sql, rel, se->token, NULL, l, opt_else, f);
	} else if (se->token == SQL_NULLIF) {
		return rel_case(sql, rel, se->token, NULL, l, NULL, f);
	} else if (l->h->type == type_list) {
		dlist *when_search_list = l->h->data.lval;
		symbol *opt_else = l->h->next->data.sym;

		return rel_case(sql, rel, SQL_CASE, NULL, when_search_list, opt_else, f);
	} else {
		symbol *scalar_exp = l->h->data.sym;
		dlist *when_value_list = l->h->next->data.lval;
		symbol *opt_else = l->h->next->next->data.sym;

		return rel_case(sql, rel, SQL_CASE, scalar_exp, when_value_list, opt_else, f);
	}
}

static sql_exp *
rel_cast(mvc *sql, sql_rel **rel, symbol *se, int f)
{

	dlist *dl = se->data.lval;
	symbol *s = dl->h->data.sym;
	sql_subtype *tpe = &dl->h->next->data.typeval;
	exp_kind ek = {type_value, card_column, FALSE};
	sql_exp *e = rel_value_exp(sql, rel, s, f, ek);

	if (!e)
		return NULL;
	/* strings may need too be truncated */
	if (tpe ->type ->localtype == TYPE_str) {
		if (tpe->digits > 0) {
			sql_subtype *et = exp_subtype(e);
			sql_subtype *it = sql_bind_localtype("int");
			sql_subfunc *c = sql_bind_func(sql->sa, sql->session->schema, "truncate", et, it);
			if (c)
				e = exp_binop(sql->sa, e, exp_atom_int(sql->sa, tpe->digits), c);
		}
	}
	if (e)
		return rel_check_type(sql, tpe, e, type_cast);
	return NULL;
}

static sql_exp *
rel_next_value_for( mvc *sql, symbol *se )
{
	char *seq = qname_table(se->data.lval);
	char *sname = qname_schema(se->data.lval);
	sql_schema *s = NULL;
	sql_subtype t;
	sql_subfunc *f;

	if (sname && !(s = mvc_bind_schema(sql, sname)))
		return sql_error(sql, 02,
			"NEXT VALUE FOR: no such schema '%s'", sname);
	if (!s)
		s = sql->session->schema;

	if (!find_sql_sequence(s, seq) && !stack_find_rel_view(sql, seq))
		return sql_error(sql, 02, "NEXT VALUE FOR: "
			"no such sequence '%s'.'%s'", s->base.name, seq);
	sql_find_subtype(&t, "varchar", 0, 0);
	f = sql_bind_func(sql->sa, s, "next_value_for", &t, &t);
	assert(f);
	return exp_binop(sql->sa, exp_atom_str(sql->sa, s->base.name, &t),
			exp_atom_str(sql->sa, seq, &t), f);
}

/* some users like to use aliases already in the groupby */
static sql_exp *
rel_selection_ref(mvc *sql, sql_rel *rel, symbol *grp, dlist *selection )
{
	dnode *n;
	dlist *gl = grp->data.lval;
	char *name = NULL;
	exp_kind ek = {type_value, card_column, FALSE};

	if (dlist_length(gl) > 1)
		return NULL;
	if (!selection)
		return NULL;

	name = gl->h->data.sval;
	for (n = selection->h; n; n = n->next) {
		/* we only look for columns */
		if (n->data.sym->token == SQL_COLUMN) {
			dlist *l = n->data.sym->data.lval;
			/* AS name */
			if (l->h->next->data.sval &&
					strcmp(l->h->next->data.sval, name) == 0){
				sql_exp *ve = rel_value_exp(sql, &rel, l->h->data.sym, sql_sel, ek);
				if (ve) {
					dlist *l = dlist_create(sql->sa);
					symbol *sym;
					exp_setname(sql->sa, ve, NULL, name);
					/* now we should rewrite the selection
					   such that it uses the new group
					   by column
					*/
					dlist_append_string(sql->sa, l,
						sa_strdup(sql->sa, name));
					sym = symbol_create_list(sql->sa, SQL_COLUMN, l);
					l = dlist_create(sql->sa);
					dlist_append_symbol(sql->sa, l, sym);
					/* no alias */
					dlist_append_symbol(sql->sa, l, NULL);
					n->data.sym = symbol_create_list(sql->sa, SQL_COLUMN, l);
				
				}
				return ve;
			}
		}
	}
	return NULL;
}

static list *
rel_group_by(mvc *sql, sql_rel *rel, symbol *groupby, dlist *selection, int f )
{
	sql_rel *or = rel;
	dnode *o = groupby->data.lval->h;
	list *exps = new_exp_list(sql->sa);

	for (; o; o = o->next) {
		symbol *grp = o->data.sym;
		sql_exp *e = rel_column_ref(sql, &rel, grp, f);

		if (or != rel)
			return NULL;
		if (!e) {
			char buf[ERRSIZE];
			/* reset error */
			sql->session->status = 0;
			strcpy(buf, sql->errstr);
			sql->errstr[0] = '\0';

			e = rel_selection_ref(sql, rel, grp, selection);
			if (!e) {
				if (sql->errstr[0] == 0)
					strcpy(sql->errstr, buf);
				return NULL;
			}
		}
		append(exps, e);
	}
	return exps;
}

/* find selection expressions matching the order by column expression */

/* first limit to simple columns only */
static sql_exp *
rel_order_by_simple_column_exp(mvc *sql, sql_rel *r, symbol *column_r)
{
	sql_exp *e = NULL;
	dlist *l = column_r->data.lval;

	if (column_r->type == type_int)
		return NULL;
	assert(column_r->token == SQL_COLUMN && column_r->type == type_list);

	assert(is_project(r->op));
	r = r->l;
	if (!r)
		return e;
	set_processed(r);
	if (dlist_length(l) == 1) {
		char *name = l->h->data.sval;
		e = rel_bind_column(sql, r, name, sql_sel);
	}
	if (dlist_length(l) == 2) {
		char *tname = l->h->data.sval;
		char *name = l->h->next->data.sval;

		e = rel_bind_column2(sql, r, tname, name, sql_sel);
	}
	if (!e) {
		/* now we need to rewrite r
			project( project( all exps + order_exps),
				order_exps, exps )
		*/
	}
	if (e)
		return e;
	return sql_error(sql, 02, "ORDER BY: absolute column names not supported");
}

/* second complex columns only */
static sql_exp *
rel_order_by_column_exp(mvc *sql, sql_rel **R, symbol *column_r)
{
	sql_rel *r = *R;
	sql_exp *e = NULL;
	exp_kind ek = {type_value, card_column, FALSE};

	/* TODO rewrite relation !!! */
	assert(is_project(r->op));
	r = r->l;
	
	if (!r)
		return e;

	if (!is_project(r->op)) {
		r = rel_project(sql->sa, r, rel_projections(sql, r, NULL, 1, 1));
		(*R)->l = r;
		set_processed(r);
	}

	if (!e) {
		sql_rel *or = r;

		e = rel_value_exp(sql, &r, column_r, sql_sel, ek);
		if (r && or != r)
			(*R)->l = r;
		/* add to internal project */
		if (e) {
			rel_project_add_exp(sql, r, e);
			e = rel_lastexp(sql, r);
		}
	}
	if (e)
		return e;
	return sql_error(sql, 02, "ORDER BY: absolute column names not supported");
}


static list *
rel_order_by(mvc *sql, sql_rel **R, symbol *orderby, int f )
{
	sql_rel *rel = *R;
	sql_rel *or = rel;
	list *exps = new_exp_list(sql->sa);
	dnode *o = orderby->data.lval->h;

	for (; o; o = o->next) {
		symbol *order = o->data.sym;

		if (order->token == SQL_COLUMN) {
			symbol *col = order->data.lval->h->data.sym;
			int direction = order->data.lval->h->next->data.i_val;
			sql_exp *e = NULL;

			if (col->token == SQL_COLUMN) {
				e = rel_column_ref(sql, &rel, col, f);
				if (e && e->card <= CARD_ATOM) {
					sql_subtype *tpe = &e->tpe;
					/* integer atom on the stack */
					if (e->type == e_atom &&
					    tpe->type->eclass == EC_NUM) {
						atom *a = e->l?e->l:sql->args[e->flag];
						int nr = (int)atom_get_int(a);

						e = exps_get_exp(rel->exps, nr);
						if (!e)
							return NULL;
						e = exp_column(sql->sa, e->rname, e->r, exp_subtype(e), rel->card, has_nil(e), is_intern(e));
					} else {
						return sql_error(sql, 02, "order not of type SQL_COLUMN\n");
					}
				}
			}

			assert(order->data.lval->h->next->type == type_int);
			if (or != rel)
				return NULL;
			if (!e && sql->session->status != -ERR_AMBIGUOUS && col->token == SQL_COLUMN) {
				/* reset error */
				sql->session->status = 0;
				sql->errstr[0] = '\0';

				e = rel_order_by_simple_column_exp(sql, rel, col);
				if (e && e->card != rel->card) 
					e = NULL;
			}
			if (!e && sql->session->status != -ERR_AMBIGUOUS) {
				/* reset error */
				sql->session->status = 0;
				sql->errstr[0] = '\0';

				e = rel_order_by_column_exp(sql, &rel, col);
				if (e && e->card != rel->card) 
					e = NULL;
			}
			if (!e) 
				return NULL;
			set_direction(e, direction);
			append(exps, e);
		} else {
			return sql_error(sql, 02, "order not of type SQL_COLUMN\n");
		}
	}
	*R = rel;
	return exps;
}

/* window functions */
static sql_exp *
rel_rankop(mvc *sql, sql_rel **rel, symbol *se, int f)
{
	dlist *l = se->data.lval;
	symbol *window_function = l->h->data.sym;
	dlist *window_specification = l->h->next->data.lval;
	char *aggrstr = NULL;
	sql_subfunc *wf = NULL;
	sql_exp *e = NULL;
	sql_rel *r = *rel;
	list *gbe = NULL, *obe = NULL;
	sql_subtype *idtype = sql_bind_localtype("oid");
	
	if (window_function->token == SQL_RANK) {
		aggrstr = window_function->data.sval;
	} else { /* window aggr function */
		aggrstr = window_function->data.lval->h->data.sval;
	}

	if (f == sql_where) {
		char *uaggrstr = malloc(strlen(aggrstr) + 1);
		e = sql_error(sql, 02, "%s: not allowed in WHERE clause",
				toUpperCopy(uaggrstr, aggrstr));
		free(uaggrstr);
		return e;
	}

	/* window operations are only allowed in the projection */
	if (r->op != op_project)
		return sql_error(sql, 02, "OVER: only possible within the selection");

	/* Partition By */
	if (window_specification->h->data.sym) {
		gbe = rel_group_by(sql, r, window_specification->h->data.sym, NULL /* cannot use (selection) column references, as this result is a selection column */, f );
		if (!gbe)
			return NULL;
	}
	/* Order By */
	if (window_specification->h->next->data.sym) {
		obe = rel_order_by(sql, &r, window_specification->h->next->data.sym, f);
		if (!obe)
			return NULL;
	}
	wf = sql_bind_func(sql->sa, sql->session->schema, aggrstr, idtype, NULL);
	if (!wf)
		return sql_error(sql, 02, "SELECT: function '%s' not found", aggrstr );
	/* now we need the gbe and obe lists */
	e = exp_op(sql->sa, gbe, wf);
	/* make sure the expression has the proper cardinality */
	e->card = CARD_AGGR;
	if (obe)
		e->r = obe;
	else	/* e->r specifies window expression */
		e->r = new_exp_list(sql->sa);
	return e;
}

sql_exp *
rel_value_exp2(mvc *sql, sql_rel **rel, symbol *se, int f, exp_kind ek, int *is_last)
{
	if (!se)
		return NULL;

	if (THRhighwater())
		return sql_error(sql, 10, "SELECT: too many nested operators");

	switch (se->token) {
	case SQL_OP:
		return rel_op(sql, se, ek);
	case SQL_UNOP:
		return rel_unop(sql, rel, se, f, ek);
	case SQL_BINOP:
		return rel_binop(sql, rel, se, f, ek);
	case SQL_NOP:
		return rel_nop(sql, rel, se, f, ek);
	case SQL_AGGR:
		return rel_aggr(sql, rel, se, f);
	case SQL_RANK:
		return rel_rankop(sql, rel, se, f);
	case SQL_COLUMN:
		return rel_column_ref(sql, rel, se, f );
	case SQL_NAME:
		return rel_var_ref(sql, se->data.sval, 1);
	case SQL_SELECT: {
		sql_rel *r;

		r = rel_subquery(sql, NULL, se, ek);
		if (r) {
			sql_exp *e;

			rel_setsubquery(r);
			e = rel_lastexp(sql, r);

			/* group by needed ? */
			if (e->card > CARD_ATOM) {
				sql_subaggr *zero_or_one = sql_bind_aggr(sql->sa, sql->session->schema, "zero_or_one", exp_subtype(e));

				e = exp_aggr1(sql->sa, e, zero_or_one, 0, 0, CARD_ATOM, 0);
			}
			if (*rel) {
				/* current projection list */
				sql_rel *p = *rel;
				list *pre_proj = (*rel)->exps;

				if (is_project((*rel)->op) && (*rel)->l) {
					(*rel)->exps = NULL;
					p = rel_dup((*rel)->l);
					rel_destroy(*rel);
					*rel = NULL;
				} else {
					pre_proj = rel_projections(sql, *rel, NULL, 1, 1);
				}
				if (*rel && is_project((*rel)->op) && !(*rel)->l) {
					list *l = (*rel)->exps;
					int need_preproj = 0;

					/* handles 2 simple (expression) projections */
					if (is_project(r->op) && l && list_length(l)) {
						l = list_merge(l, r->exps, (fdup)NULL);
						r->exps = l;
						(*rel)->exps = NULL;

					/* but also project ( project[] [x], [x]) */
					} else if (is_project(r->op) && l && !list_length(l)) {
						need_preproj = 1;
					}
					rel_destroy(*rel);
					*rel = r;
					if (need_preproj)
						*rel = rel_project(sql->sa, *rel, pre_proj);
				} else {
					*rel = rel_crossproduct(sql->sa, p, r, op_join);
					*rel = rel_project(sql->sa, *rel, pre_proj);
				}
				*is_last = 1;
				return rel_lastexp(sql, r);
			} else {
				*rel = r;
			}
			*is_last=1;
			return e;
		}
		if (!r && sql->session->status != -ERR_AMBIGUOUS) {
			if (!*rel)
				return NULL;

			/* reset error */
			sql->session->status = 0;
			sql->errstr[0] = '\0';

			*rel = r = rel_subquery(sql, *rel, se, ek);
		}
		if (!r)
			return NULL;
		return rel_find_lastexp(*rel);
	}
	case SQL_TABLE: {
		/* turn a subquery into a tabular result */
		*rel = rel_selects(sql, se->data.sym /*, *rel, se->data.sym, ek*/);
		if (*rel)
			return rel_find_lastexp(*rel);
		return NULL;
	}
	case SQL_PARAMETER:{
		if (sql->emode != m_prepare)
			return sql_error(sql, 02, "SELECT: parameters ('?') not allowed in normal queries, use PREPARE");
		assert(se->type == type_int);
		return exp_atom_ref(sql->sa, se->data.i_val, NULL);
	}
	case SQL_NULL:
		return exp_atom(sql->sa, atom_general(sql->sa, sql_bind_localtype("str"), NULL));
	case SQL_ATOM:{
		AtomNode *an = (AtomNode *) se;

		if (!an || !an->a) {
			return exp_atom(sql->sa, atom_general(sql->sa, sql_bind_localtype("str"), NULL));
		} else {
			return exp_atom(sql->sa, atom_dup(sql->sa, an->a));
		}
	}
		break;
	case SQL_NEXT:
		return rel_next_value_for(sql, se);
	case SQL_CAST:
		return rel_cast(sql, rel, se, f);
	case SQL_CASE:
	case SQL_COALESCE:
	case SQL_NULLIF:
		return rel_case_exp(sql, rel, se, f);
	case SQL_XMLELEMENT:
	case SQL_XMLFOREST:
	case SQL_XMLCOMMENT:
	case SQL_XMLATTRIBUTE:
	case SQL_XMLCONCAT:
	case SQL_XMLDOCUMENT:
	case SQL_XMLPI:
	case SQL_XMLTEXT:
		return rel_xml(sql, rel, se, f, ek);
	default:
		return rel_logical_value_exp(sql, rel, se, f);
	}
}

sql_exp *
rel_value_exp(mvc *sql, sql_rel **rel, symbol *se, int f, exp_kind ek)
{
	int is_last = 0;
	sql_exp *e;
	if (!se)
		return NULL;

	if (THRhighwater())
		return sql_error(sql, 10, "SELECT: too many nested operators");

	e = rel_value_exp2(sql, rel, se, f, ek, &is_last);
	if (e && (se->token == SQL_SELECT || se->token == SQL_TABLE) && !is_last) {
		assert(*rel);
		return rel_lastexp(sql, *rel);
	}
	return e;
}

static sql_exp *
column_exp(mvc *sql, sql_rel **rel, symbol *column_e, int f)
{
	dlist *l = column_e->data.lval;
	exp_kind ek = {type_value, card_column, FALSE};
	sql_exp *ve = rel_value_exp(sql, rel, l->h->data.sym, f, ek);

	if (!ve)
		return NULL;
	/* AS name */
	if (ve && l->h->next->data.sval)
		exp_setname(sql->sa, ve, NULL, l->h->next->data.sval);
	return ve;
}

static list *
rel_table_exp(mvc *sql, sql_rel *rel, symbol *column_e )
{
	if (column_e->token == SQL_TABLE) {
		char *tname = column_e->data.lval->h->data.sval;
		list *exps;
	
		if ((exps = rel_table_projections(sql, rel, tname)) != NULL)
			return exps;
		if (!tname)
			return sql_error(sql, 02,
				"Table expression without table name");
		return sql_error(sql, 02,
				"Column expression Table '%s' unknown", tname);
	}
	return NULL;
}

sql_exp *
rel_column_exp(mvc *sql, sql_rel **rel, symbol *column_e, int f)
{
	if (column_e->token == SQL_COLUMN) {
		return column_exp(sql, rel, column_e, f);
	}
	return NULL;
}

static sql_rel *
rel_simple_select(mvc *sql, sql_rel *rel, symbol *where, dlist *selection, int distinct)
{
	dnode *n = 0;

	if (!selection)
		return sql_error(sql, 02, "SELECT: the selection or from part is missing");
	if (where) {
		sql_rel *r = rel_logical_exp(sql, rel, where, sql_where);
		if (!r)
			return NULL;
		rel = r;
	}
	if (!rel || rel->op != op_project)
		rel = rel_project(sql->sa, rel, new_exp_list(sql->sa));
	for (n = selection->h; n; n = n->next ) {
		/* Here we could get real column expressions (including single
		 * atoms) but also table results. Therefore we try both
		 * rel_column_exp and rel_table_exp.
		 */
		list *te = NULL;
		sql_exp *ce = rel_column_exp(sql, &rel, n->data.sym, sql_sel);

		if (ce && exp_subtype(ce)) {
			/* new relational, we need to rewrite */
			rel_project_add_exp(sql, rel, ce);
			continue;
		} else if (!ce) {
			te = rel_table_exp(sql, rel, n->data.sym );
		} else 
			ce = NULL;
		if (!ce && !te)
			return sql_error(sql, 02, "SELECT: subquery result missing");
		/* here we should merge the column expressions we obtained
		 * so far with the table expression, ie t1.* or a subquery
		 */
		if (te)
			list_merge( rel->exps, te, (fdup)NULL);
	}
	if (rel)
		set_processed(rel);

	if (rel && distinct)
		rel = rel_distinct(rel);

	return rel;
}

static sql_rel *
join_on_column_name(mvc *sql, sql_rel *rel, sql_rel *t1, sql_rel *t2, int op, int l_nil, int r_nil)
{
	int nr = ++sql->label, found = 0, full = (op != op_join);
	char name[16], *nme;
	list *exps = rel_projections(sql, t1, NULL, 1, 0);
	list *r_exps = rel_projections(sql, t2, NULL, 1, 0);
	list *outexps = new_exp_list(sql->sa);
	node *n;

	nme = number2name(name, 16, nr);
	if (!exps)
		return NULL;
	for (n = exps->h; n; n = n->next) {
		sql_exp *le = n->data;
		char *nm = le->name;
		sql_exp *re = exps_bind_column(r_exps, nm, NULL);

		if (re) {
			found = 1;
			rel = rel_compare_exp(sql, rel, le, re, "=", NULL, TRUE);
			if (full) {
				sql_exp *cond;
				cond = rel_unop_(sql, le, NULL, "isnull", card_value);
				le = rel_nop_(sql, cond, re, le, NULL, NULL, "ifthenelse", card_value);
			}
			exp_setname(sql->sa, le, nme, nm = sa_strdup(sql->sa, nm));
			append(outexps, le);
			list_remove_data(r_exps, re);
		} else {
			if (l_nil)
				set_has_nil(le);
			append(outexps, le);
		}
	}
	if (!found) {
		sql_error(sql, 02, "JOIN: no columns of tables '%s' and '%s' match", rel_get_name(t1)?rel_get_name(t1):"", rel_get_name(t2)?rel_get_name(t2):"");
		rel_destroy(rel);
		return NULL;
	}
	for (n = r_exps->h; n; n = n->next) {
		sql_exp *re = n->data;
		if (r_nil)
			set_has_nil(re);
		append(outexps, re);
	}
	rel = rel_project(sql->sa, rel, outexps);
	set_processed(rel);
	return rel;
}


#if 0
static sql_rel *exp_top_relation(sql_exp *e )
{
	switch(e->type) {	
	case e_atom:
		return NULL;
	case e_convert:
	case e_cmp:
		if (e->l)
			return exp_top_relation(e->l);
		break;
	case e_column:
	default:
		return NULL;
	}
	return NULL;
}
#endif

static int
check_correlation_exps( list *exps )
{
	node *n;

	for (n = exps->h; n; n = n->next) {
		sql_exp *e = n->data;
		if (e->type != e_cmp || e->flag != cmp_equal)
			return -1;
	}
	return 0;
}

static int
exp_is_not_intern(sql_exp *e)
{
	return is_intern(e)?-1:0;
}

static void
rel_remove_internal_exp(sql_rel *rel)
{
	if (rel->exps) {
		list *n_exps = list_select(rel->exps, rel, (fcmp)&exp_is_not_intern, (fdup)NULL);

		rel->exps = n_exps;
	}
}

static sql_rel *
rel_select_exp(mvc *sql, sql_rel *rel, sql_rel *outer, SelectNode *sn, exp_kind ek)
{
	dnode *n;
	int aggr = 0;
	list *jexps = NULL;
	list *pre_prj = NULL;
	list *outer_gbexps = NULL;
	sql_rel *inner = NULL;
	int decorrelated = 0;

	assert(sn->s.token == SQL_SELECT);
	if (!sn->selection)
		return sql_error(sql, 02, "SELECT: the selection or from part is missing");

	if (!sn->from)
		return rel_simple_select(sql, rel, sn->where, sn->selection, sn->distinct);

	/* if within the selection, keep the current projections */
	if (outer && is_project(outer->op) && !is_processed(outer) && !rel_is_ref(outer)) {
		/* keep projections the hard way, ie don't rename them */
		assert(rel->l == outer);

		pre_prj = outer->exps;
		outer->exps = NULL;

		rel->l = rel_dup(outer->l);
		rel_destroy(outer);
		outer = rel->l;
	}

	if (sn->where) {
		sql_rel *r = rel_logical_exp(sql, rel, sn->where, sql_where);
		if (!r)
			return sql_error(sql, 02, "Subquery result missing");
		rel = r;
	}

	if (rel) {
		if (rel && sn->groupby) {
			list *gbe = rel_group_by(sql, rel, sn->groupby, sn->selection, sql_sel );

			if (!gbe)
				return NULL;
			rel = rel_groupby(sql->sa, rel, gbe);
			aggr = 1;
		}

		/* decorrelate if possible */

		/* TODO if ek.card == card_set (IN/EXISTS etc), we could do
			something less expensive as group by's ! */
		if (outer && rel->op == op_join && rel->l == outer && ek.card != card_set) {
			node *n;
			/* correlation expressions */
			list *ce = list_select(rel->exps, rel, (fcmp) &exp_is_correlation, (fdup)NULL);

			if (!ce || list_length(ce) == 0 || check_correlation_exps(ce) != 0) {
				if (ek.card != card_set) {
					node *n;
					/* group by on identity */
					sql_exp *e;

					outer_gbexps = rel_projections(sql, outer, NULL, 1, 1);
					if (!is_project(outer->op))
						rel->l = outer = rel_project(sql->sa, outer, rel_projections(sql, outer, NULL, 1, 1));
					e = rel_unop_(sql, outer->exps->h->data, NULL, "identity", card_value);
					set_intern(e);
					rel_project_add_exp(sql, outer, e);
					set_processed(outer);
					e = rel_lastexp(sql, outer);

					assert(pre_prj != NULL);
					for(n = pre_prj->h; n; n = n->next) {
						sql_exp *e = n->data;
						e->card = CARD_AGGR;
					}
					rel = rel_groupby_gbe(sql->sa, rel, e);
				}
			} else {
				node *m;

				list *gbexps = new_exp_list(sql->sa);

				decorrelated = 1;
				jexps = new_exp_list(sql->sa);
				for (n = ce->h; n; n = n->next) {
					sql_exp *e = n->data;
		
					/* add right expressions to the
					group by exps */
					append(gbexps, e->r);
				}
	
				/* now create a groupby on the inner */
				inner = rel_groupby(sql->sa, rel_dup(rel->r), gbexps);
				inner = rel_label(sql, inner);
				for (n = ce->h, m = gbexps->h; n && m; n = n->next, m = m->next) {
					sql_exp *e = n->data;
					sql_exp *gbe = m->data;
	
					assert(e->type == e_cmp);
					gbe = exp_column(sql->sa, rel_name(inner), exp_name(gbe), exp_subtype(gbe), inner->card, has_nil(gbe), is_intern(gbe));
					e = exp_compare(sql->sa, e->l, gbe, e->flag);
					append(jexps, e);
				}
				outer = rel_dup(outer);
				rel_destroy(rel);
				rel = inner;
				inner = NULL;
			}
		}
	}

	if (sn->having) {
		/* having implies group by, ie if not supplied do a group by */
		if (rel->op != op_groupby)
			rel = rel_groupby(sql->sa,  rel, NULL);
		aggr = 1;
	}

	n = sn->selection->h;
	if (!outer || (!decorrelated && ek.card == card_set)) {
		if (outer) /* for non decorrelated or card_set sub
			      queries we project all of the outer */
			rel = rel_project(sql->sa, rel,
				rel_projections(sql, outer, NULL, 1, 1));
		else
			rel = rel_project(sql->sa, rel, new_exp_list(sql->sa));
	}
	if (!inner)
		inner = rel;
	for (; n; n = n->next) {
		/* Here we could get real column expressions
		 * (including single atoms) but also table results.
		 * Therefor we try both rel_column_exp
		 * and rel_table_exp.

		 * TODO
			the rel_table_exp should simply return a new
			relation
		 */
		list *te = NULL;
		sql_rel *o_inner = inner;
		sql_exp *ce = rel_column_exp(sql, &inner, n->data.sym, sql_sel);

		if (inner != o_inner) /* relation got rewritten */
			rel = inner;

		if (ce && exp_subtype(ce)) {
			if (rel->card < ce->card) {
				/* This doesn't work without de-correlations, ie in that case it should be done later */
				if (outer) {
					sql_subaggr *zero_or_one = sql_bind_aggr(sql->sa, sql->session->schema, "zero_or_one", exp_subtype(ce));

					ce = exp_aggr1(sql->sa, ce, zero_or_one, 0, 0, rel->card, 0);
				} else if (ce->name) {
					return sql_error(sql, 02, "SELECT: cannot use non GROUP BY column '%s' in query results without an aggregate function", ce->name);
				} else {
					return sql_error(sql, 02, "SELECT: cannot use non GROUP BY column in query results without an aggregate function");
				}
			}
			/*
			   because of the selection, the inner
			   relation may change.
			   We try hard to keep a projection
			   around this inner relation.
			*/
			if (!is_project(inner->op)) {
				if (outer && pre_prj) {
					inner = rel_project(sql->sa, inner, pre_prj);
					pre_prj = rel_projections(sql, inner, NULL, 1, 1);
				} else
					inner = rel_project(sql->sa, inner, new_exp_list(sql->sa));
			}
			rel_project_add_exp(sql, inner, ce);
			rel = inner;
			continue;
		} else if (!ce) {
			te = rel_table_exp(sql, rel, n->data.sym);
		} else 
			ce = NULL;
		if (!ce && !te)
			return sql_error(sql, 02, "SELECT: subquery result missing");
		/* here we should merge the column expressions we
		 * obtained so far with the table expression, ie
		 * t1.* or a subquery.
		 */
		list_merge( rel->exps, te, (fdup)NULL);
	}

	if (sn->having) {
		inner = rel->l;
		assert(is_project(rel->op) && inner);
	
		inner = rel_logical_exp(sql, inner, sn->having, sql_having);

		if (!inner)
			return NULL;
		if (inner -> exps && exps_card(inner->exps) > CARD_AGGR)
			return sql_error(sql, 02, "SELECT: cannot compare sets with values, probably an aggregate function missing");
		rel -> l = inner;
	}
	if (rel)
		set_processed(rel);
	if (aggr && rel) {
		sql_rel *l = rel;
		while(l && !is_groupby(l->op))
			if (is_project(l->op) || is_select(l->op))
				l = l -> l;
			else
				l = NULL;
		if (l)
			set_processed(l);
	}

	if (rel && sn->distinct)
		rel = rel_distinct(rel);

	if (outer && jexps) {
		rel = rel_crossproduct(sql->sa, outer, rel, (pre_prj||!ek.reduce)?op_join:op_semi);
		rel_join_add_exps(sql->sa, rel, jexps );

		/* We need to project all of the (old) outer
			(ie its current projected and any other column) */
		if (pre_prj) {
			sql_exp *e = rel_lastexp(sql, rel->r);
			list *exps = list_dup(pre_prj, (fdup)NULL);

			exps = list_merge(exps, rel_projections(sql, outer, NULL, 1, 1), (fdup)NULL);
			rel = rel_project(sql->sa, rel, exps);
			rel_project_add_exp(sql, rel, e);
			set_processed(rel);
		} else if (!ek.reduce) {
			if (!is_project(rel->op)) {
				rel = rel_project(sql->sa, rel, rel_projections(sql, rel, NULL, 1, 1));
				set_processed(rel);
			}
		}
	}

	/* Joins within the selection should recreate the projection
	   and use an outer join */
	if (outer && pre_prj) {
		sql_rel *l;
		node *n;

		if (outer_gbexps) {
			assert(is_groupby(rel->op));

			/* merge the expressions of the correlated outer, ie
			   first prepend the identity then append
			   the result expression.  */
			for (n=rel->exps->h; n && n->next; n = n->next)
				list_prepend(outer_gbexps, n->data);
			list_append(outer_gbexps, n->data);
			rel->exps = outer_gbexps;
			exps_fix_card(outer_gbexps, rel->card);
		}
		l = rel = rel_project(sql->sa, rel, pre_prj);
		while(l && l->op != op_join)
			l = l->l;
		if (l && l->op == op_join && l->l == outer && ek.card != card_set)
			l->op = op_left;
	}

	if (rel && sn->orderby) {
		list *obe = rel_order_by(sql, &rel, sn->orderby, sql_sel);

		if (!obe)
			return NULL;
		rel = rel_orderby(sql, rel, obe);
	}
	if (!rel)
		return NULL;

	if (sn->limit || sn->offset) {
		sql_subtype *wrd = sql_bind_localtype("wrd");
		list *exps = new_exp_list(sql->sa);

		if (sn->limit) {
			sql_exp *l = rel_value_exp( sql, NULL, sn->limit, 0, ek);

			if (!l || !(l=rel_check_type(sql, wrd, l, type_equal)))
				return NULL;
		if ((ek.card != card_relation && sn->limit) &&
			(ek.card == card_value && sn->limit)) {
			sql_subaggr *zero_or_one = sql_bind_aggr(sql->sa, sql->session->schema, "zero_or_one", exp_subtype(l));

			l = exp_aggr1(sql->sa, l, zero_or_one, 0, 0, CARD_ATOM, 0);
		}
	/*	return sql_error(sql, 01, "SELECT: LIMIT only allowed on outermost SELECT"); */
			append(exps, l);
		} else
			append(exps, NULL);
		if (sn->offset) {
			sql_exp *o = rel_value_exp( sql, NULL, sn->offset, 0, ek);
			if (!o || !(o=rel_check_type(sql, wrd, o, type_equal)))
				return NULL;
			append(exps, o);
		}
		rel = rel_topn(sql->sa, rel, exps);
	}
	return rel;
}


static sql_rel *
rel_query(mvc *sql, sql_rel *rel, symbol *sq, int toplevel, exp_kind ek)
{
	sql_rel *res = NULL;
	SelectNode *sn = NULL;

	if (sq->token != SQL_SELECT)
		return table_ref(sql, rel, sq);

	/* select ... into is currently not handled here ! */
 	sn = (SelectNode *) sq;
	if (sn->into)
		return NULL;

	if (ek.card != card_relation && sn->orderby)
		return sql_error(sql, 01, "SELECT: ORDER BY only allowed on outermost SELECT");


	if (sn->from) {		/* keep variable list with tables and names */
		dlist *fl = sn->from->data.lval;
		dnode *n = NULL;
		sql_rel *fnd = NULL;

		for (n = fl->h; n ; n = n->next) {
			fnd = table_ref(sql, rel, n->data.sym);

			if (!fnd)
				break;
			if (res)
				res = rel_crossproduct(sql->sa, res, fnd, op_join);
			else
				res = fnd;
		}
		if (!fnd) {
			if (res)
				rel_destroy(res);
			return NULL;
		}
		if (rel /*&& !toplevel */) {
			rel_setsubquery(res);
			res = rel_crossproduct(sql->sa, rel, res, op_join);
		}

	} else if (toplevel || !res) {	/* only on top level query */
		return rel_simple_select(sql, rel, sn->where, sn->selection, sn->distinct);
	}
	if (res)
		rel = rel_select_exp(sql, res, rel, sn, ek);
	return rel;
}

static sql_rel *
rel_setquery_(mvc *sql, sql_rel *l, sql_rel *r, dlist *cols, int op )
{
	sql_rel *rel;

	if (!cols) {
		node *n, *m;
		int changes = 0;

		list *ls = rel_projections(sql, l, NULL, 0, 1);
		list *rs = rel_projections(sql, r, NULL, 0, 1);
		list *nls = new_exp_list(sql->sa);
		list *nrs = new_exp_list(sql->sa);

		for (n = ls->h, m = rs->h; n && m; n = n->next, m = m->next) {
			sql_exp *le = n->data, *lb = le;
			sql_exp *re = m->data, *rb = re;

			if ((rel_convert_types(sql, &le, &re, 1, type_set) < 0))
				return NULL;
			if (le != lb || re != rb)
				changes = 1;
			append(nls, le);
			append(nrs, re);
		}
		if (changes) {
			l = rel_project(sql->sa, l, nls);
			r = rel_project(sql->sa, r, nrs);
			set_processed(l);
			set_processed(r);
		}
	}
	rel = rel_setop(sql->sa, l, r, (operator_type)op);
	rel->exps = rel_projections(sql, rel, NULL, 0, 1);
	set_processed(rel);
	return rel;
}


static sql_rel *
rel_setquery(mvc *sql, sql_rel *rel, symbol *q)
{
	sql_rel *res = NULL;
	dnode *n = q->data.lval->h;
	symbol *tab_ref1 = n->data.sym;
	int dist = n->next->data.i_val;
	dlist *corresponding = n->next->next->data.lval;
	symbol *tab_ref2 = n->next->next->next->data.sym;

	sql_rel *t1 = table_ref(sql, NULL, tab_ref1);
	sql_rel *t2 = table_ref(sql, NULL, tab_ref2);

	assert(n->next->type == type_int);
	(void)rel; /* TODO correlated unions */
	if (!t1 || !t2)
		return NULL;

	rel_remove_internal_exp(t1);
	rel_remove_internal_exp(t2);
	if (list_length(t1->exps) != list_length(t2->exps)) {
		int t1nrcols = list_length(t1->exps);
		int t2nrcols = list_length(t2->exps);
		char *op = "UNION";
		if (q->token == SQL_EXCEPT)
			op = "EXCEPT";
		else if (q->token == SQL_INTERSECT)
			op = "INTERSECT";
		rel_destroy(t1);
		rel_destroy(t2);
		return sql_error(sql, 02, "%s: column counts (%d and %d) do not match", op, t1nrcols, t2nrcols);
	}
	if (t1 && dist)
		t1 = rel_distinct(t1);
	if ( q->token == SQL_UNION) {
		if (t2 && dist)
			t2 = rel_distinct(t2);
		res = rel_setquery_(sql, t1, t2, corresponding, op_union );
		if (res && dist)
			res = rel_distinct(res);
	}
	if ( q->token == SQL_EXCEPT)
		res = rel_setquery_(sql, t1, t2, corresponding, op_except );
	if ( q->token == SQL_INTERSECT)
		res = rel_setquery_(sql, t1, t2, corresponding, op_inter );
	return res;
}



static sql_rel *
rel_joinquery_(mvc *sql, sql_rel *rel, symbol *tab1, int natural, jt jointype, symbol *tab2, symbol *js)
{
	operator_type op = op_join;
	sql_rel *t1, *t2, *inner;
	int l_nil = 0, r_nil = 0;

	t1 = table_ref(sql, rel, tab1);
	t2 = table_ref(sql, rel, tab2);

	if (!t1 || !t2)
		return NULL;

	if (rel_name(t1) && rel_name(t2) && strcmp(rel_name(t1), rel_name(t2)) == 0) {
		sql_error(sql, 02, "SELECT: '%s' on both sides of the JOIN expression;", rel_name(t1));
		rel_destroy(t1);
		rel_destroy(t2);
		return NULL;
	}

	switch(jointype) {
	case jt_inner: op = op_join;
		break;
	case jt_left: op = op_left;
		r_nil = 1;
		break;
	case jt_right: op = op_right;
		l_nil = 1;
		break;
	case jt_full: op = op_full;
		l_nil = 1;
		r_nil = 1;
		break;
	case jt_union:
		/* fool compiler */
		return NULL;
	}
	inner = rel = rel_crossproduct(sql->sa, t1, t2, op_join);

	if (js && natural) {
		return sql_error(sql, 02, "SELECT: cannot have a NATURAL JOIN with a join specification (ON or USING);");
	}
	if (!js && !natural) {
		return sql_error(sql, 02, "SELECT: must have NATURAL JOIN or a JOIN with a join specification (ON or USING);");
	}

	if (js && js->token != SQL_USING) {	/* On sql_logical_exp */
		rel = rel_logical_exp(sql, rel, js, sql_where);

		if (!rel)
			return rel;
		if (l_nil || r_nil) { /* add projection for correct NOT NULL */
			list *outexps = new_exp_list(sql->sa), *exps;
			node *m;

			exps = rel_projections(sql, t1, rel_get_name(t1), 1, 1);
			for (m = exps->h; m; m = m->next) {
				sql_exp *ls = m->data;
				if (l_nil)
					set_has_nil(ls);
				append(outexps, ls);
			}
			exps = rel_projections(sql, t2, rel_get_name(t2), 1, 1);
			for (m = exps->h; m; m = m->next) {
				sql_exp *rs = m->data;
				if (r_nil)
					set_has_nil(rs);
				append(outexps, rs);
			}
			rel = rel_project(sql->sa, rel, outexps);
		}
	} else if (js) {	/* using */
		char rname[16], *rnme;
		dnode *n = js->data.lval->h;
		list *outexps = new_exp_list(sql->sa), *exps;
		node *m;

		rnme = number2name(rname, 16, ++sql->label);
		for (; n; n = n->next) {
			char *nm = n->data.sval;
			sql_exp *cond;
			sql_exp *ls = rel_bind_column(sql, t1, nm, sql_where);
			sql_exp *rs = rel_bind_column(sql, t2, nm, sql_where);

			if (!ls || !rs) {
				sql_error(sql, 02, "JOIN: tables '%s' and '%s' do not have a matching column '%s'\n", rel_get_name(t1)?rel_get_name(t1):"", rel_get_name(t2)?rel_get_name(t2):"", nm);
				rel_destroy(rel);
				return NULL;
			}
			rel = rel_compare_exp(sql, rel, ls, rs, "=", NULL, TRUE);
			cond = rel_unop_(sql, ls, NULL, "isnull", card_value);
			ls = rel_nop_(sql, cond, rs, ls, NULL, NULL, "ifthenelse", card_value);
			exp_setname(sql->sa, ls, rnme, nm);
			append(outexps, ls);
			if (!rel) 
				return NULL;
		}
		exps = rel_projections(sql, t1, NULL, 1, 1);
		for (m = exps->h; m; m = m->next) {
			char *nm = exp_name(m->data);
			int fnd = 0;

			for (n = js->data.lval->h; n; n = n->next) {
				if (strcmp(nm, n->data.sval) == 0) {
					fnd = 1;
					break;
				}
			}
			if (!fnd) {
				sql_exp *ls = m->data;
				if (l_nil)
					set_has_nil(ls);
				append(outexps, ls);
			}
		}
		exps = rel_projections(sql, t2, NULL, 1, 1);
		for (m = exps->h; m; m = m->next) {
			char *nm = exp_name(m->data);
			int fnd = 0;

			for (n = js->data.lval->h; n; n = n->next) {
				if (strcmp(nm, n->data.sval) == 0) {
					fnd = 1;
					break;
				}
			}
			if (!fnd) {
				sql_exp *rs = m->data;
				if (r_nil)
					set_has_nil(rs);
				append(outexps, rs);
			}
		}
		rel = rel_project(sql->sa, rel, outexps);
	} else {		/* ! js -> natural join */
		rel = join_on_column_name(sql, rel, t1, t2, op, l_nil, r_nil);
	}
	if (!rel)
		return NULL;
	inner->op = op;
	set_processed(rel);
	return rel;
}

static sql_rel *
rel_joinquery(mvc *sql, sql_rel *rel, symbol *q)
{

	dnode *n = q->data.lval->h;
	symbol *tab_ref1 = n->data.sym;
	int natural = n->next->data.i_val;
	jt jointype = (jt) n->next->next->data.i_val;
	symbol *tab_ref2 = n->next->next->next->data.sym;
	symbol *joinspec = n->next->next->next->next->data.sym;

	assert(n->next->type == type_int);
	assert(n->next->next->type == type_int);
	return rel_joinquery_(sql, rel, tab_ref1, natural, jointype, tab_ref2, joinspec);
}

static sql_rel *
rel_crossquery(mvc *sql, sql_rel *rel, symbol *q)
{
	dnode *n = q->data.lval->h;
	symbol *tab1 = n->data.sym;
	symbol *tab2 = n->next->data.sym;
	sql_rel *t1 = table_ref(sql, rel, tab1);
	sql_rel *t2 = table_ref(sql, rel, tab2);

	if (!t1 || !t2)
		return NULL;

	rel = rel_crossproduct(sql->sa, t1, t2, op_join);
	return rel;
}
	
static sql_rel *
rel_unionjoinquery(mvc *sql, sql_rel *rel, symbol *q)
{
	dnode *n = q->data.lval->h;
	sql_rel *lv = table_ref(sql, rel, n->data.sym);
	sql_rel *rv = table_ref(sql, rel, n->next->next->data.sym);
	int all = n->next->data.i_val;
	list *lexps, *rexps;
	node *m;
	int found = 0;

	assert(n->next->type == type_int);
	if (!lv || !rv)
		return NULL;

	lexps = rel_projections(sql, lv, NULL, 1, 1);
	/* find the matching columns (all should match?)
	 * union these
	 * if !all do a distinct operation at the end
	 */
	/* join all result columns ie join(lh,rh) on column_name */
	rexps = new_exp_list(sql->sa);
	for (m = lexps->h; m; m = m->next) {
		sql_exp *le = m->data;
		sql_exp *rc = rel_bind_column(sql, rv, le->name, sql_where);
			
		if (!rc && all)
			break;
		if (rc) {
			found = 1;
			append(rexps, rc);
		}
	}
	if (!found) {
		rel_destroy(rel);
		return NULL;
	}
	lv = rel_project(sql->sa, lv, lexps);
	rv = rel_project(sql->sa, rv, rexps);
	set_processed(lv);
	set_processed(rv);
	rel = rel_setop(sql->sa, lv, rv, op_union);
	rel->exps = rel_projections(sql, rel, NULL, 0, 1);
	if (!all)
		rel = rel_distinct(rel);
	return rel;
}

sql_rel *
rel_subquery(mvc *sql, sql_rel *rel, symbol *sq, exp_kind ek)
{
	int toplevel = 0;

	if (!rel || (rel->op == op_project &&
		(!rel->exps || list_length(rel->exps) == 0)))
		toplevel = 1;

	return rel_query(sql, rel, sq, toplevel, ek);
}

sql_rel *
rel_selects(mvc *sql, symbol *s)
{
	sql_rel *ret = NULL;

	switch (s->token) {
	case SQL_SELECT: {
		exp_kind ek = {type_value, card_relation, TRUE};
		ret = rel_subquery(sql, NULL, s, ek);
		sql->type = Q_TABLE;
	}	break;
	case SQL_JOIN:
		ret = rel_joinquery(sql, NULL, s);
		sql->type = Q_TABLE;
		break;
	case SQL_CROSS:
		ret = rel_crossquery(sql, NULL, s);
		sql->type = Q_TABLE;
		break;
	case SQL_UNION:
	case SQL_EXCEPT:
	case SQL_INTERSECT:
		ret = rel_setquery(sql, NULL, s);
		sql->type = Q_TABLE;
		break;
	default:
		return NULL;
	}
	if (mvc_debug_on(sql,32768)) {
		rel_print(sql, ret, 0);
		printf("\n");
	}
	if (!ret && sql->errstr[0] == 0)
		(void) sql_error(sql, 02, "relational query without result");
	return ret;
}
