/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0.  If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 *
 * Copyright 1997 - July 2008 CWI, August 2008 - 2017 MonetDB B.V.
 */

/*#define DEBUG*/

#include "monetdb_config.h"
#include "rel_distribute.h"
#include "rel_rel.h"
#include "rel_exp.h"
#include "rel_prop.h"
#include "rel_dump.h"

static int 
has_remote_or_replica( sql_rel *rel ) 
{
	if (!rel)
		return 0;

	switch (rel->op) {
	case op_basetable: {
		sql_table *t = rel->l;

		if (t && (isReplicaTable(t) || isRemote(t))) 
			return 1;
		break;
	}
	case op_table:
		break;
	case op_join: 
	case op_left: 
	case op_right: 
	case op_full: 

	case op_apply: 
	case op_semi: 
	case op_anti: 

	case op_union: 
	case op_inter: 
	case op_except: 
		if (has_remote_or_replica( rel->l ) ||
		    has_remote_or_replica( rel->r ))
			return 1;
		break;
	case op_project:
	case op_select: 
	case op_groupby: 
	case op_topn: 
	case op_sample: 
		if (has_remote_or_replica( rel->l )) 
			return 1;
		break;
	case op_ddl: 
		if (has_remote_or_replica( rel->l )) 
			return 1;
		/* fall through */
	case op_insert:
	case op_update:
	case op_delete:
		if (rel->r && has_remote_or_replica( rel->r )) 
			return 1;
		break;
	}
	return 0;
}

static sql_rel *
rewrite_replica( mvc *sql, sql_rel *rel, sql_table *t, sql_table *p, int remote_prop)
{
	node *n, *m;
	sql_rel *r = rel_basetable(sql, p, t->base.name);

	for (n = rel->exps->h, m = r->exps->h; n && m; n = n->next, m = m->next) {
		sql_exp *e = n->data;
		sql_exp *ne = m->data;

		exp_setname(sql->sa, ne, e->rname, e->name);
	}
	rel_destroy(rel);

	/* set_remote() */
	if (remote_prop && p && isRemote(p)) {
		char *uri = p->query;
		prop *p = r->p = prop_create(sql->sa, PROP_REMOTE, r->p); 

		p->value = uri;
	}
	return r;
}

static sql_rel *
replica(mvc *sql, sql_rel *rel, char *uri) 
{
	if (!rel)
		return rel;

	if (rel_is_ref(rel)) {
		if (has_remote_or_replica(rel)) {
			sql_rel *nrel = rel_copy(sql->sa, rel);

			if (nrel && rel->p)
				nrel->p = prop_copy(sql->sa, rel->p);
			rel_destroy(rel);
			rel = nrel;
		} else {
			return rel;
		}
	}
	switch (rel->op) {
	case op_basetable: {
		sql_table *t = rel->l;

		if (t && isReplicaTable(t)) {
			node *n;

			if (uri) {
				/* replace by the replica which matches the uri */
				for (n = t->tables.set->h; n; n = n->next) {
					sql_table *p = n->data;
	
					if (isRemote(p) && strcmp(uri, p->query) == 0) {
						rel = rewrite_replica(sql, rel, t, p, 0);
						break;
					}
				}
			} else { /* no match, use first */
				sql_table *p = NULL;

				if (t->tables.set) {
					p = t->tables.set->h->data;
					rel = rewrite_replica(sql, rel, t, p, 1);
				} else {
					rel = NULL;
				}
			}
		}
		break;
	}
	case op_table:
		break;
	case op_join: 
	case op_left: 
	case op_right: 
	case op_full: 

	case op_apply: 
	case op_semi: 
	case op_anti: 

	case op_union: 
	case op_inter: 
	case op_except: 
		rel->l = replica(sql, rel->l, uri);
		rel->r = replica(sql, rel->r, uri);
		break;
	case op_project:
	case op_select: 
	case op_groupby: 
	case op_topn: 
	case op_sample: 
		rel->l = replica(sql, rel->l, uri);
		break;
	case op_ddl: 
		rel->l = replica(sql, rel->l, uri);
		if (rel->r)
			rel->r = replica(sql, rel->r, uri);
		break;
	case op_insert:
	case op_update:
	case op_delete:
		rel->r = replica(sql, rel->r, uri);
		break;
	}
	return rel;
}

static sql_rel *
distribute(mvc *sql, sql_rel *rel) 
{
	sql_rel *l = NULL, *r = NULL;
	prop *p, *pl, *pr;

	if (!rel)
		return rel;

	if (rel_is_ref(rel)) {
		if (has_remote_or_replica(rel)) {
			sql_rel *nrel = rel_copy(sql->sa, rel);

			if (nrel && rel->p)
				nrel->p = prop_copy(sql->sa, rel->p);
			rel_destroy(rel);
			rel = nrel;
		} else {
			return rel;
		}
	}

	switch (rel->op) {
	case op_basetable: {
		sql_table *t = rel->l;

		/* set_remote() */
		if (t && isRemote(t)) {
			char *uri = t->query;

			p = rel->p = prop_create(sql->sa, PROP_REMOTE, rel->p); 
			p->value = uri;
		}
		break;
	}
	case op_table:
		break;
	case op_join: 
	case op_left: 
	case op_right: 
	case op_full: 

	case op_apply: 
	case op_semi: 
	case op_anti: 

	case op_union: 
	case op_inter: 
	case op_except: 
		l = rel->l = distribute(sql, rel->l);
		r = rel->r = distribute(sql, rel->r);

		if (l && (pl = find_prop(l->p, PROP_REMOTE)) != NULL &&
		    r && find_prop(r->p, PROP_REMOTE) == NULL) {
			r = rel->r = distribute(sql, replica(sql, rel->r, pl->value));
		} else if (l && find_prop(l->p, PROP_REMOTE) == NULL &&
		    	   r && (pr = find_prop(r->p, PROP_REMOTE)) != NULL) {
			l = rel->l = distribute(sql, replica(sql, rel->l, pr->value));
		}
		if (l && (pl = find_prop(l->p, PROP_REMOTE)) != NULL &&
		    r && (pr = find_prop(r->p, PROP_REMOTE)) != NULL && 
		    strcmp(pl->value, pr->value) == 0) {
			l->p = prop_remove(l->p, pl);
			r->p = prop_remove(r->p, pr);
			pl->p = rel->p;
			rel->p = pl;
		}
		break;
	case op_project:
	case op_select: 
	case op_groupby: 
	case op_topn: 
	case op_sample: 
		rel->l = distribute(sql, rel->l);
		l = rel->l;
		if (l && (p = find_prop(l->p, PROP_REMOTE)) != NULL) {
			l->p = prop_remove(l->p, p);
			p->p = rel->p;
			rel->p = p;
		}
		break;
	case op_ddl: 
		rel->l = distribute(sql, rel->l);
		if (rel->r)
			rel->r = distribute(sql, rel->r);
		break;
	case op_insert:
	case op_update:
	case op_delete:
		rel->r = distribute(sql, rel->r);
		break;
	}
	return rel;
}

static sql_rel *
rel_remote_func(mvc *sql, sql_rel *rel)
{
	if (!rel)
		return rel;

	switch (rel->op) {
	case op_basetable: 
	case op_table:
		break;
	case op_join: 
	case op_left: 
	case op_right: 
	case op_full: 

	case op_apply: 
	case op_semi: 
	case op_anti: 

	case op_union: 
	case op_inter: 
	case op_except: 
		rel->l = rel_remote_func(sql, rel->l);
		rel->r = rel_remote_func(sql, rel->r);
		break;
	case op_project:
	case op_select: 
	case op_groupby: 
	case op_topn: 
	case op_sample: 
		rel->l = rel_remote_func(sql, rel->l);
		break;
	case op_ddl: 
		rel->l = rel_remote_func(sql, rel->l);
		if (rel->r)
			rel->r = rel_remote_func(sql, rel->r);
		break;
	case op_insert:
	case op_update:
	case op_delete:
		rel->r = rel_remote_func(sql, rel->r);
		break;
	}
	if (find_prop(rel->p, PROP_REMOTE) != NULL) {
		list *exps = rel_projections(sql, rel, NULL, 1, 1);
		rel = rel_relational_func(sql->sa, rel, exps);
	}
	return rel;
}

sql_rel *
rel_distribute(mvc *sql, sql_rel *rel) 
{
	rel = distribute(sql, rel);
	rel = replica(sql, rel, NULL);
	return rel_remote_func(sql, rel);
}
