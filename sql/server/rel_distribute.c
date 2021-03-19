/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0.  If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 *
 * Copyright 1997 - July 2008 CWI, August 2008 - 2021 MonetDB B.V.
 */

#include "monetdb_config.h"
#include "rel_distribute.h"
#include "rel_rel.h"
#include "rel_basetable.h"
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
		if (IS_TABLE_PROD_FUNC(rel->flag) || rel->flag == TABLE_FROM_RELATION)
			if (has_remote_or_replica( rel->l ))
				return 1;
		break;
	case op_join:
	case op_left:
	case op_right:
	case op_full:

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
	case op_truncate:
		if (has_remote_or_replica( rel->l ))
			return 1;
		break;
	case op_ddl:
		if (rel->flag == ddl_output || rel->flag == ddl_create_seq || rel->flag == ddl_alter_seq /*|| rel->flag == ddl_alter_table || rel->flag == ddl_create_table || rel->flag == ddl_create_view*/) {
			if (has_remote_or_replica( rel->l ))
				return 1;
		} else if (rel->flag == ddl_list || rel->flag == ddl_exception) {
			if (has_remote_or_replica( rel->l ) ||
				has_remote_or_replica( rel->r ))
			return 1;
		}
		break;
	case op_insert:
	case op_update:
	case op_delete:
		if (has_remote_or_replica( rel->l ) ||
			has_remote_or_replica( rel->r ))
			return 1;
		break;
	}
	return 0;
}

static sql_rel *
rewrite_replica( mvc *sql, sql_rel *rel, sql_table *t, sql_part *pd, int remote_prop)
{
	node *n, *m;
	sql_table *p = find_sql_table_id(sql->session->tr, t->s, pd->member);
	sql_rel *r = rel_basetable(sql, p, t->base.name);

	for (n = rel->exps->h; n; n = n->next) {
		sql_exp *e = n->data;

		node *n = ol_find_name(t->columns, exp_name(e));
		if (n) {
			sql_column *c = n->data;

			rel_base_use(sql, r, c->colnr);
		} else if (strcmp(exp_name(e), TID) == 0) {
			rel_base_use_tid(sql, r);
		} else {
			assert(0);
		}
	}
	rel = rewrite_basetable(sql, r);
	for (n = rel->exps->h, m = r->exps->h; n && m; n = n->next, m = m->next) {
		sql_exp *e = n->data;
		sql_exp *ne = m->data;

		exp_prop_alias(sql->sa, ne, e);
	}
	rel_destroy(rel);

	/* set_remote() */
	if (remote_prop && p && isRemote(p)) {
		char *local_name = sa_strconcat(sql->sa, sa_strconcat(sql->sa, p->s->base.name, "."), p->base.name);
		if (!local_name) {
			return NULL;
		}
		prop *p = r->p = prop_create(sql->sa, PROP_REMOTE, r->p);
		if (!p) {
			return NULL;
		}

		p->value = local_name;
	}
	return r;
}

static list * exps_replica(mvc *sql, list *exps, char *uri) ;
static sql_rel * replica(mvc *sql, sql_rel *rel, char *uri);

static sql_exp *
exp_replica(mvc *sql, sql_exp *e, char *uri)
{
	switch(e->type) {
	case e_column:
		break;
	case e_atom:
		if (e->f)
			e->f = exps_replica(sql, e->f, uri);
		break;
	case e_convert:
		e->l = exp_replica(sql, e->l, uri);
		break;
	case e_aggr:
	case e_func:
		e->l = exps_replica(sql, e->l, uri);
		e->r = exps_replica(sql, e->r, uri);
		break;
	case e_cmp:
		if (e->flag == cmp_or || e->flag == cmp_filter) {
			e->l = exps_replica(sql, e->l, uri);
			e->r = exps_replica(sql, e->r, uri);
		} else if (e->flag == cmp_in || e->flag == cmp_notin) {
			e->l = exp_replica(sql, e->l, uri);
			e->r = exps_replica(sql, e->r, uri);
		} else {
			e->l = exp_replica(sql, e->l, uri);
			e->r = exps_replica(sql, e->r, uri);
			if (e->f)
				e->f = exps_replica(sql, e->f, uri);
		}
		break;
	case e_psm:
		if (e->flag & PSM_SET || e->flag & PSM_RETURN || e->flag & PSM_EXCEPTION) {
			e->l = exp_replica(sql, e->l, uri);
		} else if (e->flag & PSM_WHILE || e->flag & PSM_IF) {
			e->l = exp_replica(sql, e->l, uri);
			e->r = exps_replica(sql, e->r, uri);
			if (e->f)
				e->f = exps_replica(sql, e->f, uri);
		} else if (e->flag & PSM_REL) {
			e->l = replica(sql, e->l, uri);
		}
		break;
	}
	return e;
}

static list *
exps_replica(mvc *sql, list *exps, char *uri)
{
	node *n;

	if (!exps)
		return exps;
	for( n = exps->h; n; n = n->next)
		n->data = exp_replica(sql, n->data, uri);
	return exps;
}

static sql_rel *
replica(mvc *sql, sql_rel *rel, char *uri)
{
	if (!rel)
		return rel;

	if (rel_is_ref(rel)) {
		if (has_remote_or_replica(rel)) {
			sql_rel *nrel = rel_copy(sql, rel, 1);

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
				for (n = t->members->h; n; n = n->next) {
					sql_part *p = n->data;
					sql_table *pt = find_sql_table_id(sql->session->tr, t->s, p->member);

					if (isRemote(pt) && strcmp(uri, pt->query) == 0) {
						rel = rewrite_replica(sql, rel, t, p, 0);
						break;
					}
				}
			} else { /* no match, find one without remote or use first */
				if (t->members) {
					int fnd = 0;
					sql_part *p;
					for (n = t->members->h; n; n = n->next) {
						sql_part *p = n->data;
						sql_table *pt = find_sql_table_id(sql->session->tr, t->s, p->member);

						if (!isRemote(pt)) {
							fnd = 1;
							rel = rewrite_replica(sql, rel, t, p, 0);
							break;
						}
					}
					if (!fnd) {
						p = t->members->h->data;
						rel = rewrite_replica(sql, rel, t, p, 1);
					}
				} else {
					rel = NULL;
				}
			}
		}
	} break;
	case op_table:
		if (IS_TABLE_PROD_FUNC(rel->flag) || rel->flag == TABLE_FROM_RELATION)
			rel->l = replica(sql, rel->l, uri);
		break;
	case op_join:
	case op_left:
	case op_right:
	case op_full:

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
	case op_truncate:
		rel->l = replica(sql, rel->l, uri);
		break;
	case op_ddl:
		if ((rel->flag == ddl_psm || rel->flag == ddl_exception) && rel->exps)
			rel->exps = exps_replica(sql, rel->exps, uri);
		if (rel->flag == ddl_output || rel->flag == ddl_create_seq || rel->flag == ddl_alter_seq /*|| rel->flag == ddl_alter_table || rel->flag == ddl_create_table || rel->flag == ddl_create_view*/) {
			rel->l = replica(sql, rel->l, uri);
		} else if (rel->flag == ddl_list || rel->flag == ddl_exception) {
			rel->l = replica(sql, rel->l, uri);
			rel->r = replica(sql, rel->r, uri);
		}
		break;
	case op_insert:
	case op_update:
	case op_delete:
		rel->l = replica(sql, rel->l, uri);
		rel->r = replica(sql, rel->r, uri);
		break;
	}
	return rel;
}

static list * exps_distribute(mvc *sql, list *exps) ;
static sql_rel * distribute(mvc *sql, sql_rel *rel);

static sql_exp *
exp_distribute(mvc *sql, sql_exp *e)
{
	switch(e->type) {
	case e_column:
		break;
	case e_atom:
		if (e->f)
			e->f = exps_distribute(sql, e->f);
		break;
	case e_convert:
		e->l = exp_distribute(sql, e->l);
		break;
	case e_aggr:
	case e_func:
		e->l = exps_distribute(sql, e->l);
		e->r = exps_distribute(sql, e->r);
		break;
	case e_cmp:
		if (e->flag == cmp_or || e->flag == cmp_filter) {
			e->l = exps_distribute(sql, e->l);
			e->r = exps_distribute(sql, e->r);
		} else if (e->flag == cmp_in || e->flag == cmp_notin) {
			e->l = exp_distribute(sql, e->l);
			e->r = exps_distribute(sql, e->r);
		} else {
			e->l = exp_distribute(sql, e->l);
			e->r = exps_distribute(sql, e->r);
			if (e->f)
				e->f = exps_distribute(sql, e->f);
		}
		break;
	case e_psm:
		if (e->flag & PSM_SET || e->flag & PSM_RETURN || e->flag & PSM_EXCEPTION) {
			e->l = exp_distribute(sql, e->l);
		} else if (e->flag & PSM_WHILE || e->flag & PSM_IF) {
			e->l = exp_distribute(sql, e->l);
			e->r = exps_distribute(sql, e->r);
			if (e->f)
				e->f = exps_distribute(sql, e->f);
		} else if (e->flag & PSM_REL) {
			e->l = distribute(sql, e->l);
		}
		break;
	}
	return e;
}

static list *
exps_distribute(mvc *sql, list *exps)
{
	node *n;

	if (!exps)
		return exps;
	for( n = exps->h; n; n = n->next)
		n->data = exp_distribute(sql, n->data);
	return exps;
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
			sql_rel *nrel = rel_copy(sql, rel, 1);

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
			//TODO: check for allocation failure
			char *local_name = sa_strconcat(sql->sa, sa_strconcat(sql->sa, t->s->base.name, "."), t->base.name);
			if (!local_name)
				return NULL;

			p = rel->p = prop_create(sql->sa, PROP_REMOTE, rel->p);
			if (!p)
				return NULL;
			p->value = local_name;
		}
	} break;
	case op_table:
		if (IS_TABLE_PROD_FUNC(rel->flag) || rel->flag == TABLE_FROM_RELATION) {
			l = rel->l = distribute(sql, rel->l);

			if (l && (p = find_prop(l->p, PROP_REMOTE)) != NULL) {
				l->p = prop_remove(l->p, p);
				if (!find_prop(rel->p, PROP_REMOTE)) {
					p->p = rel->p;
					rel->p = p;
				}
			}
		} break;
	case op_join:
	case op_left:
	case op_right:
	case op_full:

	case op_semi:
	case op_anti:

	case op_union:
	case op_inter:
	case op_except:
		l = rel->l = distribute(sql, rel->l);
		r = rel->r = distribute(sql, rel->r);

		if (is_join(rel->op) && list_empty(rel->exps) &&
			find_prop(l->p, PROP_REMOTE) == NULL &&
			find_prop(r->p, PROP_REMOTE) == NULL) {
			/* cleanup replica's */
			l = rel->l = replica(sql, l, NULL);
			r = rel->r = replica(sql, r, NULL);
		}
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
			if (!find_prop(rel->p, PROP_REMOTE)) {
				pl->p = rel->p;
				rel->p = pl;
			}
		}
		break;
	case op_project:
	case op_select:
	case op_groupby:
	case op_topn:
	case op_sample:
		l = rel->l = distribute(sql, rel->l);

		if (l && (p = find_prop(l->p, PROP_REMOTE)) != NULL) {
			l->p = prop_remove(l->p, p);
			if (!find_prop(rel->p, PROP_REMOTE)) {
				p->p = rel->p;
				rel->p = p;
			}
		}
		break;
	case op_ddl:
		if ((rel->flag == ddl_psm || rel->flag == ddl_exception) && rel->exps)
			rel->exps = exps_distribute(sql, rel->exps);
		if (rel->flag == ddl_output || rel->flag == ddl_create_seq || rel->flag == ddl_alter_seq /*|| rel->flag == ddl_alter_table || rel->flag == ddl_create_table || rel->flag == ddl_create_view*/) {
			l = rel->l = distribute(sql, rel->l);

			if (l && (p = find_prop(l->p, PROP_REMOTE)) != NULL) {
				l->p = prop_remove(l->p, p);
				if (!find_prop(rel->p, PROP_REMOTE)) {
					p->p = rel->p;
					rel->p = p;
				}
			}
		} else if (rel->flag == ddl_list || rel->flag == ddl_exception) {
			l = rel->l = distribute(sql, rel->l);
			r = rel->r = distribute(sql, rel->r);

			if (l && (pl = find_prop(l->p, PROP_REMOTE)) != NULL &&
				r && (pr = find_prop(r->p, PROP_REMOTE)) != NULL &&
				strcmp(pl->value, pr->value) == 0) {
				l->p = prop_remove(l->p, pl);
				r->p = prop_remove(r->p, pr);
				if (!find_prop(rel->p, PROP_REMOTE)) {
					pl->p = rel->p;
					rel->p = pl;
				}
			}
		}
		break;
	case op_insert:
	case op_update:
	case op_delete:
		l = rel->l = distribute(sql, rel->l);
		r = rel->r = distribute(sql, rel->r);

		if (l && (pl = find_prop(l->p, PROP_REMOTE)) != NULL &&
			r && (pr = find_prop(r->p, PROP_REMOTE)) != NULL &&
			strcmp(pl->value, pr->value) == 0) {
			l->p = prop_remove(l->p, pl);
			r->p = prop_remove(r->p, pr);
			if (!find_prop(rel->p, PROP_REMOTE)) {
				pl->p = rel->p;
				rel->p = pl;
			}
		}
		break;
	case op_truncate:
		l = rel->l = distribute(sql, rel->l);

		if (l && (p = find_prop(l->p, PROP_REMOTE)) != NULL) {
			l->p = prop_remove(l->p, p);
			if (!find_prop(rel->p, PROP_REMOTE)) {
				p->p = rel->p;
				rel->p = p;
			}
		}
		break;
	}
	return rel;
}

static list * exps_remote_func(mvc *sql, list *exps) ;
static sql_rel * rel_remote_func(mvc *sql, sql_rel *rel);

static sql_exp *
exp_remote_func(mvc *sql, sql_exp *e)
{
	switch(e->type) {
	case e_column:
		break;
	case e_atom:
		if (e->f)
			e->f = exps_remote_func(sql, e->f);
		break;
	case e_convert:
		e->l = exp_remote_func(sql, e->l);
		break;
	case e_aggr:
	case e_func:
		e->l = exps_remote_func(sql, e->l);
		e->r = exps_remote_func(sql, e->r);
		break;
	case e_cmp:
		if (e->flag == cmp_or || e->flag == cmp_filter) {
			e->l = exps_remote_func(sql, e->l);
			e->r = exps_remote_func(sql, e->r);
		} else if (e->flag == cmp_in || e->flag == cmp_notin) {
			e->l = exp_remote_func(sql, e->l);
			e->r = exps_remote_func(sql, e->r);
		} else {
			e->l = exp_remote_func(sql, e->l);
			e->r = exps_remote_func(sql, e->r);
			if (e->f)
				e->f = exps_remote_func(sql, e->f);
		}
		break;
	case e_psm:
		if (e->flag & PSM_SET || e->flag & PSM_RETURN)
			e->l = exp_remote_func(sql, e->l);
		else if (e->flag & PSM_WHILE || e->flag & PSM_IF) {
			e->l = exp_remote_func(sql, e->l);
			e->r = exps_remote_func(sql, e->r);
			if (e->f)
				e->f = exps_remote_func(sql, e->f);
		} else if (e->flag & PSM_REL)
			e->l = rel_remote_func(sql, e->l);
		else if (e->flag & PSM_EXCEPTION)
			e->l = exp_remote_func(sql, e->l);
		break;
	}
	return e;
}

static list *
exps_remote_func(mvc *sql, list *exps)
{
	node *n;

	if (!exps)
		return exps;
	for( n = exps->h; n; n = n->next)
		n->data = exp_remote_func(sql, n->data);
	return exps;
}

static sql_rel *
rel_remote_func(mvc *sql, sql_rel *rel)
{
	if (!rel)
		return rel;

	switch (rel->op) {
	case op_basetable:
	case op_truncate:
		break;
	case op_table:
		if (IS_TABLE_PROD_FUNC(rel->flag) || rel->flag == TABLE_FROM_RELATION)
			rel->l = rel_remote_func(sql, rel->l);
		break;
	case op_join:
	case op_left:
	case op_right:
	case op_full:

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
		if ((rel->flag == ddl_psm || rel->flag == ddl_exception) && rel->exps)
			rel->exps = exps_remote_func(sql, rel->exps);
		if (rel->flag == ddl_output || rel->flag == ddl_create_seq || rel->flag == ddl_alter_seq /*|| rel->flag == ddl_alter_table || rel->flag == ddl_create_table || rel->flag == ddl_create_view*/) {
			rel->l = rel_remote_func(sql, rel->l);
		} else if (rel->flag == ddl_list || rel->flag == ddl_exception) {
			rel->l = rel_remote_func(sql, rel->l);
			rel->r = rel_remote_func(sql, rel->r);
		}
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
