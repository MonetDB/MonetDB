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

		return t && (isReplicaTable(t) || isRemote(t));
	}
	case op_table:
		if (IS_TABLE_PROD_FUNC(rel->flag) || rel->flag == TABLE_FROM_RELATION)
			return has_remote_or_replica( rel->l );
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
	case op_merge:

	case op_insert:
	case op_update:
	case op_delete:
		return has_remote_or_replica( rel->l ) || has_remote_or_replica( rel->r );
	case op_project:
	case op_select:
	case op_groupby:
	case op_topn:
	case op_sample:
	case op_truncate:
		return has_remote_or_replica( rel->l );
	case op_ddl:
		if (rel->flag == ddl_output || rel->flag == ddl_create_seq || rel->flag == ddl_alter_seq /*|| rel->flag == ddl_alter_table || rel->flag == ddl_create_table || rel->flag == ddl_create_view*/)
			return has_remote_or_replica( rel->l );
		if (rel->flag == ddl_list || rel->flag == ddl_exception)
			return has_remote_or_replica( rel->l ) || has_remote_or_replica( rel->r );
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
		prop *p = r->p = prop_create(sql->sa, PROP_REMOTE, r->p);
		p->value = local_name;
	}
	return r;
}

static sql_rel *
replica(visitor *v, sql_rel *rel)
{
	const char *uri = v->data;
 
	if (rel_is_ref(rel)) {
		if (has_remote_or_replica(rel)) {
			sql_rel *nrel = rel_copy(v->sql, rel, 1);

			rel_destroy(rel);
			rel = nrel;
		} else {
			return rel;
		}
	}
	if (is_basetable(rel->op)) {
		sql_table *t = rel->l;

		if (t && isReplicaTable(t) && !list_empty(t->members)) {
			if (uri) {
				/* replace by the replica which matches the uri */
				for (node *n = t->members->h; n; n = n->next) {
					sql_part *p = n->data;
					sql_table *pt = find_sql_table_id(v->sql->session->tr, t->s, p->member);

					if (isRemote(pt) && strcmp(uri, pt->query) == 0) {
						rel = rewrite_replica(v->sql, rel, t, p, 0);
						break;
					}
				}
			} else { /* no match, find one without remote or use first */
				int fnd = 0;
				sql_part *p;
				for (node *n = t->members->h; n; n = n->next) {
					sql_part *p = n->data;
					sql_table *pt = find_sql_table_id(v->sql->session->tr, t->s, p->member);

					if (!isRemote(pt)) {
						fnd = 1;
						rel = rewrite_replica(v->sql, rel, t, p, 0);
						break;
					}
				}
				if (!fnd) {
					p = t->members->h->data;
					rel = rewrite_replica(v->sql, rel, t, p, 1);
				}
			}
		}
	}
	return rel;
}

static sql_rel *
distribute(visitor *v, sql_rel *rel)
{
	sql_rel *l = rel->l, *r = rel->r;
	prop *p, *pl, *pr;

	if (rel_is_ref(rel)) {
		if (has_remote_or_replica(rel)) {
			sql_rel *nrel = rel_copy(v->sql, rel, 1);

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
			char *local_name = sa_strconcat(v->sql->sa, sa_strconcat(v->sql->sa, t->s->base.name, "."), t->base.name);
			p = rel->p = prop_create(v->sql->sa, PROP_REMOTE, rel->p);
			p->value = local_name;
		}
	} break;
	case op_table:
		if (IS_TABLE_PROD_FUNC(rel->flag) || rel->flag == TABLE_FROM_RELATION) {
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

	case op_insert:
	case op_update:
	case op_delete:
	case op_merge:
		if (is_join(rel->op) && list_empty(rel->exps) &&
			find_prop(l->p, PROP_REMOTE) == NULL &&
			find_prop(r->p, PROP_REMOTE) == NULL) {
			/* cleanup replica's */
			visitor rv = { .sql = v->sql };

			l = rel->l = rel_visitor_bottomup(&rv, l, &replica);
			rv.data = NULL;
			r = rel->r = rel_visitor_bottomup(&rv, r, &replica);
		}
		if ((is_join(rel->op) || is_semi(rel->op) || is_set(rel->op)) &&
			l && (pl = find_prop(l->p, PROP_REMOTE)) != NULL &&
			r && find_prop(r->p, PROP_REMOTE) == NULL) {
			visitor rv = { .sql = v->sql, .data = pl->value };

			r = rel_visitor_bottomup(&rv, r, &replica);
			rv.data = NULL;
			r = rel->r = rel_visitor_bottomup(&rv, l, &distribute);
		} else if ((is_join(rel->op) || is_semi(rel->op) || is_set(rel->op)) &&
			l && find_prop(l->p, PROP_REMOTE) == NULL &&
			r && (pr = find_prop(r->p, PROP_REMOTE)) != NULL) {
			visitor rv = { .sql = v->sql, .data = pr->value };

			l = rel_visitor_bottomup(&rv, l, &replica);
			rv.data = NULL;
			l = rel->l = rel_visitor_bottomup(&rv, l, &distribute);
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
	case op_truncate:
		if (l && (p = find_prop(l->p, PROP_REMOTE)) != NULL) {
			l->p = prop_remove(l->p, p);
			if (!find_prop(rel->p, PROP_REMOTE)) {
				p->p = rel->p;
				rel->p = p;
			}
		}
		break;
	case op_ddl:
		if (rel->flag == ddl_output || rel->flag == ddl_create_seq || rel->flag == ddl_alter_seq /*|| rel->flag == ddl_alter_table || rel->flag == ddl_create_table || rel->flag == ddl_create_view*/) {
			if (l && (p = find_prop(l->p, PROP_REMOTE)) != NULL) {
				l->p = prop_remove(l->p, p);
				if (!find_prop(rel->p, PROP_REMOTE)) {
					p->p = rel->p;
					rel->p = p;
				}
			}
		} else if (rel->flag == ddl_list || rel->flag == ddl_exception) {
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
	}
	return rel;
}

static sql_rel *
rel_remote_func(visitor *v, sql_rel *rel)
{
	(void) v;

	if (find_prop(rel->p, PROP_REMOTE) != NULL) {
		list *exps = rel_projections(v->sql, rel, NULL, 1, 1);
		rel = rel_relational_func(v->sql->sa, rel, exps);
	}
	return rel;
}

sql_rel *
rel_distribute(mvc *sql, sql_rel *rel)
{
	visitor v = { .sql = sql };

	rel = rel_visitor_bottomup(&v, rel, &distribute);
	v.data = NULL;
	rel = rel_visitor_bottomup(&v, rel, &replica);
	rel = rel_visitor_bottomup(&v, rel, &rel_remote_func);
	return rel;
}
