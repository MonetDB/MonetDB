/*
 * SPDX-License-Identifier: MPL-2.0
 *
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0.  If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 *
 * Copyright 2024 MonetDB Foundation;
 * Copyright August 2008 - 2023 MonetDB B.V.;
 * Copyright 1997 - July 2008 CWI.
 */

#include "monetdb_config.h"
#include "rel_optimizer_private.h"
#include "rel_basetable.h"
#include "rel_exp.h"
#include "rel_remote.h"
#include "sql_privileges.h"

typedef struct rmt_prop_state {
	int depth;
	prop* rmt;
	sql_rel* orig;
	bool no_rmt_branch_rpl_leaf;
} rps;

static sql_rel*
rel_unique_exps(mvc *sql, sql_rel *rel)
{
	list *l;

	if (!is_project(rel->op))
		return rel;
	l = sa_list(sql->sa);
	for (node *n = rel->exps->h; n; n = n->next) {
		sql_exp *e = n->data;
		if (e->type == e_column) {
			const char *name = exp_name(e);
			const char *rname = exp_relname(e);

			/* If there are two identical expression names, there will be ambiguity */
			if (name && rname && exps_bind_column2(l, rname, name, NULL))
				exp_label(sql->sa, e, ++sql->label);
		}
		append(l,e);
	}
	rel->exps = l;
	return rel;
}

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
do_replica_rewrite(mvc *sql, list *exps, sql_table *t, sql_table *p, int remote_prop)
{
	node *n, *m;
	sql_rel *r = rel_basetable(sql, p, t->base.name);

	for (n = exps->h; n; n = n->next) {
		sql_exp *e = n->data;
		const char *nname = exp_name(e);

		node *nn = ol_find_name(t->columns, nname);
		if (nn) {
			sql_column *c = nn->data;
			rel_base_use(sql, r, c->colnr);
		} else if (strcmp(nname, TID) == 0) {
			rel_base_use_tid(sql, r);
		} else {
			assert(0);
		}
	}
	r = rewrite_basetable(sql, r);
	for (n = exps->h, m = r->exps->h; n && m; n = n->next, m = m->next) {
		sql_exp *e = n->data;
		sql_exp *ne = m->data;

		exp_prop_alias(sql->sa, ne, e);
	}
	list_hash_clear(r->exps); /* the child table may have different column names, so clear the hash */

	/* set_remote() */
	if (remote_prop && p && isRemote(p)) {
		list *uris = sa_list(sql->sa);
		tid_uri *tu = SA_NEW(sql->sa, tid_uri);
		tu->id = p->base.id;
		tu->uri = p->query;
		append(uris, tu);

		prop *rmt_prop = r->p = prop_create(sql->sa, PROP_REMOTE, r->p);
		rmt_prop->id = p->base.id;
		rmt_prop->value.pval = uris;
	}
	return r;
}

static sql_rel *
replica_rewrite(visitor *v, sql_table *t, list *exps)
{
	sql_rel *res = NULL;
	rps *rpstate = v->data;
	prop *rp = rpstate->rmt;
	sqlid tid = rp->id;
	list *uris = rp->value.pval;

	if (mvc_highwater(v->sql))
		return sql_error(v->sql, 10, SQLSTATE(42000) "Query too complex: running out of stack space");

	/* if there was a REMOTE property in any higher node and there is not
	 * a local tid then use the available uris to rewrite */
	if (uris && !tid) {
		for (node *n = t->members->h; n && !res; n = n->next) {
			sql_part *p = n->data;
			sql_table *pt = find_sql_table_id(v->sql->session->tr, t->s, p->member);

			if (!isRemote(pt))
				continue;

			for (node *m = uris->h; m && !res; m = m->next) {
				if (strcmp(((tid_uri*)m->data)->uri, pt->query) == 0) {
					/* we found a matching uri do the actual rewrite */
					res = do_replica_rewrite(v->sql, exps, t, pt,
							                 rpstate->no_rmt_branch_rpl_leaf ? true: false);
					/* set to the REMOTE a list with a single uri (the matching one)
					 * this is for the case that our REMOTE subtree has only replicas
					 * with multiple remotes*/
					if (list_length(rp->value.pval) > 1) {
						list *uri = sa_list(v->sql->sa);
						tid_uri *tu = SA_NEW(v->sql->sa, tid_uri);
						/* sql_gencode requires the proper tableid */
						tu->id = p->member;
						tu->uri = pt->query;
						append(uri, tu);
						rp->value.pval = uri;
						break;
					}
				}
			}
		}
	}

	/* no match, find one without remote or use first */
	if (!res) {
		sql_table *pt = NULL;
		int remote = 1;

		for (node *n = t->members->h; n; n = n->next) {
			sql_part *p = n->data;
			sql_table *next = find_sql_table_id(v->sql->session->tr, t->s, p->member);

			/* give preference to local tables and avoid empty merge or replica tables */
			if (!isRemote(next) && ((!isReplicaTable(next) && !isMergeTable(next)) || !list_empty(next->members))) {
				pt = next;
				remote = 0;
				/* if we resolved the replica to a local table we have to
				 * go and remove the remote property from the subtree */
				sql_rel *r = ((rps*)v->data)->orig;
				r->p = prop_remove(r->p, rp);
				break;
			}
		}
		if (!pt) {
			sql_part *p = t->members->h->data;
			pt = find_sql_table_id(v->sql->session->tr, t->s, p->member);
		}

		if ((isMergeTable(pt) || isReplicaTable(pt)) && list_empty(pt->members))
			return sql_error(v->sql, 02, SQLSTATE(42000) "%s '%s'.'%s' should have at least one table associated",
							TABLE_TYPE_DESCRIPTION(pt->type, pt->properties), pt->s->base.name, pt->base.name);
		res = isReplicaTable(pt) ? replica_rewrite(v, pt, exps) : do_replica_rewrite(v->sql, exps, t, pt, remote);
	}
	return res;
}

static bool
eliminate_remote_or_replica_refs(visitor *v, sql_rel **rel)
{
	if (rel_is_ref(*rel) && !((*rel)->flag&MERGE_LEFT)) {
 		if (has_remote_or_replica(*rel)) {
 			sql_rel *nrel = rel_copy(v->sql, *rel, 1);
 			rel_destroy(*rel);
 			*rel = nrel;
 			return true;
 		} else {
 			// TODO why do we want to bail out if we have a non rmt/rpl ref?
 			return false;
 		}
 	}
 	return true;
}

static sql_rel *
rel_rewrite_replica_(visitor *v, sql_rel *rel)
{
	if (!eliminate_remote_or_replica_refs(v, &rel))
		return rel;

	/* if we are higher in the tree clear the previous REMOTE prop in the visitor state */
	if (v->data && v->depth <= ((rps*)v->data)->depth) {
		v->data = NULL;
	}

	/* no-leaf nodes: store the REMOTE property uris in the state of the visitor
	 * leaf nodes: check if they are basetable replicas and proceed with the rewrite */
	prop *p;
	if (!is_basetable(rel->op)) {
		/* if there is a REMOTE prop set it to the visitor state */
		if ((p = find_prop(rel->p, PROP_REMOTE)) != NULL) {
			rps *rp = SA_NEW(v->sql->sa, rps);
			rp->depth = v->depth;
			rp->rmt = p;
			rp->orig = rel;
			rp->no_rmt_branch_rpl_leaf = false;
			v->data = rp;
		}
	} else {
		sql_table *t = rel->l;

		if (t && isReplicaTable(t)) {
			/* we might have reached a replica table through a branch that has
			 * no REMOTE property. In this case we have to set the v->data */
			if (!v->data && (p = find_prop(rel->p, PROP_REMOTE)) != NULL) {
				rps *rp = SA_NEW(v->sql->sa, rps);
				rp->depth = v->depth;
				rp->rmt = p;
				rp->orig = rel;
				rp->no_rmt_branch_rpl_leaf = true;
				v->data = rp;
			}

			if (list_empty(t->members)) /* in DDL statement cases skip if replica is empty */
				return rel;

			sql_rel *r = replica_rewrite(v, t, rel->exps);
			rel_destroy(rel);
			rel = r;
		}
	}
	return rel;
}

static sql_rel *
rel_rewrite_replica(visitor *v, global_props *gp, sql_rel *rel)
{
	(void) gp;
	return rel_visitor_topdown(v, rel, &rel_rewrite_replica_);
}

run_optimizer
bind_rewrite_replica(visitor *v, global_props *gp)
{
	(void) v;
	return gp->needs_mergetable_rewrite || gp->needs_remote_replica_rewrite ? rel_rewrite_replica : NULL;
}

static list*
rel_merge_remote_prop(visitor *v, prop *pl, prop *pr)
{
	list* uris = sa_list(v->sql->sa);
	// TODO this double loop must go (maybe use the hashmap of the list?)
	for (node* n = ((list*)pl->value.pval)->h; n; n = n->next) {
		for (node* m = ((list*)pr->value.pval)->h; m; m = m->next) {
			tid_uri* ltu = n->data;
			tid_uri* rtu = m->data;
			if (strcmp(ltu->uri, rtu->uri) == 0) {
				append(uris, n->data);
			}
		}
	}
	return uris;
}

static sql_rel *
rel_rewrite_remote_(visitor *v, sql_rel *rel)
{
	prop *p, *pl, *pr;

	if (!eliminate_remote_or_replica_refs(v, &rel))
		return rel;

	sql_rel *l = rel->l, *r = rel->r; /* look on left and right relations after possibly doing rel_copy */

	switch (rel->op) {
	case op_basetable: {
		sql_table *t = rel->l;

		/* when a basetable wraps a sql_table (->l) which is remote we want to store its remote
		 * uri to the REMOTE property. As the property is pulled up the tree it can be used in
		 * the case of binary rel operators (see later switch cases) in order to
		 * 1. resolve properly (same uri) replica tables in the other subtree (that's why we
		 *    call the do_replica_rewrite)
		 * 2. pull REMOTE over the binary op if the other subtree has a matching uri remote table
		 */
		if (t && isRemote(t) && (p = find_prop(rel->p, PROP_REMOTE)) == NULL) {
			if (t->query) {
				tid_uri *tu = SA_NEW(v->sql->sa, tid_uri);
				tu->id = t->base.id;
				tu->uri = mapiuri_uri(t->query, v->sql->sa);
				list *uris = sa_list(v->sql->sa);
				append(uris, tu);

				p = rel->p = prop_create(v->sql->sa, PROP_REMOTE, rel->p);
				p->id = 0;
				p->value.pval = uris;
			}
		}
		if (t && isReplicaTable(t) && !list_empty(t->members)) {
			/* the parts of a replica are either
			 * - remote tables for which we have to store tid and uri
			 * - local table for which we only care if they exist (localpart var)
			 * the relevant info are passed in the REMOTE property value.pval and id members
			 */
			list *uris = sa_list(v->sql->sa);
			sqlid localpart = 0;
			for (node *n = t->members->h; n; n = n->next) {
				sql_part *part = n->data;
				sql_table *ptable = find_sql_table_id(v->sql->session->tr, t->s, part->member);

				if (isRemote(ptable)) {
					assert(ptable->query);
					tid_uri *tu = SA_NEW(v->sql->sa, tid_uri);
					tu->id = ptable->base.id;
					tu->uri = mapiuri_uri(ptable->query, v->sql->sa);
					append(uris, tu);
				} else {
					localpart = ptable->base.id;
				}
			}
			/* always introduce the remote prop even if there are no remote uri's
			 * this is needed for the proper replica resolution */
			p = rel->p = prop_create(v->sql->sa, PROP_REMOTE, rel->p);
			p->id = localpart;
			p->value.pval = (void*)uris;
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

		if (rel->flag&MERGE_LEFT) /* search for any remote tables but don't propagate over to this relation */
			return rel;

		/* if both subtrees have REMOTE property with the common uri then pull it up */
		if (l && (pl = find_prop(l->p, PROP_REMOTE)) != NULL &&
			r && (pr = find_prop(r->p, PROP_REMOTE)) != NULL) {

			list *uris = rel_merge_remote_prop(v, pl, pr);

			/* if there are common uris pull the REMOTE prop with the common uris up */
			if (!list_empty(uris)) {
				l->p = prop_remove(l->p, pl);
				r->p = prop_remove(r->p, pr);
				if (!find_prop(rel->p, PROP_REMOTE)) {
					/* remove local tid ONLY if no subtree has local parts */
					if (pl->id == 0 || pr->id == 0)
						pl->id = 0;
					/* set the new (matching) uris */
					pl->value.pval = uris;
					/* push the pl REMOTE property to the list of properties */
					pl->p = rel->p;
					rel->p = pl;
				} else {
					// TODO what if we are here? can that even happen?
				}
			}
		}
		break;
	case op_project:
	case op_select:
	case op_groupby:
	case op_topn:
	case op_sample:
	case op_truncate:
		/* if the subtree has the REMOTE property just pull it up */
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
				r && (pr = find_prop(r->p, PROP_REMOTE)) != NULL) {

				list *uris = rel_merge_remote_prop(v, pl, pr);

				/* if there are common uris pull the REMOTE prop with the common uris up */
				if (!list_empty(uris)) {
					l->p = prop_remove(l->p, pl);
					r->p = prop_remove(r->p, pr);
					if (!find_prop(rel->p, PROP_REMOTE)) {
						/* remove local tid ONLY if no subtree has local parts */
						if (pl->id == 0 || pr->id == 0)
							pl->id = 0;
						/* set the new (matching) uris */
						pl->value.pval = uris;
						/* push the pl REMOTE property to the list of properties */
						pl->p = rel->p;
						rel->p = pl;
					} else {
						// TODO what if we are here? can that even happen?
					}
				}
			}
		}
		break;
	}
	return rel;
}

static sql_rel *
rel_rewrite_remote(visitor *v, global_props *gp, sql_rel *rel)
{
	(void) gp;
	rel = rel_visitor_bottomup(v, rel, &rel_rewrite_remote_);
	v->data = NULL;
	rel = rel_visitor_topdown(v, rel, &rel_rewrite_replica_);
	v->data = NULL;
	return rel;
}

run_optimizer
bind_rewrite_remote(visitor *v, global_props *gp)
{
	(void) v;
	return gp->needs_mergetable_rewrite || gp->needs_remote_replica_rewrite ? rel_rewrite_remote : NULL;
}


static sql_rel *
rel_remote_func_(visitor *v, sql_rel *rel)
{
	(void) v;

	/* Don't modify the same relation twice */
	if (is_rel_remote_func_used(rel->used))
		return rel;
	rel->used |= rel_remote_func_used;

	if (find_prop(rel->p, PROP_REMOTE) != NULL) {
		list *exps = rel_projections(v->sql, rel, NULL, 1, 1);
		rel = rel_unique_exps(v->sql, rel); /* remove any duplicate results (aliases) */
		rel = rel_relational_func(v->sql->sa, rel, exps);
	}
	return rel;
}

static sql_rel *
rel_remote_func(visitor *v, global_props *gp, sql_rel *rel)
{
	(void) gp;
	return rel_visitor_bottomup(v, rel, &rel_remote_func_);
}

run_optimizer
bind_remote_func(visitor *v, global_props *gp)
{
	(void) v;
	return gp->needs_mergetable_rewrite || gp->needs_remote_replica_rewrite ? rel_remote_func : NULL;
}
