/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0.  If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 *
 * Copyright 1997 - July 2008 CWI, August 2008 - 2021 MonetDB B.V.
 */

#include "monetdb_config.h"

#include "sql_qc.h"
#include "sql_mvc.h"
#include "sql_atom.h"
#include "rel_exp.h"
#include "gdk_time.h"

qc *
qc_create(sql_allocator *sa, int clientid, int seqnr)
{
	qc *r = SA_ZNEW(sa, qc);
	if (!r)
		return NULL;
	*r = (qc) {
		.clientid = clientid,
		.id = seqnr,
	};
	return r;
}

static void
cq_delete(int clientid, cq *q)
{
	if (q->name)
		backend_freecode(clientid, q->name);
	/* q, params and name are allocated using sa, ie need to be delete last */
	if (q->sa)
		sa_destroy(q->sa);
}

void
qc_delete(qc *cache, cq *q)
{
	cq *n, *p = NULL;

	if (cache) {
		for (n = cache->q; n; p = n, n = n->next) {
			if (n == q) {
				if (p) {
					p->next = q->next;
				} else {
					cache->q = q->next;
				}
				cq_delete(cache->clientid, q);
				cache->nr--;
				break;
			}
		}
	}
}

void
qc_clean(qc *cache)
{
	cq *n, *p = NULL;

	if (cache) {
		for (n = cache->q; n; ) {
			p = n->next;
			cq_delete(cache->clientid, n);
			cache->nr--;
			n = p;
		}
		cache->q = NULL;
	}
}

void
qc_destroy(qc *cache)
{
	cq *q, *n;

	if (cache) {
		for (q = cache->q; q; q = n) {
			n = q->next;

			cq_delete(cache->clientid, q);
			cache->nr--;
		}
	}
}

cq *
qc_find(qc *cache, int id)
{
	cq *q;

	if (cache) {
		for (q = cache->q; q; q = q->next) {
			if (q->id == id) {
				q->count++;
				return q;
			}
		}
	}
	return NULL;
}

cq *
qc_insert(qc *cache, sql_allocator *sa, sql_rel *r, symbol *s, list *params, mapi_query_t type, char *cmd, int no_mitosis)
{
	int namelen;
	sql_func *f = SA_ZNEW(sa, sql_func);
	cq *n = SA_ZNEW(sa, cq);
	list *res = NULL;

	if (!n || !f || !cache)
		return NULL;
	n->id = cache->id++;
	cache->nr++;

	n->sa = sa;
	n->rel = r;
	n->s = s;

	n->next = cache->q;
	n->type = type;
	n->count = 1;
	namelen = 5 + ((n->id+7)>>3) + ((cache->clientid+7)>>3);
	char *name = sa_alloc(sa, namelen);
	n->no_mitosis = no_mitosis;
	n->created = timestamp_current();
	if (!name)
		return NULL;
	(void) snprintf(name, namelen, "p%d_%d", n->id, cache->clientid);
	n->name = name;
	cache->q = n;

	if (r && is_project(r->op) && !list_empty(r->exps)) {
		sql_arg *a;
		node *m;

		res = sa_list(sa);
		for(m = r->exps->h; m; m = m->next) {
			sql_exp *e = m->data;
			sql_subtype *t = exp_subtype(e);

			a = NULL;
			if (t)
				a = sql_create_arg(sa, NULL, t, ARG_OUT);
			append(res, a);
		}
	}

	*f = (sql_func) {
		.mod = sql_private_module_name,
		.type = F_PROC,
		.query = cmd,
		.ops = params,
		.res = res,
	};
	base_init(sa, &f->base, 0, TR_NEW, NULL);
	f->base.id = n->id;
	f->base.name = f->imp = name;
	n->f = f;
	return n;
}

int
qc_size(qc *cache)
{
	return cache ? cache->nr : 0;
}
