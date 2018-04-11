/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0.  If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 *
 * Copyright 1997 - July 2008 CWI, August 2008 - 2018 MonetDB B.V.
 */

/*
 * The query cache
 * ===============
 *
 * An effective scheme to speedup processing of an SQL engine is
 * to keep a cache of recently executed queries around.
 * This cache can be inspected by simply searching for an
 * identical query string, or a simple pattern match against
 * the sql query tree.  Finding an element saves code generation.
 *
 * The scheme used here is based on keeping a hash-key around for
 * the original text and subsequently perform a parse-tree comparison.
 * This means that only text-identical queries are captured.
 *
 * The upper layers should consider the cache as an auxiliary
 * structure. There is no guarantee that elements remain in
 * the cache forever, nor control features to assure this.
 *
 * Entries in the cache obtain a unique(?) cache entry number.
 * It can be used as an external name.
 *
 * [todo]
 * The information retain for each cached item is back-end specific.
 * It should have a hook to update the initialize the cache entry
 * and a method to destroy it.
 *
 * The optimization/processing cost should be kept around and the re-use of
 * a cache entry.
 */

#include "monetdb_config.h"

#include "sql_qc.h"
#include "sql_mvc.h"
#include "sql_atom.h"

qc *
qc_create(int clientid, int seqnr)
{
	qc *r = MNEW(qc);
	if(!r)
		return NULL;
	r->clientid = clientid;
	r->id = seqnr;
	r->nr = 0;

	r->q = NULL;
	return r;
}

static void
cq_delete(int clientid, cq *q)
{
	if (q->code)
		backend_freecode(clientid, q->code, q->stk, q->id, q->name);
	if (q->stk)
		backend_freestack(clientid, q->stk);
	if (q->codestring)
		_DELETE(q->codestring);

	/* params and name are allocated using sa, ie need to be delete last */
	if (q->sa) 
		sa_destroy(q->sa);
	_DELETE(q);
}

void
qc_delete(qc *cache, cq *q)
{
	cq *n, *p = NULL;

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

void
qc_clean(qc *cache)
{
	cq *n, *q, *p = NULL;

	for (q = cache->q; q; ) {
		if (q->type != Q_PREPARE) {
			n = q->next;
			if (p) 
				p->next = n;
			else
				cache->q = n;
			cq_delete(cache->clientid, q);
			cache->nr--;
			q = n;
			continue;
		}
		p = q;
		q = q->next;
	}
}

void
qc_destroy(qc *cache)
{
	cq *q, *n;
	for (q = cache->q; q; q = n) {
		n = q->next;

		cq_delete(cache->clientid, q);
		cache->nr--;
	}
	_DELETE(cache);
}

static int
param_cmp(sql_subtype *t1, sql_subtype *t2)
{
	int res;

	if (t1->scale != t2->scale)
		return -1;

	if (t1->type->eclass == EC_NUM && t2->type->eclass == EC_NUM &&
		t1->digits >= t2->digits)
		return 0;

	res = is_subtype(t2, t1);
	if (!res)
		return -1;
/*
	if ((t1->digits > 0 && t1->scale == 0 && t1->digits < t2->digits) || (t1->scale > 0 && t1->digits > 0 && t1->digits - t1->scale < t2->digits - t2->scale)) {
		return -1;
	}
*/
	return 0;
}

static int
param_list_cmp(sql_subtype *typelist, atom **atoms, int plen, int type)
{
	int i;

	if (!plen && !typelist)
		return 0;

	if (!typelist || !atoms)
		return -1;
	for (i=0; i < plen; i++) {
		sql_subtype *tp = typelist + i;
		atom *a = atoms[i];

		if (atom_null(a) && type != Q_UPDATE)
			return -1;

		/* NULL values match any type */
		if (!atom_null(a) && param_cmp(tp, atom_type(a)) != 0) {
			sql_subtype *at = atom_type(a);

			if (tp->type->eclass == EC_CHAR && 
			    at->type->eclass == EC_CHAR &&
			      (!tp->digits || tp->digits == at->digits)) 
				continue;
			if (tp->type->eclass == EC_STRING && 
			    at->type->eclass == EC_CHAR &&
			      (!tp->digits || tp->digits >= at->digits)) 
				continue;
			if (type != Q_UPDATE)
				return -1;
			/* FLT == DEC/NUM and DEC/NUM are equal */
			if ((!((at->type->eclass == EC_DEC ||
			        at->type->eclass == EC_NUM) &&
			       tp->type->eclass == EC_FLT)) &&
			   (!(EC_VARCHAR(tp->type->eclass) && 
			      EC_VARCHAR(at->type->eclass) &&
			      (!tp->digits ||
			       tp->digits >= at->digits))) &&
			   (!(tp->type->eclass == EC_DEC &&
			      at->type->eclass == EC_NUM &&
			      tp->type->localtype >= at->type->localtype &&
		             atom_num_digits(a)+tp->scale <= tp->digits)) &&
			/*
			   (!(tp->type->eclass == EC_DEC &&
			      at->type->eclass == EC_DEC &&
			      tp->type->localtype >= at->type->localtype &&
			      at->digits <= tp->digits &&
			      at->scale <= tp->scale)) &&
			*/
			   (!(at->type->eclass == EC_NUM && tp->type->eclass == EC_NUM && 
			      at->type->localtype <= tp->type->localtype)))
				return -1;
		}
	}
	return 0;
}

cq *
qc_find(qc *cache, int id)
{
	cq *q;

	for (q = cache->q; q; q = q->next) {
		if (q->id == id) {
			q->count++;
			return q;
		}
	}
	return NULL;
}

cq *
qc_match(qc *cache, symbol *s, atom **params, int  plen, int key)
{
	cq *q;

	for (q = cache->q; q; q = q->next) {
		if (q->key == key) {
			if (q->paramlen == plen && param_list_cmp(q->params, params, plen, q->type) == 0 && symbol_cmp(q->s, s) == 0) {
				q->count++;
				return q;
			}
		}
	}
	return NULL;
}

cq *
qc_insert(qc *cache, sql_allocator *sa, sql_rel *r, char *qname,  symbol *s, atom **params, int paramlen, int key, int type, char *cmd, int no_mitosis)
{
	int i, namelen;
	cq *n = MNEW(cq);
	if(!n)
		return NULL;

	n->id = cache->id++;
	cache->nr++;

	n->sa = sa;
	n->rel = r;
	n->s = s;

	n->params = NULL;
	n->paramlen = paramlen;
	if (paramlen) {
		n->params = SA_NEW_ARRAY(sa, sql_subtype,paramlen);
		if(!n->params) {
			_DELETE(n);
			return NULL;
		}
		for (i = 0; i < paramlen; i++) {
			atom *a = params[i];

			n->params[i] = *(atom_type(a));
		}
	}
	n->next = cache->q;
	n->stk = 0;
	n->code = NULL;
	n->type = type;
	n->key = key;
	n->codestring = cmd;
	n->count = 1;
	namelen = 5 + ((n->id+7)>>3) + ((cache->clientid+7)>>3);
	n->name = sa_alloc(sa, namelen);
	n->no_mitosis = no_mitosis;
	if(!n->name) {
		_DELETE(n->params);
		_DELETE(n);
		return NULL;
	}
	strcpy(n->name, qname);
	cache->q = n;
	return n;
}

int
qc_isaquerytemplate(str name){
	int i,j;
	return sscanf(name, "s%d_%d", &i,&j) == 2;
}

int
qc_isapreparedquerytemplate(str name){
	int i,j;
	return sscanf(name, "p%d_%d", &i,&j) == 2;
}

int
qc_size(qc *cache)
{
	return cache->nr;
}
