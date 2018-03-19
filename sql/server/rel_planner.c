/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0.  If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 *
 * Copyright 1997 - July 2008 CWI, August 2008 - 2018 MonetDB B.V.
 */

#include "monetdb_config.h"
#include "rel_planner.h"
#include "rel_rel.h"
#include "rel_exp.h"
#include "rel_prop.h"
#include "rel_optimizer.h"

typedef struct memoitem {
	const char *name;
	list *rels;
	list *exps;
	list *joins;
	int done;
	int level;
	lng count;
	lng width;
	dbl cost;
	void *data;
} memoitem;

#define p_pkey 1
#define p_fkey 2
#define p_ukey 3

typedef struct memojoin {
	memoitem *l, *r;
	int rules; 	/* handled rules */
	int prop; 	/* pkey, fkey, ukey */
	dbl cost;
	dbl sel;
	sql_exp *e;
} memojoin;

static int
memoitem_key( memoitem *mi )
{
	return hash_key(mi->name);
}

static memoitem*
memo_find(list *memo, const char *name)
{
	int key = hash_key(name);
	sql_hash_e *he;

	MT_lock_set(&memo->ht_lock);
	he = memo->ht->buckets[key&(memo->ht->size-1)]; 
	for (; he; he = he->chain) {
		memoitem *mi = he->value;

		if (mi->name && strcmp(mi->name, name) == 0) {
			MT_lock_unset(&memo->ht_lock);
			return mi;
		}
	}
	MT_lock_unset(&memo->ht_lock);
	return NULL;
}

static char *
merge_names( sql_allocator *sa, const char *lname, const char *rname)
{
	size_t llen = strlen(lname);
	size_t rlen = strlen(rname);
	char *n = SA_NEW_ARRAY(sa, char, llen+rlen+2), *p = n;
	const char *c = lname;

	while (*c) {
		int i = 0;
		for ( ; c[i] && c[i] != ','; i++) 
			p[i] = c[i];
		p[i] = 0;
		if (strcmp(p, rname) > 0) {
			strncpy(p, rname, rlen);
			p+=rlen;
			*p++ = ',';
			strcpy(p, c);
			break;
		} else {
			p+=i;
			*p++ = ',';
			c+=i;
			if (*c == 0) 
				strcpy(p, rname);
			else
				c++;
		}
	}
	return n;
}

static memoitem *
memoitem_create( list *memo, sql_allocator *sa, const char *lname, const char *rname, int level)
{
	const char *name = lname;
	memoitem *mi;

	if (level > 1) 
		name = merge_names(sa, lname, rname);
	if (memo_find(memo, name))
		return NULL;
       	mi = SA_NEW(sa, memoitem); 
	mi->name = sa_strdup(sa, name);
	mi->joins = (rname)?sa_list(sa):NULL;
	mi->done = (rname)?0:1;
	mi->level = level;
	mi->count = 1;
	mi->cost = 0;
	mi->width = 8;
	mi->data = NULL;
	mi->rels = sa_list(sa);
	mi->exps = sa_list(sa);
	list_append(memo, mi);
	return mi;
}

static lng
rel_getcount(mvc *sql, sql_rel *rel)
{
	if (!sql->session->tr)
		return 0;

	switch(rel->op) {
	case op_basetable: {
		sql_table *t = rel->l;

		if (t && isTable(t))
			return (lng)store_funcs.count_col(sql->session->tr, t->columns.set->h->data, 1);
		if (!t && rel->r) /* dict */
			return (lng)sql_trans_dist_count(sql->session->tr, rel->r);
		return 0;
	}
	case op_select:
	case op_project:
		if (rel->l)
			return rel_getcount(sql, rel->l);
		return 1;
	default:
		return 0;
	}
}

static lng
rel_getwidth(mvc *sql, sql_rel *rel)
{
	if (!sql->session->tr)
		return 0;

	switch(rel->op) {
	case op_basetable: {
		sql_table *t = rel->l;

		if (t && isTable(t))
			return 4*list_length(rel->exps);
		return 0;
	}	
	case op_select:
		if (rel->l)
			return rel_getwidth(sql, rel->l);
		return 1;
	case op_project:
		if (rel->l)
			return 4*list_length(rel->exps);
		return 1;
	default:
		return 0;
	}
}

static lng
exp_getdcount( mvc *sql, sql_rel *r , sql_exp *e, lng count)
{
	switch(e->type) {
	case e_column: {
		/* find col */
		sql_rel *bt = NULL;
		sql_column *c = name_find_column(r, e->l, e->r, -1, &bt);
		if (c) {
			lng dcount = (lng)sql_trans_dist_count(sql->session->tr, c);
			if (dcount != 0 && dcount < count)
				return dcount;
		}
		return count;
	} 
	case e_cmp:
		assert(0);
	

	case e_convert:
		if (e->l)
			return exp_getdcount(sql, r, e->l, count);
		/* fall through */
	case e_func:
	case e_aggr:
	case e_atom:
	case e_psm:
	 	return count;
	}
	return count;
}

static int
exp_getranges( mvc *sql, sql_rel *r , sql_exp *e, void **min, void **max)
{
	switch(e->type) {
	case e_column: {
		/* find col */
		sql_rel *bt = NULL;
		sql_column *c = name_find_column(r, e->l, e->r, -1, &bt);
		if (c) 
			return sql_trans_ranges(sql->session->tr, c, min, max);
		return 0;
	} 
	case e_cmp:
		assert(0);

	case e_convert:
		if (e->l)
			return exp_getranges(sql, r, e->l, min, max);
		/* fall through */
	case e_func:
	case e_aggr:
	case e_atom:
	case e_psm:
	 	return 0;
	}
	return 0;
}

static atom *
exp_getatom( mvc *sql, sql_exp *e, atom *m) 
{
	if (is_atom(e->type))
		return exp_value(sql, e, sql->args, sql->argc);
	else if (e->type == e_convert)
		return exp_getatom(sql, e->l, m);
	else if (e->type == e_func) {
		sql_subfunc *f = e->f;
		list *l = e->l;
		/* handle date + x months */
		/* TODO add scalar -> value, ie exp->stmt-tree->exec-tree+exec */
		if (strcmp(f->func->base.name, "sql_add") == 0 && list_length(l) == 2) {
			atom *l1 = exp_getatom(sql, l->h->data, m);
			atom *l2 = exp_getatom(sql, l->h->next->data, m);
			/* data + months */
			(void)l2;
			(void)l1;
			return NULL;
		}
	}
	return m;
}

static dbl
exp_getrange_sel( mvc *sql, sql_rel *r, sql_exp *e, void *min, void *max)
{
	atom *amin, *amax, *emin, *emax;
	dbl sel = 1.0;
	sql_subtype *t = exp_subtype(e->l);

	(void)r;
	emin = amin = atom_general(sql->sa, t, min);
	emax = amax = atom_general(sql->sa, t, max);

	if (e->f || e->flag == cmp_gt || e->flag == cmp_gte) 
		emin = exp_getatom(sql, e->r, amin);
	if (e->f || e->flag == cmp_lt || e->flag == cmp_lte) 
		emax = (e->f)?exp_getatom(sql, e->f, amax):
			exp_getatom(sql, e->r, amax);

	if (!amin || !amax)
		return 0.1;

	if (!emin || !emax)
		sel = 0.125;
	/* 4 case, dbl and lng, date, timestamp */
	else if (t->type->eclass == EC_DATE) {
		sel = (emax->data.val.ival-emin->data.val.ival)/(dbl)(amax->data.val.ival-amin->data.val.ival);
	} else if (t->type->eclass == EC_TIMESTAMP) {
		sel = (emax->data.val.lval-emin->data.val.lval)/(dbl)(amax->data.val.lval-amin->data.val.lval);
	} else if (t->type->eclass == EC_FLT) {
		sel = (emax->data.val.dval-emin->data.val.dval)/(amax->data.val.dval-amin->data.val.dval);
	} else { /* lng */
		sel = (emax->data.val.lval-emin->data.val.lval)/(dbl)(amax->data.val.lval-amin->data.val.lval);
	}
	return sel;
}

static dbl
rel_exp_selectivity(mvc *sql, sql_rel *r, sql_exp *e, lng count)
{
	dbl sel = 1.0;

	if (!e)
		return 1.0;
	switch(e->type) {
	case e_cmp: {
		lng dcount = exp_getdcount( sql, r, e->l, count);

		switch (get_cmp(e)) {
		case cmp_equal: {
			sel = 1.0/dcount;
			break;
		}
		case cmp_notequal:
			sel = (dcount-1.0)/dcount;
			break;
		case cmp_gt:
		case cmp_gte:
		case cmp_lt:
		case cmp_lte: {
			void *min, *max;
			if (exp_getranges( sql, r, e->l, &min, &max )) {
				sel = (dbl)exp_getrange_sel( sql, r, e, min, max);
			} else {
				sel = 0.5;
				if (e->f) /* range */
					sel = 0.25;
			}
		} 	break;
		case cmp_filter:
			sel = 0.01;
			break;
		case cmp_in: 
		case cmp_notin: {
			list *l = e->r;
			sel = (dbl) list_length(l) / dcount;
			break;
		}
		case cmp_or:
			sel = 0.5;
			break;
		default:
			return 1.0;
		}
		break;
	}
	default:
		break;
	}
	return sel;
}

static dbl
rel_join_exp_selectivity(mvc *sql, sql_rel *l, sql_rel *r, sql_exp *e, lng lcount, lng rcount)
{
	dbl sel = 1.0;
	lng ldcount, rdcount;

	if (!e)
		return 1.0;
	assert(lcount);
	assert(rcount);
	ldcount = exp_getdcount(sql, l, e->l, lcount);
	rdcount = exp_getdcount(sql, r, e->r, rcount);
	switch(e->type) {
	case e_cmp:
		switch (get_cmp(e)) {
		case cmp_equal: 
			sel = (lcount/(dbl)ldcount)*(rcount/(dbl)rdcount);
			break;
		case cmp_notequal: {
			dbl cnt = (lcount/(dbl)ldcount)*(rcount/(dbl)rdcount);
			sel = (cnt-1)/cnt;
		}	break;
		case cmp_gt:
		case cmp_gte:
		case cmp_lt:
		case cmp_lte:
			/* ugh */
			sel = 0.5;
			if (e->f) /* range */
				sel = 0.2;
			break;
		case cmp_filter:
			sel = 0.1;
			break;
		case cmp_in: 
		case cmp_notin: {
			lng cnt = lcount*rcount;
			list *l = e->r;
			sel = (dbl) list_length(l) / cnt;
			break;
		}
		case cmp_or:
			sel = 0.5;
			break;
		default:
			return 1.0;
		}
		break;
	default:
		break;
	}
	assert(sel >= 0.000001);
	return sel;
}


static dbl
rel_exps_selectivity(mvc *sql, sql_rel *rel, list *exps, lng count) 
{
	node *n;
	dbl sel = 1.0;
	if (!exps->h)
		return 1.0;
	for(n=exps->h; n; n = n->next) { 
		dbl nsel = rel_exp_selectivity(sql, rel, n->data, count);

		sel *= nsel;
	}
	return sel;
}

/* need real values, ie
 * point select on pkey -> 1 value -> selectivity count 
 */

static dbl
rel_getsel(mvc *sql, sql_rel *rel, lng count)
{
	if (!sql->session->tr)
		return 1.0;

	switch(rel->op) {
	case op_select:
		return rel_exps_selectivity(sql, rel, rel->exps, count);
	case op_project:
		if (rel->l)
			return rel_getsel(sql, rel->l, count);
		/* fall through */
	default:
		return 1.0;
	}
}

static list*
memo_create(mvc *sql, list *rels )
{
	int len = list_length(rels);
	list *memo = sa_list(sql->sa);
	node *n;

	MT_lock_set(&memo->ht_lock);
	memo->ht = hash_new(sql->sa, len*len, (fkeyvalue)&memoitem_key);
	MT_lock_unset(&memo->ht_lock);
	for(n = rels->h; n; n = n->next) {
		sql_rel *r = n->data;
		memoitem *mi = memoitem_create(memo, sql->sa, rel_name(r), NULL, 1);
		dbl sel = 1;

		mi->count = rel_getcount(sql, r);
		sel = rel_getsel(sql, r, mi->count);
		mi->count = MAX( (lng) (mi->count*sel), 1);
		assert(mi->count);
		mi->width = rel_getwidth(sql, r);
		mi->cost = (dbl)(mi->count*mi->width);
		mi->data = r;
		append(mi->rels, r);
	}
	return memo;
}

static void
memo_add_exps(list *memo, mvc *sql, list *rels, list *jes)
{
	node *n;
	memoitem *mi;

	for(n = jes->h; n; n = n->next) {
		sql_exp *e = n->data;
		if (e->type != e_cmp || !is_complex_exp(e->flag)){
			sql_rel *l = find_one_rel(rels, e->l);
			sql_rel *r = find_one_rel(rels, e->r);
			memojoin *mj = SA_ZNEW(sql->sa, memojoin);

			mj->l = memo_find( memo, rel_name(l));
			mj->r = memo_find( memo, rel_name(r));
			mj->rules = 0;
			mj->cost = 0;
			mj->e = e;
			mj->sel = rel_join_exp_selectivity(sql, l, r, e, mj->l->count, mj->r->count);

			mi = memoitem_create(memo, sql->sa, mj->l->name, mj->r->name, 2);
			mi->width = (rel_getwidth(sql, l) + rel_getwidth(sql, r))/2;
			mi->data = e;
			mi->count = (lng)(mj->sel * MIN(mj->l->count, mj->r->count));
			append(mi->rels, l);
			append(mi->rels, r);
			append(mi->exps, e);
			list_append(mi->joins, mj);
		}
	}
}

static int
memoitem_has( memoitem *mi, const char *name)
{
	if (mi->level > 1) {
		memojoin *mj = mi->joins->h->data; 

		return (memoitem_has(mj->l, name) ||
		        memoitem_has(mj->r, name)); 
	} else {
		return strcmp(mi->name, name) == 0;
	}
}

static void
memoitem_add_attr(list *memo, mvc *sql, memoitem *mi, list *rels, list *jes, int level)
{
	node *n;

	for( n = jes->h; n; n = n->next) {
		sql_exp *e = n->data;

		if (e->type != e_cmp || !is_complex_exp(e->flag)){
			int hasl = 0, hasr = 0;
			sql_rel *l = find_one_rel(rels, e->l);
			sql_rel *r = find_one_rel(rels, e->r);

			/* check if exactly one rel is in mi */
			hasl = memoitem_has(mi, rel_name(l));
			hasr = memoitem_has(mi, rel_name(r));
			if (hasl != hasr) {
				memoitem *nmi;
				sql_rel *rr = r;

				if (!hasl)
					rr = l;
				nmi = memoitem_create(memo, sql->sa, mi->name, rel_name(rr), level);
				if (nmi) {
					memojoin *mj = SA_ZNEW(sql->sa, memojoin);
					lng mincnt = 0;

					list_merge(nmi->rels, mi->rels, (fdup)NULL);
					append(nmi->rels, rr);
					append(nmi->exps, e);

					mj->l = mi;
					mj->r = memo_find( memo, rel_name(rr));
				       	mincnt = MIN(mj->l->count, mj->r->count);
					nmi->width = mi->width + mj->r->width;
					mj->rules = 0;
					mj->cost = 0;
					mj->sel = rel_join_exp_selectivity(sql, l, r, e, mj->l->count, mj->r->count);
					list_append(nmi->joins, mj);

					if (!nmi->count)
						nmi->count = (lng)(mincnt*mj->sel);
					nmi->count = MIN((lng) (mincnt*mj->sel), nmi->count);
					assert(nmi->count >= 0);
				}
			}
		}
	}
}

static void
memo_add_attr(list *memo, mvc *sql, list *rels, list *jes)
{
	node *n;
	int l, len = list_length(rels);

	for(l=2; l<len; l++) {
		for (n = memo->h; n; n = n->next) {
			memoitem *mi = n->data;

			if (mi->level == l) 
				memoitem_add_attr( memo, sql, mi, rels, jes, l+1);
		}
	}
}

/* Rule 1: Commutativity A join B -> B join A */
static int 
memoitem_apply_r1(memoitem *mi, sql_allocator *sa)
{
	int changes = 0;
	node *n;

	if (!mi->joins)
		return 0;
	for ( n = mi->joins->h; n; n = n->next) {
		memojoin *mj = n->data;

		if (mj->rules == 0 || mj->rules == 2) {
			memojoin *mjn = SA_ZNEW(sa, memojoin);

			mjn->l = mj->r;
			mjn->r = mj->l;

			if (mj->rules)
				mj->rules = 4;
			else
				mj->rules = 1;
			mjn->rules = 4;
			mjn->cost = 0;
			mjn->sel = mj->sel;
			list_append(mi->joins, mjn);
			changes ++;
		}
	}
	return changes; 
}

/* Rule 2: Right Associativity (A join B) join C -> A join (B join C) */
static int 
memoitem_apply_r2(memoitem *mi, sql_allocator *sa, list *memo)
{
	int changes = 0;
	node *n;

	if (!mi->joins || mi->level <= 2)
		return 0;
	for ( n = mi->joins->h; n; n = n->next) {
		memojoin *mj = n->data;

		if (mj->rules <= 1 && mj->l->level >= 2) {
			node *m;

			for( m = mj->l->joins->h; m; m = m->next) {
				memoitem *r = NULL;
				memojoin *mjl = m->data;
				/* combine mjl->r and mj->r */
				char *name = merge_names(sa, mjl->r->name, mj->r->name);

				if ((r = memo_find(memo, name))) {
					memojoin *mjn = SA_ZNEW(sa, memojoin);

					mjn->l = mjl->l; 
					mjn->r = r;
					mjn->rules = 2;
					mjn->cost = 0;
					mjn->sel = 1;
					list_append(mi->joins, mjn);
					changes ++;
				}
			}
			if (mj->rules)
				mj->rules = 4;
			else
				mj->rules = 2;
		}
	}
	return changes; 
}

/* Rule 4: Exchange (A join B) join (C join D) -> (A join C) join (B join D) 
static int 
memoitem_apply_r4(memoitem *mi, sql_allocator *sa, list *memo)
{
	int changes = 0;
	node *n;

	if (!mi->joins || mi->level <= 2)
		return 0;
	for ( n = mi->joins->h; n; n = n->next) {
		memojoin *mj = n->data;

		if (mj->rules <= 1 && mj->l->level >= 2) {
			node *m;

			for( m = mj->l->joins->h; m; m = m->next) {
				memoitem *r = NULL;
				memojoin *mjl = m->data;
				char *name = merge_names(sa, mjl->r->name, mj->r->name);

				if ((r = memo_find(memo, name))) {
					memojoin *mjn = SA_ZNEW(sa, memojoin);

					mjn->l = mjl->l; 
					mjn->r = r;
					mjn->rules = 2;
					mjn->cost = 0;
					list_append(mi->joins, mjn);
					changes ++;
				}
			}
			if (mj->rules)
				mj->rules = 4;
			else
				mj->rules = 2;
		}
	}
	return changes; 
}
 * */

static void
memo_apply_rules(list *memo, sql_allocator *sa, int len) 
{
	int level;
	node *n;

	for (level = 2; level<=len; level++) {
		int gchanges = 1;

		while(gchanges) {
			gchanges = 0;
			for ( n = memo->h; n; n = n->next) {
				int changes = 0;
				memoitem *mi = n->data;
		
				if (!mi->done && mi->level == level) {
					changes += memoitem_apply_r1(mi, sa);
					changes += memoitem_apply_r2(mi, sa, memo);
					//changes += memoitem_apply_r4(mi, sa, memo);
		
					if (!changes)
						mi->done = 1;
				}
				gchanges |= changes;
			}
		}
	}
}

static void
memo_locate_exps( list *memo )
{
	node *n, *m, *o;

	for(n = memo->h; n; n = n->next) {
		memoitem *mi = n->data;
		int prop = 0;

		if (mi->level == 2) {
			sql_exp *e = mi->data;

			if (find_prop(e->p, PROP_HASHIDX))
				prop = p_pkey;
			if (find_prop(e->p, PROP_JOINIDX))
				prop = p_fkey;

			if (prop) {
				for (m = mi->joins->h; m; m = m->next) {
					memojoin *mj = m->data;
					sql_exp *e = mj->e;

					mj->prop = prop;
					if (prop == p_fkey) {
						sql_rel *l = mj->l->data, *f = NULL;
						if (!l)
							continue;
						if (e)
							f = find_one_rel(mi->rels, e->l);
						if (f != l) /* we dislike swapped pkey/fkey joins */
							mj->prop = 0;
					}
				}
			}
		} else if (mi->level > 2) {
			/* find exp which isn't in the mj->l/r->exps lists */
			for( o = mi->exps->h; o; o = o->next) {
				sql_exp *e = o->data;

				for (m = mi->joins->h; m; m = m->next) {
					memojoin *mj = m->data;

					if (list_find(mj->l->exps, e, NULL) == NULL &&
					    list_find(mj->r->exps, e, NULL) == NULL) {
						if (find_prop(e->p, PROP_HASHIDX))
							prop = p_pkey;
						if (find_prop(e->p, PROP_JOINIDX))
							prop = p_fkey;
						mj->prop = prop;
						if (prop == p_fkey) {
							sql_rel *l = find_one_rel(mi->rels, e->l); 
							sql_rel *f = find_one_rel(mj->l->rels, e->l); 
							if (!l)
								continue;
							if (f != l) /* we dislike swapped pkey/fkey joins */
								mj->prop = 0;
						}
					}
				}
			}
		}
	}
}

static void
memo_compute_cost(list *memo) 
{
	node *n, *m;

	for ( n = memo->h; n; n = n->next) {
		memoitem *mi = n->data;

		if (mi->joins) {
			lng cnt = 0, width = 1;
		        dbl cost = 0;

			/* cost minimum of join costs */
			for ( m = mi->joins->h; m; m = m->next ) {
				memojoin *mj = m->data;

				lng mincnt = MIN(mj->l->count, mj->r->count);
				dbl nsel = mj->sel;
				lng ocnt = MAX((lng) (mincnt*nsel), 1);
				dbl ncost = 0;

				/* mincnt*mincnt_size_width*hash_const_cost + mincnt * output_width(for now just sum of width) * memaccess const */
				/* current consts are 1 and 1 */
				//ncost += ocnt * MIN(mj->l->width, mj->r->width);
				width = (mj->l->count < mj->r->count)?mj->l->width:mj->r->width;
				ncost += (mincnt * width * 1 ) + ocnt * (mj->l->width + mj->r->width) * 1;
				assert(mj->l->cost > 0 && mj->r->cost > 0); 
				ncost += mj->l->cost; /* add cost of left */
				ncost += mj->r->cost; /* add cost of right */

				width = mj->l->width + mj->r->width;
				mj->cost = ncost;

				if (cnt == 0) 
					cnt = ocnt;
				cnt = MIN(cnt,ocnt);

				if (cost == 0) 
					cost = ncost;
				cost = MIN(cost,ncost);
			}
			assert(cnt > 0);
			mi->count = cnt;
			mi->cost = cost;
			mi->width = width;
		}
	}
}

#ifndef HAVE_EMBEDDED
static void
memojoin_print( memojoin *mj )
{
	printf("%s join-%s%d(cost=%f) %s", mj->l->name, mj->prop==p_pkey?"pkey":mj->prop==p_fkey?"fkey":"", mj->rules, mj->cost, mj->r->name);
}

static void
memojoins_print( list *joins )
{
	node *n;

	if (!joins)
		return;
	for(n=joins->h; n; n = n->next) {
		memojoin *mj = n->data;

		memojoin_print( mj );
		if (n->next)
			printf(" | ");
	}
}

static void
memoitem_print( memoitem *mi )
{
	printf("# %s(count="LLFMT",width="LLFMT",cost=%f): ", mi->name, mi->count, mi->width, mi->cost);
	memojoins_print(mi->joins);
}

static void
memo_print( list *memo )
{
	node *n;
	int level = 0;

	for(n=memo->h; n; n = n->next) {
		memoitem *mi = n->data;

		if (mi->level > level){
			level = mi->level;
			printf("\n");
		}
		memoitem_print( mi );
		printf("\n");
	}
}
#endif

static memojoin *
find_cheapest( list *joins )
{
	node *n;
	memojoin *cur = NULL;

	if (!joins)
		return NULL;
	cur = joins->h->data;
	for ( n = joins->h; n; n = n->next) {
		memojoin *mj = n->data;

		if (cur->cost > mj->cost)
			cur = mj;
	}
	return cur;
}

static sql_rel *
memo_select_plan( mvc *sql, list *memo, memoitem *mi, list *sdje, list *exps)
{
	if (mi->level >= 2) {
		memojoin *mj = find_cheapest(mi->joins);
		sql_rel *top;
	
		top = rel_crossproduct(sql->sa, 
			memo_select_plan(sql, memo, mj->l, sdje, exps), 
			memo_select_plan(sql, memo, mj->r, sdje, exps),
			op_join);
		if (mi->level == 2) {
			rel_join_add_exp(sql->sa, top, mi->data);
			list_remove_data(sdje, mi->data);
		} else {
			node *djn;

			/* all other join expressions on these 2 relations */
			while((djn = list_find(sdje, mi->rels, (fcmp)&exp_joins_rels)) != NULL) {
				sql_exp *e = djn->data;

				rel_join_add_exp(sql->sa, top, e);
				list_remove_data(sdje, e);
			}

			/* all other join expressions on these 2 relations */
			while((djn = list_find(exps, mi->rels, (fcmp)&exp_joins_rels)) != NULL) {
				sql_exp *e = djn->data;

				rel_join_add_exp(sql->sa, top, e);
				list_remove_data(exps, e);
			}
		}
		return top;
	} else {
		return mi->data;
	}
}

sql_rel *
rel_planner(mvc *sql, list *rels, list *sdje, list *exps)
{
	list *memo = memo_create(sql, rels);
	memoitem *mi;
	sql_rel *top;

	/* extend one attribute at a time */
	memo_add_exps(memo, sql, rels, sdje);
	memo_add_attr(memo, sql, rels, sdje);

	memo_apply_rules(memo, sql->sa, list_length(rels));
	memo_locate_exps(memo);
	memo_compute_cost(memo);
#ifndef HAVE_EMBEDDED
	//if (0)
		memo_print(memo);
#endif
	mi = memo->t->data;
	top = memo_select_plan(sql, memo, mi, sdje, exps);
	if (list_length(sdje) != 0)
		list_merge (top->exps, sdje, (fdup)NULL);
	return top;
}


