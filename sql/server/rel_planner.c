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
 * Copyright August 2008-2014 MonetDB B.V.
 * All Rights Reserved.
 */

#include "monetdb_config.h"
#include "rel_planner.h"
#include "rel_exp.h"
#include "rel_prop.h"
#include "rel_select.h"
#include "rel_optimizer.h"

typedef struct memoitem {
	char *name;
	list *rels;
	list *exps;
	list *joins;
	int done;
	int level;
	lng count;
	dbl sel;
	lng cost;
	void *data;
} memoitem;

#define p_pkey 1
#define p_fkey 2
#define p_ukey 3

typedef struct memojoin {
	memoitem *l, *r;
	int rules; 	/* handled rules */
	int prop; 	/* pkey, fkey, ukey */
	lng cost;
	sql_exp *e;
} memojoin;

static int
memoitem_key( memoitem *mi )
{
	return hash_key(mi->name);
}

static memoitem*
memo_find(list *memo, char *name)
{
	int key = hash_key(name);
	sql_hash_e *he;

	MT_lock_set(&memo->ht_lock, "memo_find");
	he = memo->ht->buckets[key&(memo->ht->size-1)]; 
	for (; he; he = he->chain) {
		memoitem *mi = he->value;

		if (mi->name && strcmp(mi->name, name) == 0) {
			MT_lock_unset(&memo->ht_lock, "memo_find");
			return mi;
		}
	}
	MT_lock_unset(&memo->ht_lock, "memo_find");
	return NULL;
}

static char *
merge_names( sql_allocator *sa, char *lname, char *rname)
{
	size_t llen = strlen(lname);
	size_t rlen = strlen(rname);
	char *n = SA_NEW_ARRAY(sa, char, llen+rlen+2), *p = n;
	char *c = lname;

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
memoitem_create( list *memo, sql_allocator *sa, char *lname, char *rname, int level)
{
	char *name = lname;
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
	mi->count = 0;
	mi->cost = 0;
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

		if (isTable(t))
			return store_funcs.count_col(sql->session->tr, t->columns.set->h->data, 1);
	}	break;
	case op_select:
	case op_project:
		if (rel->l)
			return rel_getcount(sql, rel->l);
		return 1;
	default:
		return 0;
	}
	return 0;
}

static dbl
rel_exp_selectivity(mvc *sql, sql_rel *r, sql_exp *e)
{
	int key = 0;
	dbl sel = 1.0;

	if (!e)
		return 1.0;
	/*
	if (find_prop(e->p, PROP_JOINIDX))
		*cnt += 100;
		*/
	if (find_prop(e->p, PROP_HASHCOL)) 
		key = 1;
	if (find_prop(e->p, PROP_HASHIDX)) 
		key = 1;

	switch(e->type) {
	case e_cmp:
		switch (get_cmp(e)) {
		case cmp_equal:
			if (key)
				sel = 1.0/rel_getcount(sql, r);
			else 	/* TODO: need estimates for number of distinct values here */
				sel = 1.0/100; 
			break;
		case cmp_notequal:
			if (key) {
				dbl cnt = (dbl) rel_getcount(sql,r);
				sel = (cnt-1)/cnt;
			} else 	/* TODO: need estimates for number of distinct values here */
				sel = 1.0; 
			break;
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
			list *l = e->r;
			if (key) 
				sel = (dbl) list_length(l) / rel_getcount(sql, r);
			else 	/* TODO: need estimates for number of distinct values here */
				sel = list_length(l)/100; 
			break;
		}
		case cmp_or:
			sel = 0.1;
			break;
		default:
			return 1.0;
		}
		break;
	default:
		return 1.0;
	}
	return sel;
}

static dbl
rel_exps_selectivity(mvc *sql, sql_rel *rel, list *exps) 
{
	node *n;
	dbl sel = 0;
	if (!exps->h)
		return 1.0;
	for(n=exps->h; n; n = n->next) { 
		dbl nsel = rel_exp_selectivity(sql, rel, n->data);

		sel *= nsel;
	}
	return sel;
}

/* need real values, ie
 * point select on pkey -> 1 value -> selectivity count 
 */

static dbl
rel_getsel(mvc *sql, sql_rel *rel)
{
	if (!sql->session->tr)
		return 1.0;

	switch(rel->op) {
	case op_select:
		return rel_exps_selectivity(sql, rel, rel->exps);
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

	MT_lock_set(&memo->ht_lock, "memo_create");
	memo->ht = hash_new(sql->sa, len*len, (fkeyvalue)&memoitem_key);
	MT_lock_unset(&memo->ht_lock, "memo_create");
	for(n = rels->h; n; n = n->next) {
		sql_rel *r = n->data;
		memoitem *mi = memoitem_create(memo, sql->sa, rel_name(r), NULL, 1);

		mi->count = rel_getcount(sql, r);
		mi->sel = rel_getsel(sql, r);
		if (mi->sel != 1.0) 
			mi->count = MAX( (lng) (mi->count*mi->sel), 1);
		mi->cost = mi->count;
		mi->data = r;
		append(mi->rels, r);
	}
	return memo;
}

static void
memo_add_exps(list *memo, sql_allocator *sa, list *rels, list *jes)
{
	node *n;
	memoitem *mi;

	for(n = jes->h; n; n = n->next) {
		sql_exp *e = n->data;
		if (e->type != e_cmp || !is_complex_exp(e->flag)){
			sql_rel *l = find_one_rel(rels, e->l);
			sql_rel *r = find_one_rel(rels, e->r);
			memojoin *mj = SA_ZNEW(sa, memojoin);

			mj->l = memo_find( memo, rel_name(l));
			mj->r = memo_find( memo, rel_name(r));
			mj->rules = 0;
			mj->cost = 0;
			mj->e = e;

			mi = memoitem_create(memo, sa, mj->l->name, mj->r->name, 2);
			mi->data = e;
			append(mi->rels, l);
			append(mi->rels, r);
			append(mi->exps, e);
			list_append(mi->joins, mj);
		}
	}
}

static int
memoitem_has( memoitem *mi, char *name)
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
memoitem_add_attr(list *memo, sql_allocator *sa, memoitem *mi, list *rels, list *jes, int level)
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
				nmi = memoitem_create(memo, sa, mi->name, rel_name(rr), level);
				if (nmi) {
					memojoin *mj = SA_ZNEW(sa, memojoin);

					list_merge(nmi->rels, mi->rels, (fdup)NULL);
					append(nmi->rels, rr);
					append(nmi->exps, e);

					mj->l = mi;
					mj->r = memo_find( memo, rel_name(rr));
					mj->rules = 0;
					mj->cost = 0;
					list_append(nmi->joins, mj);
				}
			}
		}
	}
}

static void
memo_add_attr(list *memo, sql_allocator *sa, list *rels, list *jes)
{
	node *n;
	int l, len = list_length(rels);

	for(l=2; l<len; l++) {
		for (n = memo->h; n; n = n->next) {
			memoitem *mi = n->data;

			if (mi->level == l) 
				memoitem_add_attr( memo, sa, mi, rels, jes, l+1);
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

		if (!mi->count && mi->joins) {
			lng cnt = 0, cost = 0;
		        dbl sel = 0.0;

			/* cost minimum of join costs */
			for ( m = mi->joins->h; m; m = m->next ) {
				memojoin *mj = m->data;
				lng ncost = 0;
				lng maxcnt = MAX(mj->l->count, mj->r->count);
				lng mincnt = MIN(mj->l->count, mj->r->count);
				lng ocnt = maxcnt;
				dbl maxsel = MIN(mj->l->sel,mj->r->sel);
				dbl minsel = MAX(mj->l->sel,mj->r->sel);
				dbl nsel = maxsel*minsel;

				if (!mj->prop)
					ocnt = maxcnt = maxcnt*mincnt;
				if (mj->prop && nsel != 1.0)
					ocnt = MAX((lng) (maxcnt*nsel), 1);
				if (mj->prop)
					ncost = mj->l->count + mj->r->count + mj->l->cost + mj->r->cost; 
				else
					ncost = mj->l->count * mj->r->count + mj->l->cost + mj->r->cost; 
				ncost += ocnt;

				if (cnt == 0) 
					cnt = ocnt;
				cnt = MIN(cnt,ocnt);

				mj->cost = ncost;

				if (cost == 0) 
					cost = ncost;
				cost = MIN(cost,ncost);

				if (sel == 0) 
					sel = nsel;
				sel = MAX(sel, nsel);
			}
			mi->count = cnt;
			mi->cost = cost;
			mi->sel = sel;
		}
	}
}

static void
memojoin_print( memojoin *mj )
{
	printf("%s join-%s%d(cost=%lld) %s", mj->l->name, mj->prop==p_pkey?"pkey":mj->prop==p_fkey?"fkey":"", mj->rules, mj->cost, mj->r->name);
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
	printf("# %s(%d,count=%lld,cost=%lld,sel=%f): ", mi->name, mi->done, mi->count, mi->cost, mi->sel);
	memojoins_print(mi->joins);
}

static void
memo_print( list *memo )
{
	node *n;

	for(n=memo->h; n; n = n->next) {
		memoitem *mi = n->data;

		memoitem_print( mi );
		printf("\n");
	}
}

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
memo_select_plan( mvc *sql, list *memo, memoitem *mi, list *sdje)
{
	if (mi->level >= 2) {
		memojoin *mj = find_cheapest(mi->joins);
		sql_rel *top;
	
		top = rel_crossproduct(sql->sa, 
			memo_select_plan(sql, memo, mj->l, sdje), 
			memo_select_plan(sql, memo, mj->r, sdje),
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
		}
		return top;
	} else {
		return mi->data;
	}
}

sql_rel *
rel_planner(mvc *sql, list *rels, list *sdje)
{
	list *memo = memo_create(sql, rels);
	memoitem *mi;
	sql_rel *top;

	/* extend one attribute at a time */
	memo_add_exps(memo, sql->sa, rels, sdje);
	memo_add_attr(memo, sql->sa, rels, sdje);

	memo_apply_rules(memo, sql->sa, list_length(rels));
	memo_locate_exps(memo);
	memo_compute_cost(memo);

	//if (0)
		memo_print(memo);
	mi = memo->t->data;
	top = memo_select_plan(sql, memo, mi, sdje);
	if (list_length(sdje) != 0)
		list_merge (top->exps, sdje, (fdup)NULL);
	return top;
}


