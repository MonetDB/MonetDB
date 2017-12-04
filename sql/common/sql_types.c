/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0.  If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 *
 * Copyright 1997 - July 2008 CWI, August 2008 - 2017 MonetDB B.V.
 */

/*
 * The typing scheme of SQL is quite elaborate. The standard introduces
 * several basic types with a plethora of functions.
 * As long as we haven't implemented a scheme to accept the
 * function type signature and relate it to a C-function linked
 * with the system, we have to patch the code below.
 *
 * Given the large number of examples, it should be relatively
 * easy to find something akin you intend to enter.
 */

#include "monetdb_config.h"
#include "sql_types.h"
#include "sql_keyword.h"	/* for keyword_exists(), keywords_insert(), init_keywords(), exit_keywords() */
#ifdef HAVE_HGE
#include "mal.h"		/* for have_hge */
#endif
#include <string.h>

#define END_SUBAGGR	1
#define END_AGGR	2
#define END_SUBTYPE	3
#define END_TYPE	4

list *aliases = NULL;
list *types = NULL;
list *aggrs = NULL;
list *funcs = NULL;

static sql_type *BIT = NULL;
static list *localtypes = NULL;

int digits2bits(int digits) 
{
	if (digits < 3) 
		return 8;
	else if (digits < 5) 
		return 16;
	else if (digits <= 5) 
		return 17;
	else if (digits <= 6) 
		return 20;
	else if (digits <= 7) 
		return 24;
	else if (digits <= 8) 
		return 27;
	else if (digits < 10) 
		return 32;
	else if (digits < 17) 
		return 51;
#ifdef HAVE_HGE
	else if (digits < 19 || !have_hge)
		return 64;
	return 128;
#else
	return 64;
#endif
}

int bits2digits(int bits) 
{
	if (bits < 4) 
		return 1;
	else if (bits < 7) 
		return 2;
	else if (bits < 10) 
		return 3;
	else if (bits < 14) 
		return 4;
	else if (bits < 16) 
		return 5;
	else if (bits < 20) 
		return 6;
	else if (bits < 24) 
		return 7;
	else if (bits <= 27) 
		return 8;
	else if (bits <= 30) 
		return 9;
	else if (bits <= 32) 
		return 10;
#ifdef HAVE_HGE
	else if (bits <= 64 || !have_hge)
		return 19;
	return 39;
#else
	return 19;
#endif
}

/* 0 cannot convert */
/* 1 set operations have very limited coersion rules */
/* 2 automatic coersion (could still require dynamic checks for overflow) */
/* 3 casts are allowed (requires dynamic checks) (sofar not used) */
static int convert_matrix[EC_MAX][EC_MAX] = {

/* EC_ANY */	{ 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1 }, /* NULL */
/* EC_TABLE */	{ 0, 0, 0, 1, 1, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0 },
/* EC_BIT */	{ 0, 0, 1, 1, 1, 0, 2, 2, 2, 0, 0, 0, 0, 0, 0, 0, 0 },
/* EC_CHAR */	{ 2, 2, 2, 1, 1, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2 },
/* EC_STRING */	{ 2, 2, 2, 1, 1, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2 },
/* EC_BLOB */	{ 0, 0, 0, 0, 1, 1, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0 },
/* EC_POS */	{ 0, 0, 2, 1, 1, 0, 1, 1, 1, 1, 1, 1, 0, 0, 0, 0, 0 },
/* EC_NUM */	{ 0, 0, 2, 1, 1, 0, 1, 1, 1, 1, 1, 1, 0, 0, 0, 0, 0 },
/* EC_MONTH*/   { 0, 0, 0, 1, 1, 0, 1, 1, 1, 0, 0, 0, 0, 1, 0, 0, 0 },
/* EC_SEC*/     { 0, 0, 0, 1, 1, 0, 1, 1, 0, 1, 1, 0, 1, 0, 1, 0, 0 },
/* EC_DEC */	{ 0, 0, 0, 1, 1, 0, 1, 1, 0, 1, 1, 1, 0, 0, 0, 0, 0 },
/* EC_FLT */	{ 0, 0, 0, 1, 1, 0, 1, 1, 0, 3, 1, 1, 0, 0, 0, 0, 0 },
/* EC_TIME */	{ 0, 0, 0, 1, 1, 0, 0, 0, 0, 0, 0, 0, 1, 0, 0, 0, 0 },
/* EC_DATE */	{ 0, 0, 0, 1, 1, 0, 0, 0, 0, 0, 0, 0, 0, 1, 3, 0, 0 },
/* EC_TSTAMP */	{ 0, 0, 0, 1, 1, 0, 0, 0, 0, 0, 0, 0, 1, 1, 1, 0, 0 },
/* EC_GEOM */	{ 0, 0, 0, 1, 1, 0, 0, 0, 0, 0, 0, 0, 1, 1, 1, 1, 0 },
/* EC_EXTERNAL*/{ 0, 0, 0, 1, 1, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0 }
};

int sql_type_convert (int from, int to) 
{
	int c = convert_matrix[from][to];
	return c;
}

int is_commutative(const char *fnm)
{
	if (strcmp("sql_add", fnm) == 0 ||
	    strcmp("sql_mul", fnm) == 0)
	    	return 1;
	return 0;
}

void
base_init(sql_allocator *sa, sql_base * b, sqlid id, int flag, const char *name)
{
	b->id = id;

	assert(sa);
	b->wtime = 0;
	b->rtime = 0;
	b->flag = flag;
	b->name = NULL;
	b->refcnt = 1;
	if (name)
		b->name = sa_strdup(sa,name);
}

void
sql_init_subtype(sql_subtype *res, sql_type *t, unsigned int digits, unsigned int scale)
{
	res->type = t;
	res->digits = digits ? digits : t->digits;
	if (t->digits && res->digits > t->digits)
		res->digits = t->digits;
	res->scale = scale;
}

sql_subtype *
sql_create_subtype(sql_allocator *sa, sql_type *t, unsigned int digits, unsigned int scale)
{
	sql_subtype *res = SA_ZNEW(sa, sql_subtype);

	sql_init_subtype(res, t, digits, scale);
	return res;
}

static int
localtypes_cmp(int nlt, int olt)
{
	if (nlt == TYPE_flt || nlt == TYPE_dbl) {
		nlt = TYPE_dbl;
#ifdef HAVE_HGE
	} else if (nlt == TYPE_bte || nlt == TYPE_sht || nlt == TYPE_int || nlt == TYPE_lng || nlt == TYPE_hge) {
		assert(have_hge || nlt != TYPE_hge);
		nlt = have_hge ? TYPE_hge : TYPE_lng;
#else
	} else if (nlt == TYPE_bte || nlt == TYPE_sht || nlt == TYPE_int || nlt == TYPE_lng) {
		nlt = TYPE_lng;
#endif
	}
	if (nlt == olt)
		return 1;
	return 0;
}

sql_subtype *
sql_find_numeric(sql_subtype *r, int localtype, unsigned int digits)
{
	node *m, *n;

	if (localtype == TYPE_flt || localtype == TYPE_dbl) {
		localtype = TYPE_dbl;
	} else {
#ifdef HAVE_HGE
		if (have_hge) {
			localtype = TYPE_hge;
			if (digits >= 128)
				digits = 127;
		} else
#endif
		{
			localtype = TYPE_lng;
			if (digits >= 64)
				digits = 63;
		}
	}

	for (n = types->h; n; n = n->next) {
		sql_type *t = n->data;

		if (localtypes_cmp(t->localtype, localtype)) {
			if ((digits && t->digits > digits) || (!digits && digits == t->digits)) {
				sql_init_subtype(r, t, digits, 0);
				return r;
			}
			for (m = n->next; m; m = m->next) {
				t = m->data;
				if (!localtypes_cmp(t->localtype, localtype)) {
					break;
				}
				n = m;
				if ((digits && t->digits > digits) || (!digits && digits == t->digits)) {
					sql_init_subtype(r, t, digits, 0);
					return r;
				}
			}
		}
	}
	return NULL;
}

int 
sql_find_subtype(sql_subtype *res, const char *name, unsigned int digits, unsigned int scale)
{
	/* todo add approximate info
	 * if digits/scale == 0 and no approximate with digits/scale == 0
	 * exists we could return the type with largest digits
	 *
	 * returning the largest when no exact match is found is now the
	 * (wrong?) default
	 */
	/* assumes the types are ordered on name,digits,scale where is always
	 * 0 > n
	 */
	node *m, *n;

	for (n = types->h; n; n = n->next) {
		sql_type *t = n->data;

		if (t->sqlname[0] == name[0] && strcmp(t->sqlname, name) == 0) {
			if ((digits && t->digits >= digits) || (digits == t->digits)) {
				sql_init_subtype(res, t, digits, scale);
				return 1;
			}
			for (m = n->next; m; m = m->next) {
				t = m->data;
				if (strcmp(t->sqlname, name) != 0) {
					break;
				}
				n = m;
				if ((digits && t->digits >= digits) || (digits == t->digits)) {
					sql_init_subtype(res, t, digits, scale);
					return 1;
				}
			}
			t = n->data;
			sql_init_subtype(res, t, digits, scale);
			return 1;
		}
	}
	return 0;
}

sql_subtype *
sql_bind_subtype(sql_allocator *sa, const char *name, unsigned int digits, unsigned int scale)
{
	sql_subtype *res = (sa)?SA_ZNEW(sa, sql_subtype):ZNEW(sql_subtype);
	
	if (!sql_find_subtype(res, name, digits, scale)) {
		return NULL;
	}
	return res;
}

char *
sql_subtype_string(sql_subtype *t)
{
	char buf[BUFSIZ];

	if (t->digits && t->scale)
		snprintf(buf, BUFSIZ, "%s(%u,%u)", t->type->sqlname, t->digits, t->scale);
	else if (t->digits && t->type->radix != 2)
		snprintf(buf, BUFSIZ, "%s(%u)", t->type->sqlname, t->digits);

	else
		snprintf(buf, BUFSIZ, "%s", t->type->sqlname);
	return _STRDUP(buf);
}

sql_subtype *
sql_bind_localtype(const char *name)
{
	node *n = localtypes->h;

	while (n) {
		sql_subtype *t = n->data;

		if (strcmp(t->type->base.name, name) == 0) {
			return t;
		}
		n = n->next;
	}
	assert(0);
	return NULL;
}

int
type_cmp(sql_type *t1, sql_type *t2)
{
	int res = 0;

	if (!t1 || !t2)
		return -1;
	/* types are only equal
	   iff they map onto the same systemtype */
	res = (t1->localtype - t2->localtype);
	if (res)
		return res;

	/* iff they fall into the same equivalence class */
	res = (t1->eclass - t2->eclass);
	if (res)
		return res;

	/* external types with the same system type are treated equaly */
	if (t1->eclass == EC_EXTERNAL)
		return res;

	/* sql base types need the same 'sql' name */
	return (strcmp(t1->sqlname, t2->sqlname));
}

int
subtype_cmp(sql_subtype *t1, sql_subtype *t2)
{
	if (!t1->type || !t2->type)
		return -1;
	if ( !(t1->type->eclass == t2->type->eclass && 
	       EC_INTERVAL(t1->type->eclass)) &&
	      (t1->digits != t2->digits || 
	      (!(t1->type->eclass == t2->type->eclass && 
	       t1->type->eclass == EC_FLT) &&
	       t1->scale != t2->scale)) )
		return -1;

	/* subtypes are only equal iff
	   they map onto the same systemtype */
	return (type_cmp(t1->type, t2->type));
}

int
is_subtype(sql_subtype *sub, sql_subtype *super)
/* returns true if sub is a sub type of super */
{
	if (!sub || !super)
		return 0;
	if (super->digits > 0 && sub->digits > super->digits) 
		return 0;
	if (super->digits == 0 && super->type->eclass == EC_STRING && 
	    (sub->type->eclass == EC_STRING || sub->type->eclass == EC_CHAR))
		return 1;
	if (super->digits != sub->digits && sub->type->eclass == EC_CHAR)
		return 0;
	/* subtypes are only equal iff
	   they map onto the same systemtype */
	return (type_cmp(sub->type, super->type) == 0);
}

static int
is_subtypeof(sql_subtype *sub, sql_subtype *super)
/* returns true if sub is a sub type of super */
{
	if (!sub || !super)
		return 0;
	if (super->digits > 0 && sub->digits > super->digits) 
		return 0;
	if (super->digits == 0 && super->type->eclass == EC_STRING && 
	    (sub->type->eclass == EC_STRING || sub->type->eclass == EC_CHAR))
		return 1;
	if (super->type->eclass == sub->type->eclass)
		return 1;
	/* subtypes are only equal iff
	   they map onto the same systemtype */
	return (type_cmp(sub->type, super->type) == 0);
}

char *
subtype2string(sql_subtype *t)
{
	char buf[BUFSIZ]; 

	if (t->digits > 0) {
		if (t->scale > 0)
			snprintf(buf, BUFSIZ, "%s(%u,%u)", 
				t->type->sqlname, t->digits, t->scale);
		else
			snprintf(buf, BUFSIZ, "%s(%u)", 
				t->type->sqlname, t->digits);
	} else {
			snprintf(buf, BUFSIZ, "%s", t->type->sqlname);
	}
	return _STRDUP(buf);
}

int 
subaggr_cmp( sql_subaggr *a1, sql_subaggr *a2)
{
	if (a1->aggr == a2->aggr) 
	    return list_cmp(a1->res, a2->res, (fcmp) &subtype_cmp);
	return -1;
}

int 
subfunc_cmp( sql_subfunc *f1, sql_subfunc *f2)
{
	if (f1->func == f2->func) 
	    return list_cmp(f1->res, f2->res, (fcmp) &subtype_cmp);
	return -1;
}

int
arg_subtype_cmp(sql_arg *a, sql_subtype *t)
{
	if (a->type.type->eclass == EC_ANY)
		return 0;
	return (is_subtype(t, &a->type )?0:-1);
}


static sql_subaggr *
_dup_subaggr(sql_allocator *sa, sql_func *a, sql_subtype *member)
{
	node *tn;
	unsigned int scale = 0, digits = 0;
	sql_subaggr *ares = SA_ZNEW(sa, sql_subaggr);

	assert (a->res);

	ares->aggr = a;
	ares->res = sa_list(sa);
	for(tn = a->res->h; tn; tn = tn->next) {
		sql_arg *rarg = tn->data;
		sql_subtype *res, *r = &rarg->type; 

		digits = r->digits;
		scale = r->scale;
		/* same scale as the input */
		if (member && (member->scale != scale ||
			(digits != member->digits && !EC_NUMBER(member->type->eclass)))) {
			if (member->digits > digits)
				digits = member->digits;
			scale = member->scale;
		}
		/* same type as the input */
		if (r->type->eclass == EC_ANY) 
			r = member;
		res = sql_create_subtype(sa, r->type, digits, scale);
		list_append(ares->res, res);
	}
	return ares;
}

sql_subaggr *
sql_bind_aggr(sql_allocator *sa, sql_schema *s, const char *sqlaname, sql_subtype *type)
{
	node *n = aggrs->h;

	while (n) {
		sql_func *a = n->data;
		sql_arg *arg = NULL;

		if (a->ops->h)
			arg = a->ops->h->data;

		if (strcmp(a->base.name, sqlaname) == 0 && (!arg ||
		    arg->type.type->eclass == EC_ANY || 
		    (type && is_subtype(type, &arg->type )))) 
			return _dup_subaggr(sa, a, type);
		n = n->next;
	}
	if (s) {
		node *n;

		if (s->funcs.set) for (n=s->funcs.set->h; n; n = n->next) {
			sql_func *a = n->data;
			sql_arg *arg = NULL;

			if ((!IS_AGGR(a) || !a->res))
				continue;

			if (a->ops->h)
				arg = a->ops->h->data;

			if (strcmp(a->base.name, sqlaname) == 0 && (!arg ||
		    	 	arg->type.type->eclass == EC_ANY || 
		    		(type && is_subtype(type, &arg->type )))) 
				return _dup_subaggr(sa, a, type);
		}
	}
	return NULL;
}

sql_subaggr *
sql_bind_aggr_(sql_allocator *sa, sql_schema *s, const char *sqlaname, list *ops)
{
	node *n = aggrs->h;
	sql_subtype *type = NULL;

	if (ops->h)
		type = ops->h->data;

	while (n) {
		sql_func *a = n->data;

		if (strcmp(a->base.name, sqlaname) == 0 &&  
		    list_cmp(a->ops, ops, (fcmp) &arg_subtype_cmp) == 0)
			return _dup_subaggr(sa, a, type);
		n = n->next;
	}
	if (s) {
		node *n;

		if (s->funcs.set) for (n=s->funcs.set->h; n; n = n->next) {
			sql_func *a = n->data;

			if ((!IS_AGGR(a) || !a->res))
				continue;

			if (strcmp(a->base.name, sqlaname) == 0 &&  
		    	    list_cmp(a->ops, ops, (fcmp) &arg_subtype_cmp) == 0)
				return _dup_subaggr(sa, a, type);
		}
	}
	return NULL;
}

sql_subaggr *
sql_bind_member_aggr(sql_allocator *sa, sql_schema *s, const char *sqlaname, sql_subtype *type, int nrargs)
{
	node *n = aggrs->h;

	while (n) {
		sql_func *a = n->data;

		if (strcmp(a->base.name, sqlaname) == 0 && list_length(a->ops) == nrargs &&
		    arg_subtype_cmp(a->ops->h->data, type) == 0)
			return _dup_subaggr(sa, a, NULL);
		n = n->next;
	}
	if (s) {
		node *n;

		if (s->funcs.set) for (n=s->funcs.set->h; n; n = n->next) {
			sql_func *a = n->data;

			if ((!IS_AGGR(a) || !a->res))
				continue;

			if (strcmp(a->base.name, sqlaname) == 0 && list_length(a->ops) == nrargs &&
		    	    arg_subtype_cmp(a->ops->h->data, type) == 0)
				return _dup_subaggr(sa, a, NULL);
		}
	}
	return NULL;
}

sql_subaggr *
sql_find_aggr(sql_allocator *sa, sql_schema *s, const char *sqlaname)
{
	node *n = aggrs->h;

	(void)s;
	while (n) {
		sql_func *a = n->data;

		if (strcmp(a->base.name, sqlaname) == 0)
			return _dup_subaggr(sa, a, NULL);
		n = n->next;
	}
	if (s) {
		node *n;

		if (s->funcs.set) for (n=s->funcs.set->h; n; n = n->next) {
			sql_func *a = n->data;

			if ((!IS_AGGR(a) || !a->res))
				continue;

			if (strcmp(a->base.name, sqlaname) == 0) 
				return _dup_subaggr(sa, a, NULL);
		}
	}
	return NULL;
}

char *
sql_func_imp(sql_func *f)
{
	if (f->sql)
		return f->base.name;
	else
		return f->imp;
}

char *
sql_func_mod(sql_func *f)
{
	return f->mod;
}

int
is_sqlfunc(sql_func *f)
{
	return f->sql;
}

sql_subfunc*
sql_dup_subfunc(sql_allocator *sa, sql_func *f, list *ops, sql_subtype *member)
{
	node *tn;
	unsigned int scale = 0, digits = 0;
	sql_subfunc *fres = SA_ZNEW(sa, sql_subfunc);

	fres->func = f;
	if (IS_FILT(f)) {
		fres->res = sa_list(sa);
		list_append(fres->res, sql_bind_localtype("bit"));
	} else if (IS_FUNC(f) || IS_UNION(f) || IS_ANALYTIC(f)) { /* not needed for PROC */
		unsigned int mscale = 0, mdigits = 0;

		if (ops) {
			if (ops->h && ops->h->data && f->imp &&
			    strcmp(f->imp, "round") == 0) {
				/* special case for round(): result is
				 * same type as first argument */
				sql_subtype *a = ops->h->data;
				mscale = a->scale;
				mdigits = a->digits;
			} else {
				for (tn = ops->h; tn; tn = tn->next) {
					sql_subtype *a = tn->data;

					/* same scale as the input */
					if (a && a->scale > mscale)
						mscale = a->scale;
					if (a && f->fix_scale == INOUT)
						mdigits = a->digits;
				}
			}
		}

		if (!member) {
			node *m;
			sql_arg *ma = NULL;

			if (ops) for (tn = ops->h, m = f->ops->h; tn; tn = tn->next, m = m->next) {
				sql_arg *s = m->data;

				if (!member && s->type.type->eclass == EC_ANY) {
					member = tn->data;
					ma = s;
				}
				/* largest type */
				if (member && s->type.type->eclass == EC_ANY &&
				    s->type.type->localtype > ma->type.type->localtype ) {
					member = tn->data;
					ma = s;
				}
			}
		}

		if (f->res) {
			fres->res = sa_list(sa);
			for(tn = f->res->h; tn; tn = tn->next) {
				sql_arg *rarg = tn->data;
				sql_subtype *res, *r = &rarg->type;
	
				/* same scale as the input */
				if (member && member->scale > scale)
					scale = member->scale;
				digits = r->digits;
				if (!member) {
					if (f->fix_scale > SCALE_NONE && f->fix_scale < SCALE_EQ) {
						scale = mscale;
						digits = mdigits;
					} else if (r->scale) 
						scale = r->scale;
				}
				if (member && f->fix_scale == INOUT)
					digits = member->digits;
				if (member && r->type->eclass == EC_ANY) 
					r = member;
				res = sql_create_subtype(sa, r->type, digits, scale);
				list_append(fres->res, res);
			}
		}
		if (member) { /* check that the types of all EC_ANY's are equal */
			sql_subtype *st = NULL;
			node *m;

			if (ops) for (tn = ops->h, m = f->ops->h; tn; tn = tn->next, m = m->next) {
				sql_arg *s = m->data;

				if (s->type.type->eclass == EC_ANY) {
					if (!st)
						st = tn->data;
					else if (subtype_cmp(st, tn->data))
						return NULL;
				}
			}
		}
	}
	return fres;
}

static sql_subfunc *
func_cmp(sql_allocator *sa, sql_func *f, const char *name, int nrargs) 
{
	if (strcmp(f->base.name, name) == 0) {
		if (f->vararg) 
			return sql_dup_subfunc(sa, f, NULL, NULL);
		if (nrargs < 0 || list_length(f->ops) == nrargs) 
			return sql_dup_subfunc(sa, f, NULL, NULL);
	}
	return NULL;
}

sql_subfunc *
sql_find_func(sql_allocator *sa, sql_schema *s, const char *sqlfname, int nrargs, int type, sql_subfunc *prev)
{
	sql_subfunc *fres;
	int key = hash_key(sqlfname);
	sql_hash_e *he;
	int found = 0;
	int filt = (type == F_FUNC)?F_FILT:type;

	assert(nrargs);
	MT_lock_set(&funcs->ht_lock);
	he = funcs->ht->buckets[key&(funcs->ht->size-1)]; 
	if (prev) {
		for (; he && !found; he = he->chain) 
			if (he->value == prev->func)
				found = 1;
		if (found)
			he = he->chain;
	}
	for (; he; he = he->chain) {
		sql_func *f = he->value;

		if (f->type != type && f->type != filt) 
			continue;
		if ((fres = func_cmp(sa, f, sqlfname, nrargs )) != NULL) {
			MT_lock_unset(&funcs->ht_lock);
			return fres;
		}
	}
	MT_lock_unset(&funcs->ht_lock);
	if (s) {
		node *n;
		/*
		sql_func * f = find_sql_func(s, sqlfname);

		if (f && f->type == type && (fres = func_cmp(sa, f, sqlfname, nrargs )) != NULL)
			return fres;
			*/
		if (s->funcs.set) {
			MT_lock_set(&s->funcs.set->ht_lock);
			if (s->funcs.set->ht) {
				he = s->funcs.set->ht->buckets[key&(s->funcs.set->ht->size-1)];
				if (prev) {
					for (; he && !found; he = he->chain) 
						if (he->value == prev->func)
							found = 1;
					if (found)
						he = he->chain;
				}
				for (; he; he = he->chain) {
					sql_func *f = he->value;

					if (f->type != type && f->type != filt) 
						continue;
					if ((fres = func_cmp(sa, f, sqlfname, nrargs )) != NULL) {
						MT_lock_unset(&s->funcs.set->ht_lock);
						return fres;
					}
				}
				MT_lock_unset(&s->funcs.set->ht_lock);
			} else {
				MT_lock_unset(&s->funcs.set->ht_lock);
				n = s->funcs.set->h;
				if (prev) {
					for (; n && !found; n = n->next) 
						if (n->data == prev)
							found = 1;
					if (found)
						n = n->next;
				}
				for (; n; n = n->next) {
					sql_func *f = n->data;

					if (f->type != type && f->type != filt) 
						continue;
					if ((fres = func_cmp(sa, f, sqlfname, nrargs )) != NULL)
						return fres;
				}
			}
		}
	}
	return NULL;
}

list *
sql_find_funcs(sql_allocator *sa, sql_schema *s, const char *sqlfname, int nrargs, int type)
{
	sql_subfunc *fres;
	int key = hash_key(sqlfname);
	sql_hash_e *he;
	int filt = (type == F_FUNC)?F_FILT:type;
	list *res = sa_list(sa);

	assert(nrargs);
	MT_lock_set(&funcs->ht_lock);
	he = funcs->ht->buckets[key&(funcs->ht->size-1)]; 
	for (; he; he = he->chain) {
		sql_func *f = he->value;

		if (f->type != type && f->type != filt) 
			continue;
		if ((fres = func_cmp(sa, f, sqlfname, nrargs )) != NULL) 
			list_append(res, fres);
	}
	MT_lock_unset(&funcs->ht_lock);
	if (s) {
		node *n;

		if (s->funcs.set) {
			MT_lock_set(&s->funcs.set->ht_lock);
			if (s->funcs.set->ht) {
				he = s->funcs.set->ht->buckets[key&(s->funcs.set->ht->size-1)];
				for (; he; he = he->chain) {
					sql_func *f = he->value;

					if (f->type != type && f->type != filt) 
						continue;
					if ((fres = func_cmp(sa, f, sqlfname, nrargs )) != NULL)
						list_append(res, fres);
				}
			} else {
				n = s->funcs.set->h;
				for (; n; n = n->next) {
					sql_func *f = n->data;

					if (f->type != type && f->type != filt) 
						continue;
					if ((fres = func_cmp(sa, f, sqlfname, nrargs )) != NULL)
						list_append(res, fres);
				}
			}
			MT_lock_unset(&s->funcs.set->ht_lock);
		}
	}
	return res;
}


/* find function based on first argument */
sql_subfunc *
sql_bind_member(sql_allocator *sa, sql_schema *s, const char *sqlfname, sql_subtype *tp, int nrargs, sql_subfunc *prev)
{
	node *n = funcs->h;
	int found = 1;

	assert(nrargs);
	if (prev) {
		found = 0;
		for(; n && !found; n = n->next)
			if (n->data == prev->func)
				found = 1;
		if (n)
			n = n->next;
	}
	for (; n; n = n->next) {
		sql_func *f = n->data;

		if (!f->res && !IS_FILT(f))
			continue;
		if (strcmp(f->base.name, sqlfname) == 0) {
			if (list_length(f->ops) == nrargs && is_subtypeof(tp, &((sql_arg *) f->ops->h->data)->type)) 
				return sql_dup_subfunc(sa, f, NULL, tp);
		}
	}
	if (s) {
		node *n;

		if (!s->funcs.set) 
			return NULL;
		n = s->funcs.set->h;
		if (prev && !found) {
			for(; n && !found; n = n->next)
				if (n->data == prev->func)
					found = 1;
			if (n)
				n = n->next;
		}
		for (; n; n = n->next) {
			sql_func *f = n->data;

			if (!f->res && !IS_FILT(f))
				continue;
			if (strcmp(f->base.name, sqlfname) == 0) {
				if (list_length(f->ops) == nrargs && is_subtypeof(tp, &((sql_arg *) f->ops->h->data)->type)) 
					return sql_dup_subfunc(sa, f, NULL, tp);
			}
		}
	}
	return NULL;
}

sql_subfunc *
sql_bind_func(sql_allocator *sa, sql_schema *s, const char *sqlfname, sql_subtype *tp1, sql_subtype *tp2, int type)
{
	list *l = sa_list(sa);
	sql_subfunc *fres;

	if (tp1)
		list_append(l, tp1);
	if (tp2)
		list_append(l, tp2);

	fres = sql_bind_func_(sa, s, sqlfname, l, type);
	return fres;
}

sql_subfunc *
sql_bind_func3(sql_allocator *sa, sql_schema *s, const char *sqlfname, sql_subtype *tp1, sql_subtype *tp2, sql_subtype *tp3, int type)
{
	list *l = sa_list(sa);
	sql_subfunc *fres;

	if (tp1)
		list_append(l, tp1);
	if (tp2)
		list_append(l, tp2);
	if (tp3)
		list_append(l, tp3);

	fres = sql_bind_func_(sa, s, sqlfname, l, type);
	return fres;
}

sql_subfunc *
sql_bind_func_(sql_allocator *sa, sql_schema *s, const char *sqlfname, list *ops, int type)
{
	node *n = funcs->h;
	int filt = (type == F_FUNC)?F_FILT:type;

	(void)s;
	for (; n; n = n->next) {
		sql_func *f = n->data;

		if (f->type != type && f->type != filt) 
			continue;
		if (strcmp(f->base.name, sqlfname) == 0) {
			if (list_cmp(f->ops, ops, (fcmp) &arg_subtype_cmp) == 0) 
				return sql_dup_subfunc(sa, f, ops, NULL);
		}
	}
	if (s) {
		node *n;

		if (s->funcs.set) for (n=s->funcs.set->h; n; n = n->next) {
			sql_func *f = n->data;

			if (f->type != type && f->type != filt) 
				continue;
			if (strcmp(f->base.name, sqlfname) == 0) {
				if (list_cmp(f->ops, ops, (fcmp) &arg_subtype_cmp) == 0) 
					return sql_dup_subfunc(sa, f, ops, NULL);
			}
		}
	}
	return NULL;
}

sql_subfunc *
sql_bind_func_result(sql_allocator *sa, sql_schema *s, const char *sqlfname, sql_subtype *tp1, sql_subtype *tp2, sql_subtype *res)
{
	list *l = sa_list(sa);
	sql_subfunc *fres;

	if (tp1)
		list_append(l, tp1);
	if (tp2)
		list_append(l, tp2);

	fres = sql_bind_func_result_(sa, s, sqlfname, l, res);
	return fres;
}

sql_subfunc *
sql_bind_func_result3(sql_allocator *sa, sql_schema *s, const char *sqlfname, sql_subtype *tp1, sql_subtype *tp2, sql_subtype *tp3, sql_subtype *res)
{
	list *l = sa_list(sa);
	sql_subfunc *fres;

	if (tp1)
		list_append(l, tp1);
	if (tp2)
		list_append(l, tp2);
	if (tp3)
		list_append(l, tp3);

	fres = sql_bind_func_result_(sa, s, sqlfname, l, res);
	return fres;
}


sql_subfunc *
sql_bind_func_result_(sql_allocator *sa, sql_schema *s, const char *sqlfname, list *ops, sql_subtype *res)
{
	node *n = funcs->h;

	for (; n; n = n->next) {
		sql_func *f = n->data;
		sql_arg *firstres = NULL;

		if (!f->res && !IS_FILT(f))
			continue;
		firstres = IS_FILT(f)?BIT:f->res->h->data;
		if (strcmp(f->base.name, sqlfname) == 0 && (is_subtype(&firstres->type, res) || firstres->type.type->eclass == EC_ANY) && list_cmp(f->ops, ops, (fcmp) &arg_subtype_cmp) == 0) 
			return sql_dup_subfunc(sa, f, ops, NULL);
	}
	if (s && s->funcs.set)
		n = s->funcs.set->h;
	for (; n; n = n->next) {
		sql_func *f = n->data;
		sql_arg *firstres = NULL;

		if (!f->res && !IS_FILT(f))
			continue;
		firstres = IS_FILT(f)?BIT:f->res->h->data;
		if (strcmp(f->base.name, sqlfname) == 0 && (is_subtype(&firstres->type, res) || firstres->type.type->eclass == EC_ANY) && list_cmp(f->ops, ops, (fcmp) &arg_subtype_cmp) == 0) 
			return sql_dup_subfunc(sa, f, ops, NULL);
	}
	return NULL;
}

static void
sql_create_alias(sql_allocator *sa, const char *name, const char *alias)
{
	sql_alias *a = SA_ZNEW(sa, sql_alias);

	if(a) {
		a->name = sa_strdup(sa, name);
		a->alias = sa_strdup(sa, alias);
		list_append(aliases, a);
		if (!keyword_exists(a->alias) )
			(void) keywords_insert(a->alias, KW_ALIAS);
	}
}

char *
sql_bind_alias(const char *alias)
{
	node *n;

	for (n = aliases->h; n; n = n->next) {
		sql_alias *a = n->data;

		if (strcmp(a->alias, alias) == 0) {
			return a->name;
		}
	}
	return NULL;
}


sql_type *
sql_create_type(sql_allocator *sa, const char *sqlname, unsigned int digits, unsigned int scale, unsigned char radix, unsigned char eclass, const char *name)
{
	sql_type *t = SA_ZNEW(sa, sql_type);

	base_init(sa, &t->base, store_next_oid(), TR_OLD, name);
	t->sqlname = sa_strdup(sa, sqlname);
	t->digits = digits;
	t->scale = scale;
	t->localtype = ATOMindex(t->base.name);
	t->radix = radix;
	t->eclass = eclass;
	t->s = NULL;
	if (!keyword_exists(t->sqlname) && !EC_INTERVAL(eclass))
		(void) keywords_insert(t->sqlname, KW_TYPE);
	list_append(types, t);

	list_append(localtypes, sql_create_subtype(sa, t, 0, 0));

	return t;
}

static sql_arg *
create_arg(sql_allocator *sa, const char *name, sql_subtype *t, char inout)
{
	sql_arg *a = (sa)?SA_ZNEW(sa, sql_arg):ZNEW(sql_arg);

	if(a) {
		a->name = name?sa_strdup(sa, name):NULL;
		a->type = *t;
		a->inout = inout;
	}
	return a;
}

sql_arg *
sql_create_arg(sql_allocator *sa, const char *name, sql_subtype *t, char inout)
{
	return create_arg(sa, name, t, inout);
}

sql_arg *
arg_dup(sql_allocator *sa, sql_arg *oa)
{
	sql_arg *a = SA_ZNEW(sa, sql_arg);

	if(a) {
		a->name = sa_strdup(sa, oa->name);
		a->type = oa->type;
		a->inout = oa->inout;
	}
	return a;
}

sql_func *
sql_create_aggr(sql_allocator *sa, const char *name, const char *mod, const char *imp, sql_type *tpe, sql_type *res)
{
	list *l = sa_list(sa);
	sql_arg *sres;

	if (tpe)
		list_append(l, create_arg(sa, NULL, sql_create_subtype(sa, tpe, 0, 0), ARG_IN));
	assert(res);
	sres = create_arg(sa, NULL, sql_create_subtype(sa, res, 0, 0), ARG_OUT);
	return sql_create_func_(sa, name, mod, imp, l, sres, FALSE, F_AGGR, SCALE_NONE);
}

sql_func *
sql_create_aggr2(sql_allocator *sa, const char *name, const char *mod, const char *imp, sql_type *tp1, sql_type *tp2, sql_type *res)
{
	list *l = sa_list(sa);
	sql_arg *sres;

	list_append(l, create_arg(sa, NULL, sql_create_subtype(sa, tp1, 0, 0), ARG_IN));
	list_append(l, create_arg(sa, NULL, sql_create_subtype(sa, tp2, 0, 0), ARG_IN));
	assert(res);
	sres = create_arg(sa, NULL, sql_create_subtype(sa, res, 0, 0), ARG_OUT);
	return sql_create_func_(sa, name, mod, imp, l, sres, FALSE, F_AGGR, SCALE_NONE);
}

sql_func *
sql_create_func(sql_allocator *sa, const char *name, const char *mod, const char *imp, sql_type *tpe1, sql_type *tpe2, sql_type *res, int fix_scale)
{
	list *l = sa_list(sa);
	sql_arg *sres;

	if (tpe1)
		list_append(l,create_arg(sa, NULL, sql_create_subtype(sa, tpe1, 0, 0), ARG_IN));
	if (tpe2)
		list_append(l,create_arg(sa, NULL, sql_create_subtype(sa, tpe2, 0, 0), ARG_IN));

	sres = create_arg(sa, NULL, sql_create_subtype(sa, res, 0, 0), ARG_OUT);
	return sql_create_func_(sa, name, mod, imp, l, sres, FALSE, F_FUNC, fix_scale);
}

static sql_func *
sql_create_func_res(sql_allocator *sa, const char *name, const char *mod, const char *imp, sql_type *tpe1, sql_type *tpe2, sql_type *res, int fix_scale, int scale)
{
	list *l = sa_list(sa);
	sql_arg *sres;

	if (tpe1)
		list_append(l,create_arg(sa, NULL, sql_create_subtype(sa, tpe1, 0, 0), ARG_IN));
	if (tpe2)
		list_append(l,create_arg(sa, NULL, sql_create_subtype(sa, tpe2, 0, 0), ARG_IN));

	sres = create_arg(sa, NULL, sql_create_subtype(sa, res, 0, 0), ARG_OUT);
	sres->type.scale = scale;
	return sql_create_func_(sa, name, mod, imp, l, sres, FALSE, F_FUNC, fix_scale);
}

sql_func *
sql_create_funcSE(sql_allocator *sa, const char *name, const char *mod, const char *imp, sql_type *tpe1, sql_type *tpe2, sql_type *res, int fix_scale)
{
	list *l = sa_list(sa);
	sql_arg *sres;

	if (tpe1)
		list_append(l,create_arg(sa, NULL, sql_create_subtype(sa, tpe1, 0, 0), ARG_IN));
	if (tpe2)
		list_append(l,create_arg(sa, NULL, sql_create_subtype(sa, tpe2, 0, 0), ARG_IN));

	sres = create_arg(sa, NULL, sql_create_subtype(sa, res, 0, 0), ARG_OUT);
	return sql_create_func_(sa, name, mod, imp, l, sres, TRUE, F_FUNC, fix_scale);
}


sql_func *
sql_create_func3(sql_allocator *sa, const char *name, const char *mod, const char *imp, sql_type *tpe1, sql_type *tpe2, sql_type *tpe3, sql_type *res, int fix_scale)
{
	list *l = sa_list(sa);
	sql_arg *sres;

	list_append(l, create_arg(sa, NULL, sql_create_subtype(sa, tpe1, 0, 0), ARG_IN));
	list_append(l, create_arg(sa, NULL, sql_create_subtype(sa, tpe2, 0, 0), ARG_IN));
	list_append(l, create_arg(sa, NULL, sql_create_subtype(sa, tpe3, 0, 0), ARG_IN));
	sres = create_arg(sa, NULL, sql_create_subtype(sa, res, 0, 0), ARG_OUT);
	return sql_create_func_(sa, name, mod, imp, l, sres, FALSE, F_FUNC, fix_scale);
}

static sql_func *
sql_create_analytic(sql_allocator *sa, const char *name, const char *mod, const char *imp, sql_type *tpe1, sql_type *tpe2, sql_type *tpe3, sql_type *res, int fix_scale)
{
	list *l = sa_list(sa);
	sql_arg *sres;

	list_append(l, create_arg(sa, NULL, sql_create_subtype(sa, tpe1, 0, 0), ARG_IN));
	if (tpe2)
		list_append(l, create_arg(sa, NULL, sql_create_subtype(sa, tpe2, 0, 0), ARG_IN));
	if (tpe3)
		list_append(l, create_arg(sa, NULL, sql_create_subtype(sa, tpe3, 0, 0), ARG_IN));
	sres = create_arg(sa, NULL, sql_create_subtype(sa, res, 0, 0), ARG_OUT);
	return sql_create_func_(sa, name, mod, imp, l, sres, FALSE, F_ANALYTIC, fix_scale);
}

sql_func *
sql_create_func4(sql_allocator *sa, const char *name, const char *mod, const char *imp, sql_type *tpe1, sql_type *tpe2, sql_type *tpe3, sql_type *tpe4, sql_type *res, int fix_scale)
{
	list *l = sa_list(sa);
	sql_arg *sres;

	list_append(l, create_arg(sa, NULL, sql_create_subtype(sa, tpe1, 0, 0), ARG_IN));
	list_append(l, create_arg(sa, NULL, sql_create_subtype(sa, tpe2, 0, 0), ARG_IN));
	list_append(l, create_arg(sa, NULL, sql_create_subtype(sa, tpe3, 0, 0), ARG_IN));
	list_append(l, create_arg(sa, NULL, sql_create_subtype(sa, tpe4, 0, 0), ARG_IN));
	sres = create_arg(sa, NULL, sql_create_subtype(sa, res, 0, 0), ARG_OUT);
	return sql_create_func_(sa, name, mod, imp, l, sres, FALSE, F_FUNC, fix_scale);
}


sql_func *
sql_create_func_(sql_allocator *sa, const char *name, const char *mod, const char *imp, list *ops, sql_arg *res, bit side_effect, int type, int fix_scale)
{
	sql_func *t = SA_ZNEW(sa, sql_func);

	if (!ops)
		ops = sa_list(sa);
	base_init(sa, &t->base, store_next_oid(), TR_OLD, name);
	t->imp = sa_strdup(sa, imp);
	t->mod = sa_strdup(sa, mod);
	t->ops = ops;
	t->type = type;
	if (res) {
		t->res = sa_list(sa);
		list_append(t->res, res);
	} else {
		t->res = NULL;
	}
	t->nr = list_length(funcs);
	t->sql = 0;
	t->lang = FUNC_LANG_INT;
	t->side_effect = side_effect;
	t->fix_scale = fix_scale;
	t->s = NULL;
	if (type == F_AGGR) {
		list_append(aggrs, t);
	} else {
		list_append(funcs, t);
	}
	return t;
}

sql_func *
sql_create_sqlfunc(sql_allocator *sa, const char *name, const char *imp, list *ops, sql_arg *res)
{
	sql_func *t = SA_ZNEW(sa, sql_func);

	assert(res && ops);
	base_init(sa, &t->base, store_next_oid(), TR_OLD, name);
	t->imp = sa_strdup(sa, imp);
	t->mod = sa_strdup(sa, "SQL");
	t->ops = ops;
	if (res) {	
		t->res = sa_list(sa);
		list_append(t->res, res);
		t->type = F_FUNC;
	} else {
		t->res = NULL;
		t->type = F_PROC;
	}
	t->nr = list_length(funcs);
	t->sql = 1;
	t->lang = FUNC_LANG_SQL;
	t->side_effect = FALSE;
	list_append(funcs, t);
	return t;
}

/* SQL service initialization
This C-code version initializes the
parser catalogs with typing information. Although, in principle,
many of the function signatures can be obtained from the underlying
database kernel, we have chosen for this explicit scheme for one
simple reason. The SQL standard dictates the types and we have to
check their availability in the kernel only. The kernel itself could
include many functions for which there is no standard.
*/

static void
sqltypeinit( sql_allocator *sa)
{
	sql_type *ts[100];
	sql_type **strings, **numerical;
	sql_type **decimals, **floats, **dates, **end, **t;
	sql_type *STR, *BTE, *SHT, *INT, *LNG, *OID, *FLT, *DBL, *DEC;
#ifdef HAVE_HGE
	sql_type *HGE = NULL;
#endif
	sql_type *SECINT, *MONINT, *DTE; 
	sql_type *TME, *TMETZ, *TMESTAMP, *TMESTAMPTZ;
	sql_type *ANY, *TABLE;
	sql_type *GEOM, *MBR;
	sql_func *f;
	sql_arg *sres;
	sql_type *LargestINT, *LargestDEC;

	ANY = sql_create_type(sa, "ANY", 0, 0, 0, EC_ANY, "void");

	t = ts;
	TABLE = *t++ = sql_create_type(sa, "TABLE", 0, 0, 0, EC_TABLE, "bat");
	*t++ = sql_create_type(sa, "PTR", 0, 0, 0, EC_TABLE, "ptr");

	BIT = *t++ = sql_create_type(sa, "BOOLEAN", 1, 0, 2, EC_BIT, "bit");
	sql_create_alias(sa, BIT->sqlname, "BOOL");

	strings = t;
	*t++ = sql_create_type(sa, "CHAR",    0, 0, 0, EC_CHAR,   "str");
	STR = *t++ = sql_create_type(sa, "VARCHAR", 0, 0, 0, EC_STRING, "str");
	*t++ = sql_create_type(sa, "CLOB",    0, 0, 0, EC_STRING, "str");

	numerical = t;
#if SIZEOF_OID == SIZEOF_INT
	OID = *t++ = sql_create_type(sa, "OID", 31, 0, 2, EC_POS, "oid");
#endif
#if SIZEOF_OID == SIZEOF_LNG
	OID = *t++ = sql_create_type(sa, "OID", 63, 0, 2, EC_POS, "oid");
#endif

	BTE = *t++ = sql_create_type(sa, "TINYINT",   8, SCALE_FIX, 2, EC_NUM, "bte");
	SHT = *t++ = sql_create_type(sa, "SMALLINT", 16, SCALE_FIX, 2, EC_NUM, "sht");
	INT = *t++ = sql_create_type(sa, "INT",      32, SCALE_FIX, 2, EC_NUM, "int");
	LargestINT =
	LNG = *t++ = sql_create_type(sa, "BIGINT",   64, SCALE_FIX, 2, EC_NUM, "lng");
#ifdef HAVE_HGE
	if (have_hge) {
		LargestINT =
		HGE = *t++ = sql_create_type(sa, "HUGEINT",  128, SCALE_FIX, 2, EC_NUM, "hge");
	}
#endif

	decimals = t;
	/* decimal(d,s) (d indicates nr digits,
	   s scale indicates nr of digits after the dot .) */
	*t++ = sql_create_type(sa, "DECIMAL",  2, SCALE_FIX, 10, EC_DEC, "bte");
	*t++ = sql_create_type(sa, "DECIMAL",  4, SCALE_FIX, 10, EC_DEC, "sht");
	DEC =
	*t++ = sql_create_type(sa, "DECIMAL",  9, SCALE_FIX, 10, EC_DEC, "int");
	LargestDEC =
	*t++ = sql_create_type(sa, "DECIMAL", 18, SCALE_FIX, 10, EC_DEC, "lng");
#ifdef HAVE_HGE
	if (have_hge) {
		LargestDEC =
		*t++ = sql_create_type(sa, "DECIMAL", 38, SCALE_FIX, 10, EC_DEC, "hge");
	}
#endif

	/* float(n) (n indicates precision of atleast n digits) */
	/* ie n <= 23 -> flt */
	/*    n <= 51 -> dbl */
	/*    n <= 62 -> long long dbl (with -ieee) (not supported) */
	/* this requires a type definition */

	floats = t;
	FLT = *t++ = sql_create_type(sa, "REAL", 24, SCALE_NOFIX, 2, EC_FLT, "flt");
	DBL = *t++ = sql_create_type(sa, "DOUBLE", 53, SCALE_NOFIX, 2, EC_FLT, "dbl");

	dates = t;
	MONINT = *t++ = sql_create_type(sa, "MONTH_INTERVAL", 32, 0, 2, EC_MONTH, "int");
	SECINT = *t++ = sql_create_type(sa, "SEC_INTERVAL", 13, SCALE_FIX, 10, EC_SEC, "lng");
	TME = *t++ = sql_create_type(sa, "TIME", 7, 0, 0, EC_TIME, "daytime");
	TMETZ = *t++ = sql_create_type(sa, "TIMETZ", 7, SCALE_FIX, 0, EC_TIME, "daytime");
	DTE = *t++ = sql_create_type(sa, "DATE", 0, 0, 0, EC_DATE, "date");
	TMESTAMP = *t++ = sql_create_type(sa, "TIMESTAMP", 7, 0, 0, EC_TIMESTAMP, "timestamp");
	TMESTAMPTZ = *t++ = sql_create_type(sa, "TIMESTAMPTZ", 7, SCALE_FIX, 0, EC_TIMESTAMP, "timestamp");

	*t++ = sql_create_type(sa, "BLOB", 0, 0, 0, EC_BLOB, "sqlblob");

	if (geomcatalogfix_get() != NULL) {
		// the geom module is loaded 
		GEOM = *t++ = sql_create_type(sa, "GEOMETRY", 0, SCALE_NONE, 0, EC_GEOM, "wkb");
		/*POINT =*/ //*t++ = sql_create_type(sa, "POINT", 0, SCALE_FIX, 0, EC_GEOM, "wkb");
		*t++ = sql_create_type(sa, "GEOMETRYA", 0, SCALE_NONE, 0, EC_EXTERNAL, "wkba");

		MBR = *t++ = sql_create_type(sa, "MBR", 0, SCALE_NONE, 0, EC_EXTERNAL, "mbr");
		
		/* mbr operator functions */
		sql_create_func(sa, "mbr_overlap", "geom", "mbrOverlaps", GEOM, GEOM, BIT, SCALE_FIX);
		sql_create_func(sa, "mbr_overlap", "geom", "mbrOverlaps", MBR, MBR, BIT, SCALE_FIX);
		sql_create_func(sa, "mbr_above", "geom", "mbrAbove", GEOM, GEOM, BIT, SCALE_FIX);
		sql_create_func(sa, "mbr_above", "geom", "mbrAbove", MBR, MBR, BIT, SCALE_FIX);
		sql_create_func(sa, "mbr_below", "geom", "mbrBelow", GEOM, GEOM, BIT, SCALE_FIX);
		sql_create_func(sa, "mbr_below", "geom", "mbrBelow", MBR, MBR, BIT, SCALE_FIX);
		sql_create_func(sa, "mbr_right", "geom", "mbrRight", GEOM, GEOM, BIT, SCALE_FIX);
		sql_create_func(sa, "mbr_right", "geom", "mbrRight", MBR, MBR, BIT, SCALE_FIX);
		sql_create_func(sa, "mbr_left", "geom", "mbrLeft", GEOM, GEOM, BIT, SCALE_FIX);
		sql_create_func(sa, "mbr_left", "geom", "mbrLeft", MBR, MBR, BIT, SCALE_FIX);
		sql_create_func(sa, "mbr_overlap_or_above", "geom", "mbrOverlapOrAbove", GEOM, GEOM, BIT, SCALE_FIX);
		sql_create_func(sa, "mbr_overlap_or_above", "geom", "mbrOverlapOrAbove", MBR, MBR, BIT, SCALE_FIX);
		sql_create_func(sa, "mbr_overlap_or_below", "geom", "mbrOverlapOrBelow", GEOM, GEOM, BIT, SCALE_FIX);
		sql_create_func(sa, "mbr_overlap_or_below", "geom", "mbrOverlapOrBelow", MBR, MBR, BIT, SCALE_FIX);
		sql_create_func(sa, "mbr_overlap_or_right", "geom", "mbrOverlapOrRight", GEOM, GEOM, BIT, SCALE_FIX);
		sql_create_func(sa, "mbr_overlap_or_right", "geom", "mbrOverlapOrRight", MBR, MBR, BIT, SCALE_FIX);
		sql_create_func(sa, "mbr_overlap_or_left", "geom", "mbrOverlapOrLeft", GEOM, GEOM, BIT, SCALE_FIX);
		sql_create_func(sa, "mbr_overlap_or_left", "geom", "mbrOverlapOrLeft", MBR, MBR, BIT, SCALE_FIX);
		sql_create_func(sa, "mbr_contains", "geom", "mbrContains", GEOM, GEOM, BIT, SCALE_FIX);
		sql_create_func(sa, "mbr_contains", "geom", "mbrContains", MBR, MBR, BIT, SCALE_FIX);
		sql_create_func(sa, "mbr_contained", "geom", "mbrContained", GEOM, GEOM, BIT, SCALE_FIX);
		sql_create_func(sa, "mbr_contained", "geom", "mbrContained", MBR, MBR, BIT, SCALE_FIX);
		sql_create_func(sa, "mbr_equal", "geom", "mbrEqual", GEOM, GEOM, BIT, SCALE_FIX);
		sql_create_func(sa, "mbr_equal", "geom", "mbrEqual", MBR, MBR, BIT, SCALE_FIX);
		sql_create_func(sa, "mbr_distance", "geom", "mbrDistance", GEOM, GEOM, DBL, SCALE_FIX);
		sql_create_func(sa, "mbr_distance", "geom", "mbrDistance", MBR, MBR, DBL, SCALE_FIX);
		sql_create_func(sa, "left_shift", "geom", "mbrLeft", GEOM, GEOM, BIT, SCALE_FIX);
		sql_create_func(sa, "left_shift", "geom", "mbrLeft", MBR, MBR, BIT, SCALE_FIX);
		sql_create_func(sa, "right_shift", "geom", "mbrRight", GEOM, GEOM, BIT, SCALE_FIX);
		sql_create_func(sa, "right_shift", "geom", "mbrRight", MBR, MBR, BIT, SCALE_FIX);
	}

	end = t;
	*t = NULL;

//	sql_create_func(sa, "st_pointfromtext", "geom", "st_pointformtext", OID, NULL, OID, SCALE_FIX);

	sql_create_aggr(sa, "not_unique", "sql", "not_unique", OID, BIT);
	/* well to be precise it does reduce and map */
	sql_create_func(sa, "not_uniques", "sql", "not_uniques", LNG, NULL, OID, SCALE_NONE);
	sql_create_func(sa, "not_uniques", "sql", "not_uniques", OID, NULL, OID, SCALE_NONE);

	/* functions needed for all types */
	sql_create_func(sa, "hash", "mkey", "hash", ANY, NULL, LNG, SCALE_FIX);
	sql_create_func3(sa, "rotate_xor_hash", "calc", "rotate_xor_hash", LNG, INT, ANY, LNG, SCALE_NONE);
	sql_create_func(sa, "=", "calc", "=", ANY, ANY, BIT, SCALE_FIX);
	sql_create_func(sa, "<>", "calc", "!=", ANY, ANY, BIT, SCALE_FIX);
	sql_create_func(sa, "isnull", "calc", "isnil", ANY, NULL, BIT, SCALE_FIX);
	sql_create_func(sa, ">", "calc", ">", ANY, ANY, BIT, SCALE_FIX);
	sql_create_func(sa, ">=", "calc", ">=", ANY, ANY, BIT, SCALE_FIX);
	sql_create_func(sa, "<", "calc", "<", ANY, ANY, BIT, SCALE_FIX);
	sql_create_func(sa, "<=", "calc", "<=", ANY, ANY, BIT, SCALE_FIX);
	sql_create_aggr(sa, "zero_or_one", "sql", "zero_or_one", ANY, ANY);
	sql_create_aggr(sa, "all", "sql", "all", ANY, ANY);
	sql_create_aggr(sa, "exist", "aggr", "exist", ANY, BIT);
	sql_create_aggr(sa, "not_exist", "aggr", "not_exist", ANY, BIT);
	sql_create_func(sa, "sql_exists", "aggr", "exist", ANY, NULL, BIT, SCALE_NONE);
	sql_create_func(sa, "sql_not_exists", "aggr", "not_exist", ANY, NULL, BIT, SCALE_NONE);
	/* needed for relational version */
	sql_create_func(sa, "in", "calc", "in", ANY, ANY, BIT, SCALE_NONE);
	sql_create_func(sa, "identity", "calc", "identity", ANY, NULL, OID, SCALE_NONE);
	sql_create_func(sa, "rowid", "calc", "identity", ANY, NULL, INT, SCALE_NONE);
	/* needed for indices/clusters oid(schema.table,val) returns max(head(schema.table))+1 */
	sql_create_func3(sa, "rowid", "calc", "rowid", ANY, STR, STR, OID, SCALE_NONE);
	sql_create_aggr(sa, "min", "aggr", "min", ANY, ANY);
	sql_create_aggr(sa, "max", "aggr", "max", ANY, ANY);
	sql_create_func(sa, "sql_min", "calc", "min", ANY, ANY, ANY, SCALE_FIX);
	sql_create_func(sa, "sql_max", "calc", "max", ANY, ANY, ANY, SCALE_FIX);
	sql_create_func3(sa, "ifthenelse", "calc", "ifthenelse", BIT, ANY, ANY, ANY, SCALE_FIX);

	/* sum for numerical and decimals */
	sql_create_aggr(sa, "sum", "aggr", "sum", BTE, LargestINT);
	sql_create_aggr(sa, "sum", "aggr", "sum", SHT, LargestINT);
	sql_create_aggr(sa, "sum", "aggr", "sum", INT, LargestINT);
	//sql_create_aggr(sa, "sum", "aggr", "sum", LNG, LargestINT);
#ifdef HAVE_HGE
	if (have_hge)
		sql_create_aggr(sa, "sum", "aggr", "sum", HGE, LargestINT);
#endif
	sql_create_aggr(sa, "sum", "aggr", "sum", LNG, LNG);

	t = decimals; /* BTE */
	sql_create_aggr(sa, "sum", "aggr", "sum", *(t), LargestDEC);
	t++; /* SHT */
	sql_create_aggr(sa, "sum", "aggr", "sum", *(t), LargestDEC);
	t++; /* INT */
	sql_create_aggr(sa, "sum", "aggr", "sum", *(t), LargestDEC);
	t++; /* LNG */
	sql_create_aggr(sa, "sum", "aggr", "sum", *(t), LargestDEC);
#ifdef HAVE_HGE
	if (have_hge) {
		t++; /* HGE */
		sql_create_aggr(sa, "sum", "aggr", "sum", *(t), LargestDEC);
	}
#endif

	/* prod for numerical and decimals */
	sql_create_aggr(sa, "prod", "aggr", "prod", BTE, LargestINT);
	sql_create_aggr(sa, "prod", "aggr", "prod", SHT, LargestINT);
	sql_create_aggr(sa, "prod", "aggr", "prod", INT, LargestINT);
	sql_create_aggr(sa, "prod", "aggr", "prod", LNG, LargestINT);
#ifdef HAVE_HGE
	if (HAVE_HGE)
		sql_create_aggr(sa, "prod", "aggr", "prod", HGE, LargestINT);
#endif
	/*sql_create_aggr(sa, "prod", "aggr", "prod", LNG, LNG);*/

	t = decimals; /* BTE */
	sql_create_aggr(sa, "prod", "aggr", "prod", *(t), LargestDEC);
	t++; /* SHT */
	sql_create_aggr(sa, "prod", "aggr", "prod", *(t), LargestDEC);
	t++; /* INT */
	sql_create_aggr(sa, "prod", "aggr", "prod", *(t), LargestDEC);
	t++; /* LNG */
	sql_create_aggr(sa, "prod", "aggr", "prod", *(t), LargestDEC);
#ifdef HAVE_HGE
	if (have_hge) {
		t++; /* HGE */
		sql_create_aggr(sa, "prod", "aggr", "prod", *(t), LargestDEC);
	}
#endif

	for (t = numerical; t < dates; t++) {
		sql_create_func(sa, "mod", "calc", "%", *t, *t, *t, SCALE_FIX);
	}

	for (t = floats; t < dates; t++) {
		sql_create_aggr(sa, "sum", "aggr", "sum", *t, *t);
		sql_create_aggr(sa, "prod", "aggr", "prod", *t, *t);
	}
	sql_create_aggr(sa, "sum", "aggr", "sum", MONINT, MONINT);
	sql_create_aggr(sa, "sum", "aggr", "sum", SECINT, SECINT);
	/* do DBL first so that it is chosen as cast destination for
	 * unknown types */
	sql_create_aggr(sa, "avg", "aggr", "avg", DBL, DBL);
	sql_create_aggr(sa, "avg", "aggr", "avg", BTE, DBL);
	sql_create_aggr(sa, "avg", "aggr", "avg", SHT, DBL);
	sql_create_aggr(sa, "avg", "aggr", "avg", INT, DBL);
	sql_create_aggr(sa, "avg", "aggr", "avg", LNG, DBL);
#ifdef HAVE_HGE
	if (have_hge)
		sql_create_aggr(sa, "avg", "aggr", "avg", HGE, DBL);
#endif
	sql_create_aggr(sa, "avg", "aggr", "avg", FLT, DBL);

	sql_create_aggr(sa, "count_no_nil", "aggr", "count_no_nil", NULL, LNG);
	sql_create_aggr(sa, "count", "aggr", "count", NULL, LNG);

	/* order based operators */
	sql_create_analytic(sa, "diff", "sql", "diff", ANY, NULL, NULL, BIT, SCALE_NONE);
	sql_create_analytic(sa, "diff", "sql", "diff", BIT, ANY,  NULL, BIT, SCALE_NONE);

	sql_create_analytic(sa, "rank", "sql", "rank", ANY, BIT, BIT, INT, SCALE_NONE);
	sql_create_analytic(sa, "dense_rank", "sql", "dense_rank", ANY, BIT, BIT, INT, SCALE_NONE);
	sql_create_analytic(sa, "row_number", "sql", "row_number", ANY, BIT, BIT, INT, SCALE_NONE);

	//sql_create_analytic(sa, "percent_rank", "sql", "precent_rank", ANY, BIT, BIT, INT, SCALE_NONE);
	//sql_create_analytic(sa, "cume_dist", "sql", "cume_dist", ANY, BIT, BIT, ANY, SCALE_NONE);
	//sql_create_analytic(sa, "lag", "sql", "lag", ANY, BIT, BIT, ANY, SCALE_NONE);
	//sql_create_analytic(sa, "lead", "sql", "lead", ANY, BIT, BIT, ANY, SCALE_NONE);
	//sql_create_analytic(sa, "first_value", "sql", "first_value", ANY, BIT, BIT, ANY, SCALE_NONE);
	//sql_create_analytic(sa, "last_value", "sql", "last_value", ANY, BIT, BIT, ANY, SCALE_NONE);
	//sql_create_analytic(sa, "sum", "sql", "sum", ANY, BIT, BIT, ANY, SCALE_NONE);
	//sql_create_analytic(sa, "min", "sql", "min", ANY, BIT, BIT, ANY, SCALE_NONE);
	//sql_create_analytic(sa, "max", "sql", "max", ANY, BIT, BIT, ANY, SCALE_NONE);
	//sql_create_analytic(sa, "avg", "sql", "avg", ANY, BIT, BIT, ANY, SCALE_NONE);
	//sql_create_analytic(sa, "count", "sql", "count", ANY, BIT, BIT, ANY, SCALE_NONE);

	sql_create_func(sa, "and", "calc", "and", BIT, BIT, BIT, SCALE_FIX);
	sql_create_func(sa, "or",  "calc",  "or", BIT, BIT, BIT, SCALE_FIX);
	sql_create_func(sa, "xor", "calc", "xor", BIT, BIT, BIT, SCALE_FIX);
	sql_create_func(sa, "not", "calc", "not", BIT, NULL,BIT, SCALE_FIX);

	/* allow smaller types for arguments of mul/div */
	for (t = numerical, t++; t != decimals; t++) {
		sql_type **u;
		for (u = numerical, u++; u != decimals; u++) {
			if (t != u && (*t)->localtype >  (*u)->localtype) {
				sql_create_func(sa, "sql_mul", "calc", "*", *t, *u, *t, SCALE_MUL);
				sql_create_func(sa, "sql_mul", "calc", "*", *u, *t, *t, SCALE_MUL);
				sql_create_func(sa, "sql_div", "calc", "/", *t, *u, *t, SCALE_DIV);
			}
		}
	}
	for (t = decimals, t++; t != floats; t++) {
		sql_type **u;

		for (u = decimals, u++; u != floats; u++) {
			if (t != u && (*t)->localtype >  (*u)->localtype) {
				sql_create_func(sa, "sql_mul", "calc", "*", *t, *u, *t, SCALE_MUL);
				sql_create_func(sa, "sql_div", "calc", "/", *t, *u, *t, SCALE_DIV);
			}
		}
	}

	/* all numericals */
	for (t = numerical; *t != TME; t++) {
		sql_subtype *lt;

		lt = sql_bind_localtype((*t)->base.name);

		sql_create_func(sa, "sql_sub", "calc", "-", *t, *t, *t, SCALE_FIX);
		sql_create_func(sa, "sql_add", "calc", "+", *t, *t, *t, SCALE_FIX);
		sql_create_func(sa, "sql_mul", "calc", "*", *t, *t, *t, SCALE_MUL);
		sql_create_func(sa, "sql_div", "calc", "/", *t, *t, *t, SCALE_DIV);
		if (t < floats) {
			sql_create_func(sa, "bit_and", "calc", "and", *t, *t, *t, SCALE_FIX);
			sql_create_func(sa, "bit_or", "calc", "or", *t, *t, *t, SCALE_FIX);
			sql_create_func(sa, "bit_xor", "calc", "xor", *t, *t, *t, SCALE_FIX);
			sql_create_func(sa, "bit_not", "calc", "not", *t, NULL, *t, SCALE_FIX);
			sql_create_func(sa, "left_shift", "calc", "<<", *t, INT, *t, SCALE_FIX);
			sql_create_func(sa, "right_shift", "calc", ">>", *t, INT, *t, SCALE_FIX);
		}
		sql_create_func(sa, "sql_neg", "calc", "-", *t, NULL, *t, INOUT);
		sql_create_func(sa, "abs", "calc", "abs", *t, NULL, *t, SCALE_FIX);
		sql_create_func(sa, "sign", "calc", "sign", *t, NULL, BTE, SCALE_NONE);
		/* scale fixing for all numbers */
		sql_create_func(sa, "scale_up", "calc", "*", *t, lt->type, *t, SCALE_NONE);
		sql_create_func(sa, "scale_down", "sql", "dec_round", *t, lt->type, *t, SCALE_NONE);
		/* numeric function on INTERVALS */
		if (*t != MONINT && *t != SECINT){
			sql_create_func(sa, "sql_sub", "calc", "-", MONINT, *t, MONINT, SCALE_FIX);
			sql_create_func(sa, "sql_add", "calc", "+", MONINT, *t, MONINT, SCALE_FIX);
			sql_create_func(sa, "sql_mul", "calc", "*", MONINT, *t, MONINT, SCALE_MUL);
			sql_create_func(sa, "sql_div", "calc", "/", MONINT, *t, MONINT, SCALE_DIV);
			sql_create_func(sa, "sql_sub", "calc", "-", SECINT, *t, SECINT, SCALE_FIX);
			sql_create_func(sa, "sql_add", "calc", "+", SECINT, *t, SECINT, SCALE_FIX);
			sql_create_func(sa, "sql_mul", "calc", "*", SECINT, *t, SECINT, SCALE_MUL);
			sql_create_func(sa, "sql_div", "calc", "/", SECINT, *t, SECINT, SCALE_DIV);
		}
	}
	for (t = decimals, t++; t != floats; t++) {
		sql_type **u;
		for (u = numerical; u != floats; u++) {
			if (*u == OID)
				continue;
			if ((*t)->localtype >  (*u)->localtype) {
				sql_create_func(sa, "sql_mul", "calc", "*", *t, *u, *t, SCALE_MUL);
				sql_create_func(sa, "sql_mul", "calc", "*", *u, *t, *t, SCALE_MUL);
			}
		}
	}

	for (t = decimals; t < dates; t++) 
		sql_create_func(sa, "round", "sql", "round", *t, BTE, *t, INOUT);

	for (t = numerical; t < end; t++) {
		sql_type **u;

		for (u = numerical; u < end; u++) {
			sql_create_func(sa, "scale_up", "calc", "*", *u, *t, *t, SCALE_NONE);
		}
	}

	for (t = floats; t < dates; t++) {
		sql_create_func(sa, "power", "mmath", "pow", *t, *t, *t, SCALE_FIX);
		sql_create_func(sa, "floor", "mmath", "floor", *t, NULL, *t, SCALE_FIX);
		sql_create_func(sa, "ceil", "mmath", "ceil", *t, NULL, *t, SCALE_FIX);
		sql_create_func(sa, "ceiling", "mmath", "ceil", *t, NULL, *t, SCALE_FIX);	/* JDBC */
		sql_create_func(sa, "sin", "mmath", "sin", *t, NULL, *t, SCALE_FIX);
		sql_create_func(sa, "cos", "mmath", "cos", *t, NULL, *t, SCALE_FIX);
		sql_create_func(sa, "tan", "mmath", "tan", *t, NULL, *t, SCALE_FIX);
		sql_create_func(sa, "asin", "mmath", "asin", *t, NULL, *t, SCALE_FIX);
		sql_create_func(sa, "acos", "mmath", "acos", *t, NULL, *t, SCALE_FIX);
		sql_create_func(sa, "atan", "mmath", "atan", *t, NULL, *t, SCALE_FIX);
		sql_create_func(sa, "atan", "mmath", "atan2", *t, *t, *t, SCALE_FIX);
		sql_create_func(sa, "sinh", "mmath", "sinh", *t, NULL, *t, SCALE_FIX);
		sql_create_func(sa, "cot", "mmath", "cot", *t, NULL, *t, SCALE_FIX);
		sql_create_func(sa, "cosh", "mmath", "cosh", *t, NULL, *t, SCALE_FIX);
		sql_create_func(sa, "tanh", "mmath", "tanh", *t, NULL, *t, SCALE_FIX);
		sql_create_func(sa, "sqrt", "mmath", "sqrt", *t, NULL, *t, SCALE_FIX);
		sql_create_func(sa, "exp", "mmath", "exp", *t, NULL, *t, SCALE_FIX);
		sql_create_func(sa, "log", "mmath", "log", *t, NULL, *t, SCALE_FIX);
		sql_create_func(sa, "log10", "mmath", "log10", *t, NULL, *t, SCALE_FIX);
	}
	sql_create_func(sa, "pi", "mmath", "pi", NULL, NULL, DBL, SCALE_NONE);

	sql_create_funcSE(sa, "rand", "mmath", "rand", NULL, NULL, INT, SCALE_NONE);
	sql_create_funcSE(sa, "rand", "mmath", "sqlrand", INT, NULL, INT, SCALE_NONE);

	/* Date functions */
	sql_create_func(sa, "curdate", "mtime", "current_date", NULL, NULL, DTE, SCALE_NONE);
	sql_create_func(sa, "current_date", "mtime", "current_date", NULL, NULL, DTE, SCALE_NONE);
	sql_create_func(sa, "curtime", "mtime", "current_time", NULL, NULL, TMETZ, SCALE_NONE);
	sql_create_func(sa, "current_time", "mtime", "current_time", NULL, NULL, TMETZ, SCALE_NONE);
	sql_create_func(sa, "current_timestamp", "mtime", "current_timestamp", NULL, NULL, TMESTAMPTZ, SCALE_NONE);
	sql_create_func(sa, "localtime", "sql", "current_time", NULL, NULL, TME, SCALE_NONE);
	sql_create_func(sa, "localtimestamp", "sql", "current_timestamp", NULL, NULL, TMESTAMP, SCALE_NONE);

	sql_create_func(sa, "sql_sub", "mtime", "diff", DTE, DTE, INT, SCALE_FIX);
	sql_create_func(sa, "sql_sub", "mtime", "diff", TMETZ, TMETZ, SECINT, SCALE_NONE);
	sql_create_func(sa, "sql_sub", "mtime", "diff", TME, TME, SECINT, SCALE_FIX);
	sql_create_func(sa, "sql_sub", "mtime", "diff", TMESTAMPTZ, TMESTAMPTZ, SECINT, SCALE_NONE);
	sql_create_func(sa, "sql_sub", "mtime", "diff", TMESTAMP, TMESTAMP, SECINT, SCALE_FIX);

	sql_create_func(sa, "sql_sub", "mtime", "date_sub_msec_interval", DTE, SECINT, DTE, SCALE_NONE);
	sql_create_func(sa, "sql_sub", "mtime", "date_sub_month_interval", DTE, MONINT, DTE, SCALE_NONE);
	sql_create_func(sa, "sql_sub", "mtime", "time_sub_msec_interval", TME, SECINT, TME, SCALE_NONE);
	sql_create_func(sa, "sql_sub", "mtime", "time_sub_msec_interval", TMETZ, SECINT, TMETZ, SCALE_NONE);
	sql_create_func(sa, "sql_sub", "mtime", "timestamp_sub_msec_interval", TMESTAMP, SECINT, TMESTAMP, SCALE_NONE);
	sql_create_func(sa, "sql_sub", "mtime", "timestamp_sub_month_interval", TMESTAMP, MONINT, TMESTAMP, SCALE_NONE);
	sql_create_func(sa, "sql_sub", "mtime", "timestamp_sub_msec_interval", TMESTAMPTZ, SECINT, TMESTAMPTZ, SCALE_NONE);
	sql_create_func(sa, "sql_sub", "mtime", "timestamp_sub_month_interval", TMESTAMPTZ, MONINT, TMESTAMPTZ, SCALE_NONE);

	sql_create_func(sa, "sql_add", "mtime", "date_add_msec_interval", DTE, SECINT, DTE, SCALE_NONE);
	sql_create_func(sa, "sql_add", "mtime", "addmonths", DTE, MONINT, DTE, SCALE_NONE);
	sql_create_func(sa, "sql_add", "mtime", "timestamp_add_msec_interval", TMESTAMP, SECINT, TMESTAMP, SCALE_NONE);
	sql_create_func(sa, "sql_add", "mtime", "timestamp_add_month_interval", TMESTAMP, MONINT, TMESTAMP, SCALE_NONE);
	sql_create_func(sa, "sql_add", "mtime", "timestamp_add_msec_interval", TMESTAMPTZ, SECINT, TMESTAMPTZ, SCALE_NONE);
	sql_create_func(sa, "sql_add", "mtime", "timestamp_add_month_interval", TMESTAMPTZ, MONINT, TMESTAMPTZ, SCALE_NONE);
	sql_create_func(sa, "sql_add", "mtime", "time_add_msec_interval", TME, SECINT, TME, SCALE_NONE);
	sql_create_func(sa, "sql_add", "mtime", "time_add_msec_interval", TMETZ, SECINT, TMETZ, SCALE_NONE);

	sql_create_func(sa, "local_timezone", "mtime", "local_timezone", NULL, NULL, SECINT, SCALE_FIX);

	sql_create_func(sa, "year", "mtime", "year", DTE, NULL, INT, SCALE_FIX);
	sql_create_func(sa, "quarter", "mtime", "quarter", DTE, NULL, INT, SCALE_FIX);
	sql_create_func(sa, "month", "mtime", "month", DTE, NULL, INT, SCALE_FIX);
	sql_create_func(sa, "day", "mtime", "day", DTE, NULL, INT, SCALE_FIX);
	sql_create_func(sa, "hour", "mtime", "hours", TME, NULL, INT, SCALE_FIX);
	sql_create_func(sa, "minute", "mtime", "minutes", TME, NULL, INT, SCALE_FIX);
	sql_create_func_res(sa, "second", "mtime", "sql_seconds", TME, NULL, DEC, SCALE_NONE, 3);
	sql_create_func(sa, "hour", "mtime", "hours", TMETZ, NULL, INT, SCALE_FIX);
	sql_create_func(sa, "minute", "mtime", "minutes", TMETZ, NULL, INT, SCALE_FIX);
	sql_create_func_res(sa, "second", "mtime", "sql_seconds", TMETZ, NULL, DEC, SCALE_NONE, 3);

	sql_create_func(sa, "year", "mtime", "year", TMESTAMP, NULL, INT, SCALE_FIX);
	sql_create_func(sa, "quarter", "mtime", "quarter", TMESTAMP, NULL, INT, SCALE_FIX);
	sql_create_func(sa, "month", "mtime", "month", TMESTAMP, NULL, INT, SCALE_FIX);
	sql_create_func(sa, "day", "mtime", "day", TMESTAMP, NULL, INT, SCALE_FIX);
	sql_create_func(sa, "hour", "mtime", "hours", TMESTAMP, NULL, INT, SCALE_FIX);
	sql_create_func(sa, "minute", "mtime", "minutes", TMESTAMP, NULL, INT, SCALE_FIX);
	sql_create_func_res(sa, "second", "mtime", "sql_seconds", TMESTAMP, NULL, DEC, SCALE_NONE, 3);

	sql_create_func(sa, "year", "mtime", "year", TMESTAMPTZ, NULL, INT, SCALE_FIX);
	sql_create_func(sa, "quarter", "mtime", "quarter", TMESTAMPTZ, NULL, INT, SCALE_FIX);
	sql_create_func(sa, "month", "mtime", "month", TMESTAMPTZ, NULL, INT, SCALE_FIX);
	sql_create_func(sa, "day", "mtime", "day", TMESTAMPTZ, NULL, INT, SCALE_FIX);
	sql_create_func(sa, "hour", "mtime", "hours", TMESTAMPTZ, NULL, INT, SCALE_FIX);
	sql_create_func(sa, "minute", "mtime", "minutes", TMESTAMPTZ, NULL, INT, SCALE_FIX);
	sql_create_func_res(sa, "second", "mtime", "sql_seconds", TMESTAMPTZ, NULL, DEC, SCALE_NONE, 3);

	sql_create_func(sa, "year", "mtime", "year", MONINT, NULL, INT, SCALE_NONE);
	sql_create_func(sa, "month", "mtime", "month", MONINT, NULL, INT, SCALE_NONE);
	sql_create_func(sa, "day", "mtime", "day", SECINT, NULL, LNG, SCALE_NONE);
	sql_create_func(sa, "hour", "mtime", "hours", SECINT, NULL, INT, SCALE_NONE);
	sql_create_func(sa, "minute", "mtime", "minutes", SECINT, NULL, INT, SCALE_NONE);
	sql_create_func(sa, "second", "mtime", "seconds", SECINT, NULL, INT, SCALE_NONE);

	sql_create_func(sa, "dayofyear", "mtime", "dayofyear", DTE, NULL, INT, SCALE_FIX);
	sql_create_func(sa, "weekofyear", "mtime", "weekofyear", DTE, NULL, INT, SCALE_FIX);
	sql_create_func(sa, "dayofweek", "mtime", "dayofweek", DTE, NULL, INT, SCALE_FIX);
	sql_create_func(sa, "dayofmonth", "mtime", "day", DTE, NULL, INT, SCALE_FIX);
	sql_create_func(sa, "week", "mtime", "weekofyear", DTE, NULL, INT, SCALE_FIX);

	sql_create_funcSE(sa, "next_value_for", "sql", "next_value", STR, STR, LNG, SCALE_NONE);
	sql_create_func(sa, "get_value_for", "sql", "get_value", STR, STR, LNG, SCALE_NONE);
	sql_create_func3(sa, "restart", "sql", "restart", STR, STR, LNG, LNG, SCALE_NONE);
	for (t = strings; t < numerical; t++) {
		sql_create_func(sa, "index", "calc", "index", *t, BIT, BTE, SCALE_NONE);
		sql_create_func(sa, "index", "calc", "index", *t, BIT, SHT, SCALE_NONE);
		sql_create_func(sa, "index", "calc", "index", *t, BIT, INT, SCALE_NONE);
		sql_create_func(sa, "strings", "calc", "strings", *t, NULL, *t, SCALE_NONE);

		sql_create_func(sa, "locate", "str", "locate", *t, *t, INT, SCALE_NONE);
		sql_create_func3(sa, "locate", "str", "locate", *t, *t, INT, INT, SCALE_NONE);
		sql_create_func(sa, "charindex", "str", "locate", *t, *t, INT, SCALE_NONE);
		sql_create_func3(sa, "charindex", "str", "locate", *t, *t, INT, INT, SCALE_NONE);
		sql_create_func3(sa, "splitpart", "str", "splitpart", *t, *t, INT, *t, INOUT);
		sql_create_func(sa, "substring", "str", "substring", *t, INT, *t, INOUT);
		sql_create_func3(sa, "substring", "str", "substring", *t, INT, INT, *t, INOUT);
		sql_create_func(sa, "substr", "str", "substring", *t, INT, *t, INOUT);
		sql_create_func3(sa, "substr", "str", "substring", *t, INT, INT, *t, INOUT);
		/*
		sql_create_func(sa, "like", "algebra", "like", *t, *t, BIT, SCALE_NONE);
		sql_create_func3(sa, "like", "algebra", "like", *t, *t, *t, BIT, SCALE_NONE);
		sql_create_func(sa, "ilike", "algebra", "ilike", *t, *t, BIT, SCALE_NONE);
		sql_create_func3(sa, "ilike", "algebra", "ilike", *t, *t, *t, BIT, SCALE_NONE);
		*/
		sql_create_func(sa, "not_like", "algebra", "not_like", *t, *t, BIT, SCALE_NONE);
		sql_create_func3(sa, "not_like", "algebra", "not_like", *t, *t, *t, BIT, SCALE_NONE);
		sql_create_func(sa, "not_ilike", "algebra", "not_ilike", *t, *t, BIT, SCALE_NONE);
		sql_create_func3(sa, "not_ilike", "algebra", "not_ilike", *t, *t, *t, BIT, SCALE_NONE);

		sql_create_func(sa, "patindex", "pcre", "patindex", *t, *t, INT, SCALE_NONE);
		sql_create_func(sa, "truncate", "str", "stringleft", *t, INT, *t, SCALE_NONE);
		sql_create_func(sa, "concat", "calc", "+", *t, *t, *t, DIGITS_ADD);
		sql_create_func(sa, "ascii", "str", "ascii", *t, NULL, INT, SCALE_NONE);
		sql_create_func(sa, "code", "str", "unicode", INT, NULL, *t, SCALE_NONE);
		sql_create_func(sa, "length", "str", "length", *t, NULL, INT, SCALE_NONE);
		sql_create_func(sa, "right", "str", "stringright", *t, INT, *t, SCALE_NONE);
		sql_create_func(sa, "left", "str", "stringleft", *t, INT, *t, SCALE_NONE);
		sql_create_func(sa, "upper", "str", "toUpper", *t, NULL, *t, SCALE_NONE);
		sql_create_func(sa, "ucase", "str", "toUpper", *t, NULL, *t, SCALE_NONE);
		sql_create_func(sa, "lower", "str", "toLower", *t, NULL, *t, SCALE_NONE);
		sql_create_func(sa, "lcase", "str", "toLower", *t, NULL, *t, SCALE_NONE);
		sql_create_func(sa, "trim", "str", "trim", *t, NULL, *t, SCALE_NONE);
		sql_create_func(sa, "trim", "str", "trim", *t, *t, *t, SCALE_NONE);
		sql_create_func(sa, "ltrim", "str", "ltrim", *t, NULL, *t, SCALE_NONE);
		sql_create_func(sa, "ltrim", "str", "ltrim", *t, *t, *t, SCALE_NONE);
		sql_create_func(sa, "rtrim", "str", "rtrim", *t, NULL, *t, SCALE_NONE);
		sql_create_func(sa, "rtrim", "str", "rtrim", *t, *t, *t, SCALE_NONE);

		sql_create_func(sa, "lpad", "str", "lpad", *t, INT, *t, SCALE_NONE);
		sql_create_func3(sa, "lpad", "str", "lpad", *t, INT, *t, *t, SCALE_NONE);
		sql_create_func(sa, "rpad", "str", "rpad", *t, INT, *t, SCALE_NONE);
		sql_create_func3(sa, "rpad", "str", "rpad", *t, INT, *t, *t, SCALE_NONE);

		sql_create_func4(sa, "insert", "str", "insert", *t, INT, INT, *t, *t, SCALE_NONE);
		sql_create_func3(sa, "replace", "str", "replace", *t, *t, *t, *t, SCALE_NONE);
		sql_create_func(sa, "repeat", "str", "repeat", *t, INT, *t, SCALE_NONE);
		sql_create_func(sa, "space", "str", "space", INT, NULL, *t, SCALE_NONE);
		sql_create_func(sa, "char_length", "str", "length", *t, NULL, INT, SCALE_NONE);
		sql_create_func(sa, "character_length", "str", "length", *t, NULL, INT, SCALE_NONE);
		sql_create_func(sa, "octet_length", "str", "nbytes", *t, NULL, INT, SCALE_NONE);

		sql_create_func(sa, "soundex", "txtsim", "soundex", *t, NULL, *t, SCALE_NONE);
		sql_create_func(sa, "difference", "txtsim", "stringdiff", *t, *t, INT, SCALE_NONE);
		sql_create_func(sa, "editdistance", "txtsim", "editdistance", *t, *t, INT, SCALE_FIX);
		sql_create_func(sa, "editdistance2", "txtsim", "editdistance2", *t, *t, INT, SCALE_FIX);

		sql_create_func(sa, "similarity", "txtsim", "similarity", *t, *t, DBL, SCALE_FIX);
		sql_create_func(sa, "qgramnormalize", "txtsim", "qgramnormalize", *t, NULL, *t, SCALE_NONE);

		sql_create_func(sa, "levenshtein", "txtsim", "levenshtein", *t, *t, INT, SCALE_FIX);
		sres = create_arg(sa, NULL, sql_create_subtype(sa, INT, 0, 0), ARG_OUT);
		sql_create_func_(sa, "levenshtein", "txtsim", "levenshtein",
			 list_append(list_append (list_append (list_append(list_append(sa_list(sa), 
				create_arg(sa, NULL, sql_create_subtype(sa, *t, 0, 0), ARG_IN)), 
				create_arg(sa, NULL, sql_create_subtype(sa, *t, 0, 0), ARG_IN)), 
				create_arg(sa, NULL, sql_create_subtype(sa, INT, 0, 0), ARG_IN)), 
				create_arg(sa, NULL, sql_create_subtype(sa, INT, 0, 0), ARG_IN)), 
				create_arg(sa, NULL, sql_create_subtype(sa, INT, 0, 0), ARG_IN)), 
				sres, FALSE, F_FUNC, SCALE_FIX);
	}
	sres = create_arg(sa, NULL, sql_create_subtype(sa, TABLE, 0, 0), ARG_OUT); 
	/* copyfrom fname (arg 11) */
	f=sql_create_func_(sa, "copyfrom", "sql", "copy_from",
	 	list_append( list_append( list_append( list_append( list_append( list_append(list_append (list_append (list_append(list_append(list_append(sa_list(sa),
			create_arg(sa, NULL, sql_create_subtype(sa, STR, 0, 0), ARG_IN)), 
			create_arg(sa, NULL, sql_create_subtype(sa, STR, 0, 0), ARG_IN)), 
			create_arg(sa, NULL, sql_create_subtype(sa, STR, 0, 0), ARG_IN)), 
			create_arg(sa, NULL, sql_create_subtype(sa, STR, 0, 0), ARG_IN)), 
			create_arg(sa, NULL, sql_create_subtype(sa, STR, 0, 0), ARG_IN)), 
			create_arg(sa, NULL, sql_create_subtype(sa, STR, 0, 0), ARG_IN)), 
			create_arg(sa, NULL, sql_create_subtype(sa, LNG, 0, 0), ARG_IN)), 
			create_arg(sa, NULL, sql_create_subtype(sa, LNG, 0, 0), ARG_IN)), 
			create_arg(sa, NULL, sql_create_subtype(sa, INT, 0, 0), ARG_IN)),
			create_arg(sa, NULL, sql_create_subtype(sa, INT, 0, 0), ARG_IN)),
			create_arg(sa, NULL, sql_create_subtype(sa, STR, 0, 0), ARG_IN)), sres, FALSE, F_UNION, SCALE_FIX);
	f->varres = 1;

	/* bincopyfrom */
	f = sql_create_func_(sa, "copyfrom", "sql", "importTable",
	 	list_append(list_append(sa_list(sa), 
			create_arg(sa, NULL, sql_create_subtype(sa, STR, 0, 0), ARG_IN)), 
			create_arg(sa, NULL, sql_create_subtype(sa, STR, 0, 0), ARG_IN)), sres, FALSE, F_UNION, SCALE_FIX);
	f->varres = 1;

	/* sys_update_schemas, sys_update_tables */
	f = sql_create_func_(sa, "sys_update_schemas", "sql", "update_schemas", NULL, NULL, FALSE, F_PROC, SCALE_NONE);
	f = sql_create_func_(sa, "sys_update_tables", "sql", "update_tables", NULL, NULL, FALSE, F_PROC, SCALE_NONE);
}

void
types_init(sql_allocator *sa, int debug)
{
	(void)debug;
	aliases = sa_list(sa);
	types = sa_list(sa);
	localtypes = sa_list(sa);
	aggrs = sa_list(sa);
	funcs = sa_list(sa);
	MT_lock_set(&funcs->ht_lock);
	funcs->ht = hash_new(sa, 1024, (fkeyvalue)&base_key);
	MT_lock_unset(&funcs->ht_lock);
	sqltypeinit( sa );
}

