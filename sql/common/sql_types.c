/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0.  If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 *
 * Copyright 1997 - July 2008 CWI, August 2008 - 2020 MonetDB B.V.
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

list *aliases = NULL;
list *types = NULL;
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

/* EC_ANY */		{ 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1 }, /* NULL */
/* EC_TABLE */		{ 0, 0, 0, 1, 1, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0 },
/* EC_BIT */		{ 0, 0, 1, 1, 1, 0, 2, 2, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0 },
/* EC_CHAR */		{ 2, 2, 2, 1, 1, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2 },
/* EC_STRING */		{ 2, 2, 2, 1, 1, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2 },
/* EC_BLOB */		{ 0, 0, 0, 0, 1, 1, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0 },
/* EC_POS */		{ 0, 0, 2, 1, 1, 0, 1, 1, 1, 1, 1, 1, 0, 0, 0, 0, 0, 0, 0 },
/* EC_NUM */		{ 0, 0, 2, 1, 1, 0, 1, 1, 1, 1, 1, 1, 0, 0, 0, 0, 0, 0, 0 },
/* EC_MONTH*/   	{ 0, 0, 0, 1, 1, 0, 1, 1, 1, 0, 1, 1, 0, 0, 0, 0, 0, 0, 0 },
/* EC_SEC*/     	{ 0, 0, 0, 1, 1, 0, 1, 1, 0, 1, 1, 1, 1, 1, 0, 0, 0, 0, 0 },
/* EC_DEC */		{ 0, 0, 0, 1, 1, 0, 1, 1, 0, 1, 1, 1, 0, 0, 0, 0, 0, 0, 0 },
/* EC_FLT */		{ 0, 0, 0, 1, 1, 0, 1, 1, 0, 0, 1, 1, 0, 0, 0, 0, 0, 0, 0 },
/* EC_TIME */		{ 0, 0, 0, 1, 1, 0, 0, 0, 0, 0, 0, 0, 1, 2, 0, 0, 0, 0, 0 },
/* EC_TIME_TZ */	{ 0, 0, 0, 1, 1, 0, 0, 0, 0, 0, 0, 0, 1, 1, 0, 0, 0, 0, 0 },
/* EC_DATE */		{ 0, 0, 0, 1, 1, 0, 0, 0, 0, 0, 0, 0, 0, 0, 1, 1, 2, 0, 0 },
/* EC_TSTAMP */		{ 0, 0, 0, 1, 1, 0, 0, 0, 0, 0, 0, 0, 1, 1, 1, 1, 2, 0, 0 },
/* EC_TSTAMP_TZ */	{ 0, 0, 0, 1, 1, 0, 0, 0, 0, 0, 0, 0, 1, 1, 1, 1, 1, 0, 0 },
/* EC_GEOM */		{ 0, 0, 0, 1, 1, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 1, 0 },
/* EC_EXTERNAL*/	{ 0, 0, 0, 1, 1, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0 }
};

int sql_type_convert (int from, int to)
{
	return convert_matrix[from][to];
}

bool is_commutative(const char *fnm)
{
	return strcmp("sql_add", fnm) == 0 ||
		strcmp("sql_mul", fnm) == 0;
}

void
base_init(sql_allocator *sa, sql_base * b, sqlid id, int flags, const char *name)
{
	assert(sa);
	*b = (sql_base) {
		.id = id,
		.flags = flags,
		.refcnt = 1,
		.name = (name) ? sa_strdup(sa, name) : NULL,
	};
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

static bool
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
	return nlt == olt;
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
			if (digits == 0 ? t->digits == 0 : t->digits > digits) {
				sql_init_subtype(r, t, digits, 0);
				return r;
			}
			for (m = n->next; m; m = m->next) {
				t = m->data;
				if (!localtypes_cmp(t->localtype, localtype)) {
					break;
				}
				n = m;
				if (digits == 0 ? t->digits == 0 : t->digits > digits) {
					sql_init_subtype(r, t, digits, 0);
					return r;
				}
			}
		}
	}
	return NULL;
}

sql_subtype *
arg_type( sql_arg *a)
{
	return &a->type;
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
	      (EC_INTERVAL(t1->type->eclass) || t1->type->eclass == EC_NUM)) &&
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

char *
subtype2string2(sql_subtype *tpe) //distinguish char(n), decimal(n,m) from other SQL types
{
	char buf[BUFSIZ];

	switch (tpe->type->eclass) {
		case EC_SEC:
			snprintf(buf, BUFSIZ, "INTERVAL SECOND");
			break;
		case EC_MONTH:
			snprintf(buf, BUFSIZ, "INTERVAL MONTH");
			break;
		case EC_CHAR:
		case EC_STRING:
		case EC_DEC:
			return subtype2string(tpe);
		default:
			snprintf(buf, BUFSIZ, "%s", tpe->type->sqlname);
	}
	return _STRDUP(buf);
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

static int
arg_subtype_cmp_null(sql_arg *a, sql_subtype *t)
{
	if (a->type.type->eclass == EC_ANY)
		return 0;
	if (!t)
		return 0;
	return (is_subtypeof(t, &a->type )?0:-1);
}

static sql_subfunc *
_dup_subaggr(sql_allocator *sa, sql_func *a, sql_subtype *member)
{
	node *tn;
	unsigned int scale = 0, digits = 0;
	sql_subfunc *ares = SA_ZNEW(sa, sql_subfunc);

	assert (a->res);

	ares->func = a;
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
		if (r->type->eclass == EC_ANY && member)
			r = member;
		res = sql_create_subtype(sa, r->type, digits, scale);
		list_append(ares->res, res);
	}
	return ares;
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
	} else if (IS_FUNC(f) || IS_UNION(f) || IS_ANALYTIC(f) || IS_AGGR(f)) { /* not needed for PROC */
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

				/* same scale as the input if result has a scale */
				if (member && (r->type->eclass == EC_ANY || r->type->scale != SCALE_NONE) && member->scale > scale)
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
				if (IS_ANALYTIC(f) && mscale)
					scale = mscale;
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
					if (!st || st->type->eclass == EC_ANY) /* if input parameter is ANY, skip validation */
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
			return (f->type == F_AGGR) ? _dup_subaggr(sa, f, NULL) : sql_dup_subfunc(sa, f, NULL, NULL);
		if (nrargs < 0 || list_length(f->ops) == nrargs)
			return (f->type == F_AGGR) ? _dup_subaggr(sa, f, NULL) : sql_dup_subfunc(sa, f, NULL, NULL);
	}
	return NULL;
}

sql_subfunc *
sql_find_func(sql_allocator *sa, sql_schema *s, const char *sqlfname, int nrargs, sql_ftype type, sql_subfunc *prev)
{
	sql_subfunc *fres;
	int key = hash_key(sqlfname);
	sql_hash_e *he;
	int found = 0;
	sql_ftype filt = (type == F_FUNC)?F_FILT:type;

	assert(nrargs >= -1);
	MT_lock_set(&funcs->ht_lock);
	he = funcs->ht->buckets[key&(funcs->ht->size-1)];
	if (prev) {
		for (; he && !found; he = he->chain)
			if (he->value == prev->func)
				found = 1;
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
						if (n->data == prev->func)
							found = 1;
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
sql_find_funcs(sql_allocator *sa, sql_schema *s, const char *sqlfname, int nrargs, sql_ftype type)
{
	sql_subfunc *fres;
	int key = hash_key(sqlfname);
	sql_hash_e *he;
	sql_ftype filt = (type == F_FUNC)?F_FILT:type;
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
sql_bind_member(sql_allocator *sa, sql_schema *s, const char *sqlfname, sql_subtype *tp, sql_ftype type, int nrargs, sql_subfunc *prev)
{
	node *n = funcs->h;
	int found = 1;

	assert(nrargs);
	if (prev) {
		found = 0;
		for(; n && !found; n = n->next)
			if (n->data == prev->func)
				found = 1;
	}
	for (; n; n = n->next) {
		sql_func *f = n->data;

		if (!f->res && !IS_FILT(f))
			continue;
		if (strcmp(f->base.name, sqlfname) == 0 && f->type == type) {
			if (list_length(f->ops) == nrargs && is_subtypeof(tp, &((sql_arg *) f->ops->h->data)->type))
				return (type == F_AGGR) ? _dup_subaggr(sa, f, NULL) : sql_dup_subfunc(sa, f, NULL, tp);
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
		}
		for (; n; n = n->next) {
			sql_func *f = n->data;

			if (!f->res && !IS_FILT(f))
				continue;
			if (strcmp(f->base.name, sqlfname) == 0 && f->type == type) {
				if (list_length(f->ops) == nrargs && is_subtypeof(tp, &((sql_arg *) f->ops->h->data)->type))
					return (type == F_AGGR) ? _dup_subaggr(sa, f, NULL) : sql_dup_subfunc(sa, f, NULL, tp);
			}
		}
	}
	return NULL;
}

sql_subfunc *
sql_bind_func(sql_allocator *sa, sql_schema *s, const char *sqlfname, sql_subtype *tp1, sql_subtype *tp2, sql_ftype type)
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
sql_bind_func3(sql_allocator *sa, sql_schema *s, const char *sqlfname, sql_subtype *tp1, sql_subtype *tp2, sql_subtype *tp3, sql_ftype type)
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
sql_bind_func_(sql_allocator *sa, sql_schema *s, const char *sqlfname, list *ops, sql_ftype type)
{
	node *n = funcs->h;
	sql_ftype filt = (type == F_FUNC)?F_FILT:type;
	sql_subtype *input_type = NULL;

	if (ops && ops->h)
		input_type = ops->h->data;

	for (; n; n = n->next) {
		sql_func *f = n->data;

		if (f->type != type && f->type != filt)
			continue;
		if (strcmp(f->base.name, sqlfname) == 0) {
			if (list_cmp(f->ops, ops, (fcmp) &arg_subtype_cmp) == 0)
				return (type == F_AGGR) ? _dup_subaggr(sa, f, input_type) : sql_dup_subfunc(sa, f, ops, NULL);
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
					return (type == F_AGGR) ? _dup_subaggr(sa, f, input_type) : sql_dup_subfunc(sa, f, ops, NULL);
			}
		}
	}
	return NULL;
}

sql_subfunc *
sql_bind_func_result(sql_allocator *sa, sql_schema *s, const char *sqlfname, sql_ftype type, sql_subtype *res, int nargs, ...)
{
	node *n = funcs->h;
	list *ops = sa_list(sa);
	va_list valist;

	va_start(valist, nargs);
	for (int i = 0; i < nargs; i++) {
		sql_type *tpe = va_arg(valist, sql_type*);
		list_append(ops, tpe);
	}
	va_end(valist);

	for (; n; n = n->next) {
		sql_func *f = n->data;
		sql_arg *firstres = NULL;

		if (!f->res && !IS_FILT(f))
			continue;
		firstres = IS_FILT(f)?BIT:f->res->h->data;
		if (strcmp(f->base.name, sqlfname) == 0 && f->type == type && (is_subtype(&firstres->type, res) || firstres->type.type->eclass == EC_ANY) && list_cmp(f->ops, ops, (fcmp) &arg_subtype_cmp) == 0)
			return (type == F_AGGR) ? _dup_subaggr(sa, f, NULL) : sql_dup_subfunc(sa, f, ops, NULL);
	}
	if (s && s->funcs.set)
		n = s->funcs.set->h;
	for (; n; n = n->next) {
		sql_func *f = n->data;
		sql_arg *firstres = NULL;

		if (!f->res && !IS_FILT(f))
			continue;
		firstres = IS_FILT(f)?BIT:f->res->h->data;
		if (strcmp(f->base.name, sqlfname) == 0 && f->type == type && (is_subtype(&firstres->type, res) || firstres->type.type->eclass == EC_ANY) && list_cmp(f->ops, ops, (fcmp) &arg_subtype_cmp) == 0)
			return (type == F_AGGR) ? _dup_subaggr(sa, f, NULL) : sql_dup_subfunc(sa, f, ops, NULL);
	}
	return NULL;
}

sql_subfunc *
sql_resolve_function_with_undefined_parameters(sql_allocator *sa, sql_schema *s, const char *name, list *ops, sql_ftype type)
{
	node *n = funcs->h;
	sql_ftype filt = (type == F_FUNC)?F_FILT:type;

	for (; n; n = n->next) {
		sql_func *f = n->data;

		if (f->type != type && f->type != filt)
			continue;
		if (strcmp(f->base.name, name) == 0) {
			if (list_cmp(f->ops, ops, (fcmp) &arg_subtype_cmp_null) == 0)
				return (type == F_AGGR) ? _dup_subaggr(sa, f, NULL) : sql_dup_subfunc(sa, f, ops, NULL);
		}
	}
	if (s) {
		node *n;

		if (s->funcs.set) for (n=s->funcs.set->h; n; n = n->next) {
			sql_func *f = n->data;

			if (f->type != type && f->type != filt)
				continue;
			if (strcmp(f->base.name, name) == 0) {
				if (list_cmp(f->ops, ops, (fcmp) &arg_subtype_cmp_null) == 0)
					return (type == F_AGGR) ? _dup_subaggr(sa, f, NULL) : sql_dup_subfunc(sa, f, ops, NULL);
			}
		}
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

static sql_type *
sql_create_type(sql_allocator *sa, const char *sqlname, unsigned int digits, unsigned int scale, unsigned char radix, sql_class eclass, const char *name)
{
	sql_type *t = SA_ZNEW(sa, sql_type);

	base_init(sa, &t->base, store_next_oid(), 0, name);
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

static sql_func *
sql_create_func_(sql_allocator *sa, const char *name, const char *mod, const char *imp, sql_ftype type, bit side_effect,
				 int fix_scale, unsigned int res_scale, sql_type *res, int nargs, va_list valist)
{
	list *ops = sa_list(sa);
	sql_arg *fres = NULL;
	sql_func *t = SA_ZNEW(sa, sql_func);

	for (int i = 0; i < nargs; i++) {
		sql_type *tpe = va_arg(valist, sql_type*);
		list_append(ops, create_arg(sa, NULL, sql_create_subtype(sa, tpe, 0, 0), ARG_IN));
	}
	if (res)
		fres = create_arg(sa, NULL, sql_create_subtype(sa, res, 0, 0), ARG_OUT);
	base_init(sa, &t->base, store_next_oid(), 0, name);
	t->imp = sa_strdup(sa, imp);
	t->mod = sa_strdup(sa, mod);
	t->ops = ops;
	t->type = type;
	if (fres) {
		if (res_scale)
			fres->type.scale = res_scale;
		t->res = list_append(sa_list(sa), fres);
	} else
		t->res = NULL;
	t->nr = list_length(funcs);
	t->sql = 0;
	t->lang = FUNC_LANG_INT;
	t->side_effect = side_effect;
	t->fix_scale = fix_scale;
	t->s = NULL;
	t->system = TRUE;
	list_append(funcs, t);

	/* grouping aggregate doesn't have a backend */
	assert(strlen(imp) == 0 || strlen(mod) == 0 || backend_resolve_function(&(int){0}, t));

	return t;
}

static sql_func *
sql_create_procedure(sql_allocator *sa, const char *name, const char *mod, const char *imp, bit side_effect, int nargs, ...)
{
	sql_func *res;
	va_list valist;

	va_start(valist, nargs);
	res = sql_create_func_(sa, name, mod, imp, F_PROC, side_effect, SCALE_NONE, 0, NULL, nargs, valist);
	va_end(valist);
	return res;
}

static sql_func *
sql_create_func(sql_allocator *sa, const char *name, const char *mod, const char *imp, bit side_effect, int fix_scale,
				unsigned int res_scale, sql_type *fres, int nargs, ...)
{
	sql_func *res;
	va_list valist;

	va_start(valist, nargs);
	res = sql_create_func_(sa, name, mod, imp, F_FUNC, side_effect, fix_scale, res_scale, fres, nargs, valist);
	va_end(valist);
	return res;
}

static sql_func *
sql_create_aggr(sql_allocator *sa, const char *name, const char *mod, const char *imp, sql_type *fres, int nargs, ...)
{
	sql_func *res;
	va_list valist;

	va_start(valist, nargs);
	res = sql_create_func_(sa, name, mod, imp, F_AGGR, FALSE, SCALE_NONE, 0, fres, nargs, valist);
	va_end(valist);
	return res;
}

static sql_func *
sql_create_union(sql_allocator *sa, const char *name, const char *mod, const char *imp, bit side_effect, int fix_scale,
				unsigned int res_scale, sql_type *fres, int nargs, ...)
{
	sql_func *res;
	va_list valist;

	va_start(valist, nargs);
	res = sql_create_func_(sa, name, mod, imp, F_UNION, side_effect, fix_scale, res_scale, fres, nargs, valist);
	va_end(valist);
	return res;
}

static sql_func *
sql_create_analytic(sql_allocator *sa, const char *name, const char *mod, const char *imp, int fix_scale, sql_type *fres, int nargs, ...)
{
	sql_func *res;
	va_list valist;

	va_start(valist, nargs);
	res = sql_create_func_(sa, name, mod, imp, F_ANALYTIC, FALSE, fix_scale, 0, fres, nargs, valist);
	va_end(valist);
	return res;
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
	sql_type *BLOB;
	sql_type *ANY, *TABLE, *PTR;
	sql_type *GEOM, *MBR;
	sql_func *f;
	sql_type *LargestINT, *LargestDEC;

	ANY = sql_create_type(sa, "ANY", 0, 0, 0, EC_ANY, "void");

	t = ts;
	TABLE = *t++ = sql_create_type(sa, "TABLE", 0, 0, 0, EC_TABLE, "bat");
	PTR = *t++ = sql_create_type(sa, "PTR", 0, 0, 0, EC_TABLE, "ptr");

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

	/* float(n) (n indicates precision of at least n digits) */
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
	TMETZ = *t++ = sql_create_type(sa, "TIMETZ", 7, SCALE_FIX, 0, EC_TIME_TZ, "daytime");
	DTE = *t++ = sql_create_type(sa, "DATE", 0, 0, 0, EC_DATE, "date");
	TMESTAMP = *t++ = sql_create_type(sa, "TIMESTAMP", 7, 0, 0, EC_TIMESTAMP, "timestamp");
	TMESTAMPTZ = *t++ = sql_create_type(sa, "TIMESTAMPTZ", 7, SCALE_FIX, 0, EC_TIMESTAMP_TZ, "timestamp");

	BLOB = *t++ = sql_create_type(sa, "BLOB", 0, 0, 0, EC_BLOB, "blob");

	sql_create_func(sa, "length", "blob", "nitems", FALSE, SCALE_NONE, 0, INT, 1, BLOB);
	sql_create_func(sa, "octet_length", "blob", "nitems", FALSE, SCALE_NONE, 0, INT, 1, BLOB);

	if (geomcatalogfix_get() != NULL) {
		// the geom module is loaded
		GEOM = *t++ = sql_create_type(sa, "GEOMETRY", 0, SCALE_NONE, 0, EC_GEOM, "wkb");
		/*POINT =*/ //*t++ = sql_create_type(sa, "POINT", 0, SCALE_FIX, 0, EC_GEOM, "wkb");
		*t++ = sql_create_type(sa, "GEOMETRYA", 0, SCALE_NONE, 0, EC_EXTERNAL, "wkba");

		MBR = *t++ = sql_create_type(sa, "MBR", 0, SCALE_NONE, 0, EC_EXTERNAL, "mbr");

		/* mbr operator functions */
		sql_create_func(sa, "mbr_overlap", "geom", "mbrOverlaps", FALSE, SCALE_FIX, 0, BIT, 2, GEOM, GEOM);
		sql_create_func(sa, "mbr_overlap", "geom", "mbrOverlaps", FALSE, SCALE_FIX, 0, BIT, 2, MBR, MBR);
		sql_create_func(sa, "mbr_above", "geom", "mbrAbove", FALSE, SCALE_FIX, 0, BIT, 2, GEOM, GEOM);
		sql_create_func(sa, "mbr_above", "geom", "mbrAbove", FALSE, SCALE_FIX, 0, BIT, 2, MBR, MBR);
		sql_create_func(sa, "mbr_below", "geom", "mbrBelow", FALSE, SCALE_FIX, 0, BIT, 2, GEOM, GEOM);
		sql_create_func(sa, "mbr_below", "geom", "mbrBelow", FALSE, SCALE_FIX, 0, BIT, 2, MBR, MBR);
		sql_create_func(sa, "mbr_right", "geom", "mbrRight", FALSE, SCALE_FIX, 0, BIT, 2, GEOM, GEOM);
		sql_create_func(sa, "mbr_right", "geom", "mbrRight", FALSE, SCALE_FIX, 0, BIT, 2, MBR, MBR);
		sql_create_func(sa, "mbr_left", "geom", "mbrLeft", FALSE, SCALE_FIX, 0, BIT, 2, GEOM, GEOM);
		sql_create_func(sa, "mbr_left", "geom", "mbrLeft", FALSE, SCALE_FIX, 0, BIT, 2, MBR, MBR);
		sql_create_func(sa, "mbr_overlap_or_above", "geom", "mbrOverlapOrAbove", FALSE, SCALE_FIX, 0, BIT, 2, GEOM, GEOM);
		sql_create_func(sa, "mbr_overlap_or_above", "geom", "mbrOverlapOrAbove", FALSE, SCALE_FIX, 0, BIT, 2, MBR, MBR);
		sql_create_func(sa, "mbr_overlap_or_below", "geom", "mbrOverlapOrBelow", FALSE, SCALE_FIX, 0, BIT, 2, GEOM, GEOM);
		sql_create_func(sa, "mbr_overlap_or_below", "geom", "mbrOverlapOrBelow", FALSE, SCALE_FIX, 0, BIT, 2, MBR, MBR);
		sql_create_func(sa, "mbr_overlap_or_right", "geom", "mbrOverlapOrRight", FALSE, SCALE_FIX, 0, BIT, 2, GEOM, GEOM);
		sql_create_func(sa, "mbr_overlap_or_right", "geom", "mbrOverlapOrRight", FALSE, SCALE_FIX, 0, BIT, 2, MBR, MBR);
		sql_create_func(sa, "mbr_overlap_or_left", "geom", "mbrOverlapOrLeft", FALSE, SCALE_FIX, 0, BIT, 2, GEOM, GEOM);
		sql_create_func(sa, "mbr_overlap_or_left", "geom", "mbrOverlapOrLeft", FALSE, SCALE_FIX, 0, BIT, 2, MBR, MBR);
		sql_create_func(sa, "mbr_contains", "geom", "mbrContains", FALSE, SCALE_FIX, 0, BIT, 2, GEOM, GEOM);
		sql_create_func(sa, "mbr_contains", "geom", "mbrContains", FALSE, SCALE_FIX, 0, BIT, 2, MBR, MBR);
		sql_create_func(sa, "mbr_contained", "geom", "mbrContained", FALSE, SCALE_FIX, 0, BIT, 2, GEOM, GEOM);
		sql_create_func(sa, "mbr_contained", "geom", "mbrContained", FALSE, SCALE_FIX, 0, BIT, 2, MBR, MBR);
		sql_create_func(sa, "mbr_equal", "geom", "mbrEqual", FALSE, SCALE_FIX, 0, BIT, 2, GEOM, GEOM);
		sql_create_func(sa, "mbr_equal", "geom", "mbrEqual", FALSE, SCALE_FIX, 0, BIT, 2, MBR, MBR);
		sql_create_func(sa, "mbr_distance", "geom", "mbrDistance", FALSE, SCALE_FIX, 0, DBL, 2, GEOM, GEOM);
		sql_create_func(sa, "mbr_distance", "geom", "mbrDistance", FALSE, SCALE_FIX, 0, DBL, 2, MBR, MBR);
		sql_create_func(sa, "left_shift", "geom", "mbrLeft", FALSE, SCALE_FIX, 0, BIT, 2, GEOM, GEOM);
		sql_create_func(sa, "left_shift", "geom", "mbrLeft", FALSE, SCALE_FIX, 0, BIT, 2, MBR, MBR);
		sql_create_func(sa, "right_shift", "geom", "mbrRight", FALSE, SCALE_FIX, 0, BIT, 2, GEOM, GEOM);
		sql_create_func(sa, "right_shift", "geom", "mbrRight", FALSE, SCALE_FIX, 0, BIT, 2, MBR, MBR);
	}

	end = t;
	*t = NULL;

//	sql_create_func(sa, "st_pointfromtext", "geom", "st_pointformtext", FALSE, SCALE_NONE, 0, OID, 1, OID);
	/* The grouping aggregate doesn't have a backend implementation. It gets replaced at rel_unnest */
	sql_create_aggr(sa, "grouping", "", "", BTE, 1, ANY);
	sql_create_aggr(sa, "grouping", "", "", SHT, 1, ANY);
	sql_create_aggr(sa, "grouping", "", "", INT, 1, ANY);
	sql_create_aggr(sa, "grouping", "", "", LNG, 1, ANY);
#ifdef HAVE_HGE
	sql_create_aggr(sa, "grouping", "", "", HGE, 1, ANY);
#endif

	sql_create_aggr(sa, "not_unique", "sql", "not_unique", BIT, 1, OID);
	/* well to be precise it does reduce and map */

	/* functions needed for all types */
	sql_create_func(sa, "hash", "mkey", "hash", FALSE, SCALE_FIX, 0, LNG, 1, ANY);
	sql_create_func(sa, "rotate_xor_hash", "calc", "rotate_xor_hash", FALSE, SCALE_NONE, 0, LNG, 3, LNG, INT, ANY);
	sql_create_func(sa, "=", "calc", "=", FALSE, SCALE_FIX, 0, BIT, 2, ANY, ANY);
	sql_create_func(sa, "<>", "calc", "!=", FALSE, SCALE_FIX, 0, BIT, 2, ANY, ANY);
	sql_create_func(sa, "isnull", "calc", "isnil", FALSE, SCALE_FIX, 0, BIT, 1, ANY);
	sql_create_func(sa, ">", "calc", ">", FALSE, SCALE_FIX, 0, BIT, 2, ANY, ANY);
	sql_create_func(sa, ">=", "calc", ">=", FALSE, SCALE_FIX, 0, BIT, 2, ANY, ANY);
	sql_create_func(sa, "<", "calc", "<", FALSE, SCALE_FIX, 0, BIT, 2, ANY, ANY);
	sql_create_func(sa, "<=", "calc", "<=", FALSE, SCALE_FIX, 0, BIT, 2, ANY, ANY);
	sql_create_func(sa, "between", "calc", "between", FALSE, SCALE_FIX, 0, BIT, 8, ANY, ANY, ANY, BIT, BIT, BIT, BIT, BIT);
	sql_create_aggr(sa, "zero_or_one", "sql", "zero_or_one", ANY, 1, ANY);
	sql_create_aggr(sa, "all", "sql", "all", ANY, 1, ANY);
	sql_create_aggr(sa, "null", "sql", "null", BIT, 1, ANY);
	sql_create_func(sa, "any", "sql", "any", FALSE, SCALE_NONE, 0, BIT, 3, BIT, BIT, BIT);
	sql_create_func(sa, "all", "sql", "all", FALSE, SCALE_NONE, 0, BIT, 3, BIT, BIT, BIT);
	sql_create_aggr(sa, "anyequal", "aggr", "anyequal", BIT, 1, ANY); /* needs 3 arguments (l,r,nil)(ugh) */
	sql_create_aggr(sa, "allnotequal", "aggr", "allnotequal", BIT, 1, ANY); /* needs 3 arguments (l,r,nil)(ugh) */
	sql_create_func(sa, "sql_anyequal", "aggr", "anyequal", FALSE, SCALE_NONE, 0, BIT, 2, ANY, ANY);
	sql_create_func(sa, "sql_not_anyequal", "aggr", "not_anyequal", FALSE, SCALE_NONE, 0, BIT, 2, ANY, ANY);
	sql_create_aggr(sa, "exist", "aggr", "exist", BIT, 1, ANY);
	sql_create_aggr(sa, "not_exist", "aggr", "not_exist", BIT, 1, ANY);
	sql_create_func(sa, "sql_exists", "aggr", "exist", FALSE, SCALE_NONE, 0, BIT, 1, ANY);
	sql_create_func(sa, "sql_not_exists", "aggr", "not_exist", FALSE, SCALE_NONE, 0, BIT, 1, ANY);
	/* needed for relational version */
	sql_create_func(sa, "identity", "calc", "identity", FALSE, SCALE_NONE, 0, OID, 1, ANY);
	sql_create_func(sa, "rowid", "calc", "identity", FALSE, SCALE_NONE, 0, INT, 1, ANY);
	/* needed for indices/clusters oid(schema.table,val) returns max(head(schema.table))+1 */
	sql_create_func(sa, "rowid", "calc", "rowid", FALSE, SCALE_NONE, 0, OID, 3, ANY, STR, STR);
	sql_create_aggr(sa, "min", "aggr", "min", ANY, 1, ANY);
	sql_create_aggr(sa, "max", "aggr", "max", ANY, 1, ANY);
	sql_create_func(sa, "sql_min", "calc", "min", FALSE, SCALE_FIX, 0, ANY, 2, ANY, ANY);
	sql_create_func(sa, "sql_max", "calc", "max", FALSE, SCALE_FIX, 0, ANY, 2, ANY, ANY);
	sql_create_func(sa, "least", "calc", "min_no_nil", FALSE, SCALE_FIX, 0, ANY, 2, ANY, ANY);
	sql_create_func(sa, "greatest", "calc", "max_no_nil", FALSE, SCALE_FIX, 0, ANY, 2, ANY, ANY);
	sql_create_func(sa, "ifthenelse", "calc", "ifthenelse", FALSE, SCALE_FIX, 0, ANY, 3, BIT, ANY, ANY);
	/* nullif and coalesce don't have a backend implementation */
	sql_create_func(sa, "nullif", "", "", FALSE, SCALE_FIX, 0, ANY, 2, ANY, ANY);
	sql_create_func(sa, "coalesce", "", "", FALSE, SCALE_FIX, 0, ANY, 2, ANY, ANY);

	/* sum for numerical and decimals */
	sql_create_aggr(sa, "sum", "aggr", "sum", LargestINT, 1, BTE);
	sql_create_aggr(sa, "sum", "aggr", "sum", LargestINT, 1, SHT);
	sql_create_aggr(sa, "sum", "aggr", "sum", LargestINT, 1, INT);
	//sql_create_aggr(sa, "sum", "aggr", "sum", LargestINT, 1, LNG, LargestINT);
#ifdef HAVE_HGE
	if (have_hge)
		sql_create_aggr(sa, "sum", "aggr", "sum", LargestINT, 1, HGE);
#endif
	sql_create_aggr(sa, "sum", "aggr", "sum", LNG, 1, LNG);

	t = decimals; /* BTE */
	sql_create_aggr(sa, "sum", "aggr", "sum", LargestDEC, 1, *(t));
	t++; /* SHT */
	sql_create_aggr(sa, "sum", "aggr", "sum", LargestDEC, 1, *(t));
	t++; /* INT */
	sql_create_aggr(sa, "sum", "aggr", "sum", LargestDEC, 1, *(t));
	t++; /* LNG */
	sql_create_aggr(sa, "sum", "aggr", "sum", LargestDEC, 1, *(t));
#ifdef HAVE_HGE
	if (have_hge) {
		t++; /* HGE */
		sql_create_aggr(sa, "sum", "aggr", "sum", LargestDEC, 1, *(t));
	}
#endif

	/* prod for numerical and decimals */
	sql_create_aggr(sa, "prod", "aggr", "prod", LargestINT, 1, BTE);
	sql_create_aggr(sa, "prod", "aggr", "prod", LargestINT, 1, SHT);
	sql_create_aggr(sa, "prod", "aggr", "prod", LargestINT, 1, INT);
	sql_create_aggr(sa, "prod", "aggr", "prod", LargestINT, 1, LNG);
#ifdef HAVE_HGE
	if (have_hge)
		sql_create_aggr(sa, "prod", "aggr", "prod", LargestINT, 1, HGE);
#endif

#if 0
	/* prod for decimals introduce errors in the output scales */
	t = decimals; /* BTE */
	sql_create_aggr(sa, "prod", "aggr", "prod", LargestDEC, 1, *(t));
	t++; /* SHT */
	sql_create_aggr(sa, "prod", "aggr", "prod", LargestDEC, 1, *(t));
	t++; /* INT */
	sql_create_aggr(sa, "prod", "aggr", "prod", LargestDEC, 1, *(t));
	t++; /* LNG */
	sql_create_aggr(sa, "prod", "aggr", "prod", LargestDEC, 1, *(t));
#ifdef HAVE_HGE
	if (have_hge) {
		t++; /* HGE */
		sql_create_aggr(sa, "prod", "aggr", "prod", LargestDEC, 1, *(t));
	}
#endif
#endif

	for (t = numerical; t < dates; t++) {
		sql_create_func(sa, "mod", "calc", "%", FALSE, SCALE_FIX, 0, *t, 2, *t, *t);
	}

	for (t = floats; t < dates; t++) {
		sql_create_aggr(sa, "sum", "aggr", "sum", *t, 1, *t);
		sql_create_aggr(sa, "prod", "aggr", "prod", *t, 1, *t);
	}
	sql_create_aggr(sa, "sum", "aggr", "sum", MONINT, 1, MONINT);
	sql_create_aggr(sa, "sum", "aggr", "sum", SECINT, 1, SECINT);
	/* do DBL first so that it is chosen as cast destination for
	 * unknown types */
	sql_create_aggr(sa, "avg", "aggr", "avg", DBL, 1, DBL);
	sql_create_aggr(sa, "avg", "aggr", "avg", DBL, 1, BTE);
	sql_create_aggr(sa, "avg", "aggr", "avg", DBL, 1, SHT);
	sql_create_aggr(sa, "avg", "aggr", "avg", DBL, 1, INT);
	sql_create_aggr(sa, "avg", "aggr", "avg", DBL, 1, LNG);
#ifdef HAVE_HGE
	if (have_hge)
		sql_create_aggr(sa, "avg", "aggr", "avg", DBL, 1, HGE);
#endif
	sql_create_aggr(sa, "avg", "aggr", "avg", DBL, 1, FLT);

	sql_create_aggr(sa, "avg", "aggr", "avg", DBL, 1, MONINT);
	//sql_create_aggr(sa, "avg", "aggr", "avg", DBL, 1, SECINTL);

	sql_create_aggr(sa, "count_no_nil", "aggr", "count_no_nil", LNG, 0);
	sql_create_aggr(sa, "count", "aggr", "count", LNG, 1, ANY);

	sql_create_aggr(sa, "listagg", "aggr", "str_group_concat", STR, 1, STR);
	sql_create_aggr(sa, "listagg", "aggr", "str_group_concat", STR, 2, STR, STR);

	/* order based operators */
	sql_create_analytic(sa, "diff", "sql", "diff", SCALE_NONE, BIT, 1, ANY);
	sql_create_analytic(sa, "diff", "sql", "diff", SCALE_NONE, BIT, 2, BIT, ANY);

	sql_create_analytic(sa, "window_bound", "sql", "window_bound", SCALE_NONE, LNG, 5, ANY, INT, INT, INT, BTE);
	sql_create_analytic(sa, "window_bound", "sql", "window_bound", SCALE_NONE, LNG, 6, BIT, ANY, INT, INT, INT, BTE);
	sql_create_analytic(sa, "window_bound", "sql", "window_bound", SCALE_NONE, LNG, 5, ANY, INT, INT, INT, SHT);
	sql_create_analytic(sa, "window_bound", "sql", "window_bound", SCALE_NONE, LNG, 6, BIT, ANY, INT, INT, INT, SHT);
	sql_create_analytic(sa, "window_bound", "sql", "window_bound", SCALE_NONE, LNG, 5, ANY, INT, INT, INT, INT);
	sql_create_analytic(sa, "window_bound", "sql", "window_bound", SCALE_NONE, LNG, 6, BIT, ANY, INT, INT, INT, INT);
	sql_create_analytic(sa, "window_bound", "sql", "window_bound", SCALE_NONE, LNG, 5, ANY, INT, INT, INT, LNG);
	sql_create_analytic(sa, "window_bound", "sql", "window_bound", SCALE_NONE, LNG, 6, BIT, ANY, INT, INT, INT, LNG);
	sql_create_analytic(sa, "window_bound", "sql", "window_bound", SCALE_NONE, LNG, 5, ANY, INT, INT, INT, FLT);
	sql_create_analytic(sa, "window_bound", "sql", "window_bound", SCALE_NONE, LNG, 6, BIT, ANY, INT, INT, INT, FLT);
	sql_create_analytic(sa, "window_bound", "sql", "window_bound", SCALE_NONE, LNG, 5, ANY, INT, INT, INT, DBL);
	sql_create_analytic(sa, "window_bound", "sql", "window_bound", SCALE_NONE, LNG, 6, BIT, ANY, INT, INT, INT, DBL);
#ifdef HAVE_HGE
	if (have_hge) {
		sql_create_analytic(sa, "window_bound", "sql", "window_bound", SCALE_NONE, LNG, 5, ANY, INT, INT, INT, HGE);
		sql_create_analytic(sa, "window_bound", "sql", "window_bound", SCALE_NONE, LNG, 6, BIT, ANY, INT, INT, INT, HGE);
	}
#endif

	t = decimals; /* BTE */
	sql_create_analytic(sa, "window_bound", "sql", "window_bound", SCALE_NONE, LNG, 5, ANY, INT, INT, INT, *(t));
	sql_create_analytic(sa, "window_bound", "sql", "window_bound", SCALE_NONE, LNG, 6, BIT, ANY, INT, INT, INT, *(t));
	t++; /* SHT */
	sql_create_analytic(sa, "window_bound", "sql", "window_bound", SCALE_NONE, LNG, 5, ANY, INT, INT, INT, *(t));
	sql_create_analytic(sa, "window_bound", "sql", "window_bound", SCALE_NONE, LNG, 6, BIT, ANY, INT, INT, INT, *(t));
	t++; /* INT */
	sql_create_analytic(sa, "window_bound", "sql", "window_bound", SCALE_NONE, LNG, 5, ANY, INT, INT, INT, *(t));
	sql_create_analytic(sa, "window_bound", "sql", "window_bound", SCALE_NONE, LNG, 6, BIT, ANY, INT, INT, INT, *(t));
	t++; /* LNG */
	sql_create_analytic(sa, "window_bound", "sql", "window_bound", SCALE_NONE, LNG, 5, ANY, INT, INT, INT, *(t));
	sql_create_analytic(sa, "window_bound", "sql", "window_bound", SCALE_NONE, LNG, 6, BIT, ANY, INT, INT, INT, *(t));
#ifdef HAVE_HGE
	if (have_hge) {
		t++; /* HGE */
		sql_create_analytic(sa, "window_bound", "sql", "window_bound", SCALE_NONE, LNG, 5, ANY, INT, INT, INT, *(t));
		sql_create_analytic(sa, "window_bound", "sql", "window_bound", SCALE_NONE, LNG, 6, BIT, ANY, INT, INT, INT, *(t));
	}
#endif

	sql_create_analytic(sa, "window_bound", "sql", "window_bound", SCALE_NONE, LNG, 5, ANY, INT, INT, INT, MONINT);
	sql_create_analytic(sa, "window_bound", "sql", "window_bound", SCALE_NONE, LNG, 6, BIT, ANY, INT, INT, INT, MONINT);
	sql_create_analytic(sa, "window_bound", "sql", "window_bound", SCALE_NONE, LNG, 5, ANY, INT, INT, INT, SECINT);
	sql_create_analytic(sa, "window_bound", "sql", "window_bound", SCALE_NONE, LNG, 6, BIT, ANY, INT, INT, INT, SECINT);

	sql_create_analytic(sa, "rank", "sql", "rank", SCALE_NONE, INT, 3, ANY, BIT, BIT);
	sql_create_analytic(sa, "dense_rank", "sql", "dense_rank", SCALE_NONE, INT, 3, ANY, BIT, BIT);
	sql_create_analytic(sa, "row_number", "sql", "row_number", SCALE_NONE, INT, 3, ANY, BIT, BIT);
	sql_create_analytic(sa, "percent_rank", "sql", "percent_rank", SCALE_NONE, DBL, 3, ANY, BIT, BIT);
	sql_create_analytic(sa, "cume_dist", "sql", "cume_dist", SCALE_NONE, DBL, 3, ANY, BIT, BIT);

	sql_create_analytic(sa, "ntile", "sql", "ntile", SCALE_NONE, BTE, 4, ANY, BTE, BIT, BIT);
	sql_create_analytic(sa, "ntile", "sql", "ntile", SCALE_NONE, SHT, 4, ANY, SHT, BIT, BIT);
	sql_create_analytic(sa, "ntile", "sql", "ntile", SCALE_NONE, INT, 4, ANY, INT, BIT, BIT);
	sql_create_analytic(sa, "ntile", "sql", "ntile", SCALE_NONE, LNG, 4, ANY, LNG, BIT, BIT);
#ifdef HAVE_HGE
	if (have_hge)
		sql_create_analytic(sa, "ntile", "sql", "ntile", SCALE_NONE, HGE, 4, ANY, HGE, BIT, BIT);
#endif

	sql_create_analytic(sa, "lag", "sql", "lag", SCALE_NONE, ANY, 3, ANY, BIT, BIT);
	sql_create_analytic(sa, "lag", "sql", "lag", SCALE_NONE, ANY, 4, ANY, BTE, BIT, BIT);
	sql_create_analytic(sa, "lag", "sql", "lag", SCALE_NONE, ANY, 4, ANY, SHT, BIT, BIT);
	sql_create_analytic(sa, "lag", "sql", "lag", SCALE_NONE, ANY, 4, ANY, INT, BIT, BIT);
	sql_create_analytic(sa, "lag", "sql", "lag", SCALE_NONE, ANY, 4, ANY, LNG, BIT, BIT);
#ifdef HAVE_HGE
	if (have_hge)
		sql_create_analytic(sa, "lag", "sql", "lag", SCALE_NONE, ANY, 4, ANY, HGE, BIT, BIT);
#endif
	sql_create_analytic(sa, "lag", "sql", "lag", SCALE_NONE, ANY, 5, ANY, BTE, ANY, BIT, BIT);
	sql_create_analytic(sa, "lag", "sql", "lag", SCALE_NONE, ANY, 5, ANY, SHT, ANY, BIT, BIT);
	sql_create_analytic(sa, "lag", "sql", "lag", SCALE_NONE, ANY, 5, ANY, INT, ANY, BIT, BIT);
	sql_create_analytic(sa, "lag", "sql", "lag", SCALE_NONE, ANY, 5, ANY, LNG, ANY, BIT, BIT);
#ifdef HAVE_HGE
	if (have_hge)
		sql_create_analytic(sa, "lag", "sql", "lag", SCALE_NONE, ANY, 5, ANY, HGE, ANY, BIT, BIT);
#endif

	sql_create_analytic(sa, "lead", "sql", "lead", SCALE_NONE, ANY, 3, ANY, BIT, BIT);
	sql_create_analytic(sa, "lead", "sql", "lead", SCALE_NONE, ANY, 4, ANY, BTE, BIT, BIT);
	sql_create_analytic(sa, "lead", "sql", "lead", SCALE_NONE, ANY, 4, ANY, SHT, BIT, BIT);
	sql_create_analytic(sa, "lead", "sql", "lead", SCALE_NONE, ANY, 4, ANY, INT, BIT, BIT);
	sql_create_analytic(sa, "lead", "sql", "lead", SCALE_NONE, ANY, 4, ANY, LNG, BIT, BIT);
#ifdef HAVE_HGE
	if (have_hge)
		sql_create_analytic(sa, "lead", "sql", "lead", SCALE_NONE, ANY, 4, ANY, HGE, BIT, BIT);
#endif
	sql_create_analytic(sa, "lead", "sql", "lead", SCALE_NONE, ANY, 5, ANY, BTE, ANY, BIT, BIT);
	sql_create_analytic(sa, "lead", "sql", "lead", SCALE_NONE, ANY, 5, ANY, SHT, ANY, BIT, BIT);
	sql_create_analytic(sa, "lead", "sql", "lead", SCALE_NONE, ANY, 5, ANY, INT, ANY, BIT, BIT);
	sql_create_analytic(sa, "lead", "sql", "lead", SCALE_NONE, ANY, 5, ANY, LNG, ANY, BIT, BIT);
#ifdef HAVE_HGE
	if (have_hge)
		sql_create_analytic(sa, "lead", "sql", "lead", SCALE_NONE, ANY, 5, ANY, HGE, ANY, BIT, BIT);
#endif

	//these analytic functions support frames
	sql_create_analytic(sa, "first_value", "sql", "first_value", SCALE_NONE, ANY, 1, ANY);
	sql_create_analytic(sa, "last_value", "sql", "last_value", SCALE_NONE, ANY, 1, ANY);

	sql_create_analytic(sa, "nth_value", "sql", "nth_value", SCALE_NONE, ANY, 2, ANY, BTE);
	sql_create_analytic(sa, "nth_value", "sql", "nth_value", SCALE_NONE, ANY, 2, ANY, SHT);
	sql_create_analytic(sa, "nth_value", "sql", "nth_value", SCALE_NONE, ANY, 2, ANY, INT);
	sql_create_analytic(sa, "nth_value", "sql", "nth_value", SCALE_NONE, ANY, 2, ANY, LNG);
#ifdef HAVE_HGE
	if (have_hge)
		sql_create_analytic(sa, "nth_value", "sql", "nth_value", SCALE_NONE, ANY, 2, ANY, HGE);
#endif

	sql_create_analytic(sa, "count", "sql", "count", SCALE_NONE, LNG, 2, ANY, BIT);
	sql_create_analytic(sa, "min", "sql", "min", SCALE_NONE, ANY, 1, ANY);
	sql_create_analytic(sa, "max", "sql", "max", SCALE_NONE, ANY, 1, ANY);

	//analytical sum for numerical and decimals
	sql_create_analytic(sa, "sum", "sql", "sum", SCALE_NONE, LargestINT, 1, BTE);
	sql_create_analytic(sa, "sum", "sql", "sum", SCALE_NONE, LargestINT, 1, SHT);
	sql_create_analytic(sa, "sum", "sql", "sum", SCALE_NONE, LargestINT, 1, INT);
	sql_create_analytic(sa, "sum", "sql", "sum", SCALE_NONE, LargestINT, 1, LNG);
#ifdef HAVE_HGE
	if (have_hge)
		sql_create_analytic(sa, "sum", "sql", "sum", SCALE_NONE, LargestINT, 1, HGE);
#endif

	t = decimals; // BTE
	sql_create_analytic(sa, "sum", "sql", "sum", SCALE_NONE, LargestDEC, 1, *(t));
	t++; // SHT
	sql_create_analytic(sa, "sum", "sql", "sum", SCALE_NONE, LargestDEC, 1, *(t));
	t++; // INT
	sql_create_analytic(sa, "sum", "sql", "sum", SCALE_NONE, LargestDEC, 1, *(t));
	t++; // LNG
	sql_create_analytic(sa, "sum", "sql", "sum", SCALE_NONE, LargestDEC, 1, *(t));
#ifdef HAVE_HGE
	if (have_hge) {
		t++; // HGE
		sql_create_analytic(sa, "sum", "sql", "sum", SCALE_NONE, LargestDEC, 1, *(t));
	}
#endif

	//analytical product for numerical and decimals
	sql_create_analytic(sa, "prod", "sql", "prod", SCALE_NONE, LargestINT, 1, BTE);
	sql_create_analytic(sa, "prod", "sql", "prod", SCALE_NONE, LargestINT, 1, SHT);
	sql_create_analytic(sa, "prod", "sql", "prod", SCALE_NONE, LargestINT, 1, INT);
	sql_create_analytic(sa, "prod", "sql", "prod", SCALE_NONE, LargestINT, 1, LNG);
#ifdef HAVE_HGE
	if (have_hge)
		sql_create_analytic(sa, "prod", "sql", "prod", SCALE_NONE, LargestINT, 1, HGE);
#endif

#if 0
	/* prod for decimals introduce errors in the output scales */
	t = decimals; // BTE
	sql_create_analytic(sa, "prod", "sql", "prod", SCALE_NONE, LargestDEC, 1, *(t));
	t++; // SHT
	sql_create_analytic(sa, "prod", "sql", "prod", SCALE_NONE, LargestDEC, 1, *(t));
	t++; // INT
	sql_create_analytic(sa, "prod", "sql", "prod", SCALE_NONE, LargestDEC, 1, *(t));
	t++; // LNG
	sql_create_analytic(sa, "prod", "sql", "prod", SCALE_NONE, LargestDEC, 1, *(t));
#ifdef HAVE_HGE
	if (have_hge) {
		t++; // HGE
		sql_create_analytic(sa, "prod", "sql", "prod", SCALE_NONE, LargestDEC, 1, *(t));
	}
#endif
#endif

	for (t = floats; t < dates; t++) {
		sql_create_analytic(sa, "sum", "sql", "sum", SCALE_NONE, *t, 1, *t);
		sql_create_analytic(sa, "prod", "sql", "prod", SCALE_NONE, *t, 1, *t);
	}
	sql_create_analytic(sa, "sum", "sql", "sum", SCALE_NONE, MONINT, 1, MONINT);
	sql_create_analytic(sa, "sum", "sql", "sum", SCALE_NONE, SECINT, 1, SECINT);

	//analytical average for numerical types
	sql_create_analytic(sa, "avg", "sql", "avg", SCALE_NONE, DBL, 1, DBL);
	sql_create_analytic(sa, "avg", "sql", "avg", SCALE_NONE, DBL, 1, BTE);
	sql_create_analytic(sa, "avg", "sql", "avg", SCALE_NONE, DBL, 1, SHT);
	sql_create_analytic(sa, "avg", "sql", "avg", SCALE_NONE, DBL, 1, INT);
	sql_create_analytic(sa, "avg", "sql", "avg", SCALE_NONE, DBL, 1, LNG);
#ifdef HAVE_HGE
	if (have_hge)
		sql_create_analytic(sa, "avg", "sql", "avg", SCALE_NONE, DBL, 1, HGE);
#endif

	sql_create_analytic(sa, "avg", "sql", "avg", SCALE_NONE, DBL, 1, MONINT);
	//sql_create_analytic(sa, "avg", "sql", "avg", SCALE_NONE, DBL, 1, SECINT);

#if 0
	t = decimals; // BTE
	sql_create_analytic(sa, "avg", "sql", "avg", SCALE_NONE, DBL, 1, *(t));
	t++; // SHT
	sql_create_analytic(sa, "avg", "sql", "avg", SCALE_NONE, DBL, 1, *(t));
	t++; // INT
	sql_create_analytic(sa, "avg", "sql", "avg", SCALE_NONE, DBL, 1, *(t));
	t++; // LNG
	sql_create_analytic(sa, "avg", "sql", "avg", SCALE_NONE, DBL, 1, *(t));
#ifdef HAVE_HGE
	if (have_hge) {
		t++; // HGE
		sql_create_analytic(sa, "avg", "sql", "avg", SCALE_NONE, DBL, 1, *(t));
	}
#endif
#endif
	sql_create_analytic(sa, "avg", "sql", "avg", SCALE_NONE, DBL, 1, FLT);

	sql_create_analytic(sa, "listagg", "sql", "str_group_concat", SCALE_NONE, STR, 1, STR);
	sql_create_analytic(sa, "listagg", "sql", "str_group_concat", SCALE_NONE, STR, 2, STR, STR);

	sql_create_func(sa, "and", "calc", "and", FALSE, SCALE_FIX, 0, BIT, 2, BIT, BIT);
	sql_create_func(sa, "or",  "calc",  "or", FALSE, SCALE_FIX, 0, BIT, 2, BIT, BIT);
	sql_create_func(sa, "xor", "calc", "xor", FALSE, SCALE_FIX, 0, BIT, 2, BIT, BIT);
	sql_create_func(sa, "not", "calc", "not", FALSE, SCALE_FIX, 0, BIT, 1, BIT);

	/* allow smaller types for arguments of mul/div */
	for (t = numerical, t++; t != decimals; t++) {
		sql_type **u;
		for (u = numerical, u++; u != decimals; u++) {
			if (t != u && (*t)->localtype >  (*u)->localtype) {
				sql_create_func(sa, "sql_mul", "calc", "*", FALSE, SCALE_MUL, 0, *t, 2, *t, *u);
				sql_create_func(sa, "sql_mul", "calc", "*", FALSE, SCALE_MUL, 0, *t, 2, *u, *t);
				sql_create_func(sa, "sql_div", "calc", "/", FALSE, SCALE_DIV, 0, *t, 2, *t, *u);
			}
		}
	}
	for (t = decimals, t++; t != floats; t++) {
		sql_type **u;

		for (u = decimals, u++; u != floats; u++) {
			if (t != u && (*t)->localtype >  (*u)->localtype) {
				sql_create_func(sa, "sql_mul", "calc", "*", FALSE, SCALE_MUL, 0, *t, 2, *t, *u);
				sql_create_func(sa, "sql_div", "calc", "/", FALSE, SCALE_DIV, 0, *t, 2, *t, *u);
			}
		}
	}

	/* all numericals */
	for (t = numerical; *t != TME; t++) {
		sql_subtype *lt;

		lt = sql_bind_localtype((*t)->base.name);

		sql_create_func(sa, "sql_sub", "calc", "-", FALSE, SCALE_FIX, 0, *t, 2, *t, *t);
		sql_create_func(sa, "sql_add", "calc", "+", FALSE, SCALE_FIX, 0, *t, 2, *t, *t);
		sql_create_func(sa, "sql_mul", "calc", "*", FALSE, SCALE_MUL, 0, *t, 2, *t, *t);
		sql_create_func(sa, "sql_div", "calc", "/", FALSE, SCALE_DIV, 0, *t, 2, *t, *t);
		if (t < floats) {
			sql_create_func(sa, "bit_and", "calc", "and", FALSE, SCALE_FIX, 0, *t, 2, *t, *t);
			sql_create_func(sa, "bit_or", "calc", "or", FALSE, SCALE_FIX, 0, *t, 2, *t, *t);
			sql_create_func(sa, "bit_xor", "calc", "xor", FALSE, SCALE_FIX, 0, *t, 2, *t, *t);
			sql_create_func(sa, "bit_not", "calc", "not", FALSE, SCALE_FIX, 0, *t, 1, *t);
			sql_create_func(sa, "left_shift", "calc", "<<", FALSE, SCALE_FIX, 0, *t, 2, *t, INT);
			sql_create_func(sa, "right_shift", "calc", ">>", FALSE, SCALE_FIX, 0, *t, 2, *t, INT);
		}
		sql_create_func(sa, "sql_neg", "calc", "-", FALSE, INOUT, 0, *t, 1, *t);
		sql_create_func(sa, "abs", "calc", "abs", FALSE, SCALE_FIX, 0, *t, 1, *t);
		sql_create_func(sa, "sign", "calc", "sign", FALSE, SCALE_NONE, 0, BTE, 1, *t);
		/* scale fixing for all numbers */
		sql_create_func(sa, "scale_up", "calc", "*", FALSE, SCALE_NONE, 0, *t, 2, *t, lt->type);
		sql_create_func(sa, "scale_down", "sql", "dec_round", FALSE, SCALE_NONE, 0, *t, 2, *t, lt->type);
		/* numeric function on INTERVALS */
		if (*t != MONINT && *t != SECINT){
			sql_create_func(sa, "sql_sub", "calc", "-", FALSE, SCALE_FIX, 0, MONINT, 2, MONINT, *t);
			sql_create_func(sa, "sql_add", "calc", "+", FALSE, SCALE_FIX, 0, MONINT, 2, MONINT, *t);
			sql_create_func(sa, "sql_mul", "calc", "*", FALSE, SCALE_MUL, 0, MONINT, 2, MONINT, *t);
			sql_create_func(sa, "sql_div", "calc", "/", FALSE, SCALE_DIV, 0, MONINT, 2, MONINT, *t);
			sql_create_func(sa, "sql_sub", "calc", "-", FALSE, SCALE_FIX, 0, SECINT, 2, SECINT, *t);
			sql_create_func(sa, "sql_add", "calc", "+", FALSE, SCALE_FIX, 0, SECINT, 2, SECINT, *t);
			sql_create_func(sa, "sql_mul", "calc", "*", FALSE, SCALE_MUL, 0, SECINT, 2, SECINT, *t);
			sql_create_func(sa, "sql_div", "calc", "/", FALSE, SCALE_DIV, 0, SECINT, 2, SECINT, *t);
		}
	}
	for (t = decimals, t++; t != floats; t++) {
		sql_type **u;
		for (u = numerical; u != floats; u++) {
			if (*u == OID)
				continue;
			if ((*t)->localtype >  (*u)->localtype) {
				sql_create_func(sa, "sql_mul", "calc", "*", FALSE, SCALE_MUL, 0, *t, 2, *t, *u);
				sql_create_func(sa, "sql_mul", "calc", "*", FALSE, SCALE_MUL, 0, *t, 2, *u, *t);
			}
		}
	}

	for (t = decimals; t < dates; t++)
		sql_create_func(sa, "round", "sql", "round", FALSE, INOUT, 0, *t, 2, *t, BTE);

	for (t = numerical; t < end; t++) {
		sql_type **u;

		for (u = numerical; u < end; u++) {
			sql_create_func(sa, "scale_up", "calc", "*", FALSE, SCALE_NONE, 0, *t, 2, *u, *t);
		}
	}

	for (t = floats; t < dates; t++) {
		sql_create_func(sa, "power", "mmath", "pow", FALSE, SCALE_FIX, 0, *t, 2, *t, *t);
		sql_create_func(sa, "floor", "mmath", "floor", FALSE, SCALE_FIX, 0, *t, 1, *t);
		sql_create_func(sa, "ceil", "mmath", "ceil", FALSE, SCALE_FIX, 0, *t, 1, *t);
		sql_create_func(sa, "ceiling", "mmath", "ceil", FALSE, SCALE_FIX, 0, *t, 1, *t);	/* JDBC */
		sql_create_func(sa, "sin", "mmath", "sin", FALSE, SCALE_FIX, 0, *t, 1, *t);
		sql_create_func(sa, "cos", "mmath", "cos", FALSE, SCALE_FIX, 0, *t, 1, *t);
		sql_create_func(sa, "tan", "mmath", "tan", FALSE, SCALE_FIX, 0, *t, 1, *t);
		sql_create_func(sa, "asin", "mmath", "asin", FALSE, SCALE_FIX, 0, *t, 1, *t);
		sql_create_func(sa, "acos", "mmath", "acos", FALSE, SCALE_FIX, 0, *t, 1, *t);
		sql_create_func(sa, "atan", "mmath", "atan", FALSE, SCALE_FIX, 0, *t, 1, *t);
		sql_create_func(sa, "atan", "mmath", "atan2", FALSE, SCALE_FIX, 0, *t, 2, *t, *t);
		sql_create_func(sa, "sinh", "mmath", "sinh", FALSE, SCALE_FIX, 0, *t, 1, *t);
		sql_create_func(sa, "cot", "mmath", "cot", FALSE, SCALE_FIX, 0, *t, 1, *t);
		sql_create_func(sa, "cosh", "mmath", "cosh", FALSE, SCALE_FIX, 0, *t, 1, *t);
		sql_create_func(sa, "tanh", "mmath", "tanh", FALSE, SCALE_FIX, 0, *t, 1, *t);
		sql_create_func(sa, "sqrt", "mmath", "sqrt", FALSE, SCALE_FIX, 0, *t, 1, *t);
		sql_create_func(sa, "exp", "mmath", "exp", FALSE, SCALE_FIX, 0, *t, 1, *t);
		sql_create_func(sa, "log", "mmath", "log", FALSE, SCALE_FIX, 0, *t, 1, *t);
		sql_create_func(sa, "ln", "mmath", "log", FALSE, SCALE_FIX, 0, *t, 1, *t);
		sql_create_func(sa, "log", "mmath", "log", FALSE, SCALE_FIX, 0, *t, 2, *t, *t);
		sql_create_func(sa, "log10", "mmath", "log10", FALSE, SCALE_FIX, 0, *t, 1, *t);
		sql_create_func(sa, "log2", "mmath", "log2", FALSE, SCALE_FIX, 0, *t, 1, *t);
	}
	sql_create_func(sa, "pi", "mmath", "pi", FALSE, SCALE_NONE, 0, DBL, 0);

	sql_create_func(sa, "rand", "mmath", "rand", TRUE, SCALE_NONE, 0, INT, 0);
	sql_create_func(sa, "rand", "mmath", "sqlrand", TRUE, SCALE_NONE, 0, INT, 1, INT);

	/* Date functions */
	sql_create_func(sa, "curdate", "mtime", "current_date", FALSE, SCALE_NONE, 0, DTE, 0);
	sql_create_func(sa, "current_date", "mtime", "current_date", FALSE, SCALE_NONE, 0, DTE, 0);
	sql_create_func(sa, "curtime", "mtime", "current_time", FALSE, SCALE_NONE, 0, TMETZ, 0);
	sql_create_func(sa, "current_time", "mtime", "current_time", FALSE, SCALE_NONE, 0, TMETZ, 0);
	sql_create_func(sa, "current_timestamp", "mtime", "current_timestamp", FALSE, SCALE_NONE, 0, TMESTAMPTZ, 0);
	sql_create_func(sa, "localtime", "sql", "current_time", FALSE, SCALE_NONE, 0, TME, 0);
	sql_create_func(sa, "localtimestamp", "sql", "current_timestamp", FALSE, SCALE_NONE, 0, TMESTAMP, 0);

	sql_create_func(sa, "sql_sub", "mtime", "diff", FALSE, SCALE_FIX, 0, INT, 2, DTE, DTE);
	sql_create_func(sa, "sql_sub", "mtime", "diff", FALSE, SCALE_NONE, 0, SECINT, 2, TMETZ, TMETZ);
	sql_create_func(sa, "sql_sub", "mtime", "diff", FALSE, SCALE_FIX, 0, SECINT, 2, TME, TME);
	sql_create_func(sa, "sql_sub", "mtime", "diff", FALSE, SCALE_NONE, 0, SECINT, 2, TMESTAMPTZ, TMESTAMPTZ);
	sql_create_func(sa, "sql_sub", "mtime", "diff", FALSE, SCALE_FIX, 0, SECINT, 2, TMESTAMP, TMESTAMP);

	sql_create_func(sa, "sql_sub", "mtime", "date_sub_msec_interval", FALSE, SCALE_NONE, 0, DTE, 2, DTE, SECINT);
	sql_create_func(sa, "sql_sub", "mtime", "date_sub_month_interval", FALSE, SCALE_NONE, 0, DTE, 2, DTE, MONINT);
	sql_create_func(sa, "sql_sub", "mtime", "time_sub_msec_interval", FALSE, SCALE_NONE, 0, TME, 2, TME, SECINT);
	sql_create_func(sa, "sql_sub", "mtime", "time_sub_msec_interval", FALSE, SCALE_NONE, 0, TMETZ, 2, TMETZ, SECINT);
	sql_create_func(sa, "sql_sub", "mtime", "timestamp_sub_msec_interval", FALSE, SCALE_NONE, 0, TMESTAMP, 2, TMESTAMP, SECINT);
	sql_create_func(sa, "sql_sub", "mtime", "timestamp_sub_month_interval", FALSE, SCALE_NONE, 0, TMESTAMP, 2, TMESTAMP, MONINT);
	sql_create_func(sa, "sql_sub", "mtime", "timestamp_sub_msec_interval", FALSE, SCALE_NONE, 0, TMESTAMPTZ, 2, TMESTAMPTZ, SECINT);
	sql_create_func(sa, "sql_sub", "mtime", "timestamp_sub_month_interval", FALSE, SCALE_NONE, 0, TMESTAMPTZ, 2, TMESTAMPTZ, MONINT);

	sql_create_func(sa, "sql_add", "mtime", "date_add_msec_interval", FALSE, SCALE_NONE, 0, DTE, 2, DTE, SECINT);
	sql_create_func(sa, "sql_add", "mtime", "addmonths", FALSE, SCALE_NONE, 0, DTE, 2, DTE, MONINT);
	sql_create_func(sa, "sql_add", "mtime", "timestamp_add_msec_interval", FALSE, SCALE_NONE, 0, TMESTAMP, 2, TMESTAMP, SECINT);
	sql_create_func(sa, "sql_add", "mtime", "timestamp_add_month_interval", FALSE, SCALE_NONE, 0, TMESTAMP, 2, TMESTAMP, MONINT);
	sql_create_func(sa, "sql_add", "mtime", "timestamp_add_msec_interval", FALSE, SCALE_NONE, 0, TMESTAMPTZ, 2, TMESTAMPTZ, SECINT);
	sql_create_func(sa, "sql_add", "mtime", "timestamp_add_month_interval", FALSE, SCALE_NONE, 0, TMESTAMPTZ, 2, TMESTAMPTZ, MONINT);
	sql_create_func(sa, "sql_add", "mtime", "time_add_msec_interval", FALSE, SCALE_NONE, 0, TME, 2, TME, SECINT);
	sql_create_func(sa, "sql_add", "mtime", "time_add_msec_interval", FALSE, SCALE_NONE, 0, TMETZ, 2, TMETZ, SECINT);

	sql_create_func(sa, "local_timezone", "mtime", "local_timezone", FALSE, SCALE_FIX, 0, SECINT, 0);

	sql_create_func(sa, "century", "mtime", "century", FALSE, SCALE_FIX, 0, INT, 1, DTE);
	sql_create_func(sa, "decade", "mtime", "decade", FALSE, SCALE_FIX, 0, INT, 1, DTE);
	sql_create_func(sa, "year", "mtime", "year", FALSE, SCALE_FIX, 0, INT, 1, DTE);
	sql_create_func(sa, "quarter", "mtime", "quarter", FALSE, SCALE_FIX, 0, INT, 1, DTE);
	sql_create_func(sa, "month", "mtime", "month", FALSE, SCALE_FIX, 0, INT, 1, DTE);
	sql_create_func(sa, "day", "mtime", "day", FALSE, SCALE_FIX, 0, INT, 1, DTE);
	sql_create_func(sa, "dayofyear", "mtime", "dayofyear", FALSE, SCALE_FIX, 0, INT, 1, DTE);
	sql_create_func(sa, "weekofyear", "mtime", "weekofyear", FALSE, SCALE_FIX, 0, INT, 1, DTE);
	sql_create_func(sa, "dayofweek", "mtime", "dayofweek", FALSE, SCALE_FIX, 0, INT, 1, DTE);
	sql_create_func(sa, "dayofmonth", "mtime", "day", FALSE, SCALE_FIX, 0, INT, 1, DTE);
	sql_create_func(sa, "week", "mtime", "weekofyear", FALSE, SCALE_FIX, 0, INT, 1, DTE);

	sql_create_func(sa, "hour", "mtime", "hours", FALSE, SCALE_FIX, 0, INT, 1, TME);
	sql_create_func(sa, "minute", "mtime", "minutes", FALSE, SCALE_FIX, 0, INT, 1, TME);
	sql_create_func(sa, "second", "mtime", "sql_seconds", FALSE, SCALE_NONE, 6, DEC, 1, TME);
	sql_create_func(sa, "hour", "mtime", "hours", FALSE, SCALE_FIX, 0, INT, 1, TMETZ);
	sql_create_func(sa, "minute", "mtime", "minutes", FALSE, SCALE_FIX, 0, INT, 1, TMETZ);
	sql_create_func(sa, "second", "mtime", "sql_seconds", FALSE, SCALE_NONE, 6, DEC, 1, TMETZ);

	sql_create_func(sa, "century", "mtime", "century", FALSE, SCALE_FIX, 0, INT, 1, TMESTAMP);
	sql_create_func(sa, "decade", "mtime", "decade", FALSE, SCALE_FIX, 0, INT, 1, TMESTAMP);
	sql_create_func(sa, "year", "mtime", "year", FALSE, SCALE_FIX, 0, INT, 1, TMESTAMP);
	sql_create_func(sa, "quarter", "mtime", "quarter", FALSE, SCALE_FIX, 0, INT, 1, TMESTAMP);
	sql_create_func(sa, "month", "mtime", "month", FALSE, SCALE_FIX, 0, INT, 1, TMESTAMP);
	sql_create_func(sa, "day", "mtime", "day", FALSE, SCALE_FIX, 0, INT, 1, TMESTAMP);
	sql_create_func(sa, "hour", "mtime", "hours", FALSE, SCALE_FIX, 0, INT, 1, TMESTAMP);
	sql_create_func(sa, "minute", "mtime", "minutes", FALSE, SCALE_FIX, 0, INT, 1, TMESTAMP);
	sql_create_func(sa, "second", "mtime", "sql_seconds", FALSE, SCALE_NONE, 6, DEC, 1, TMESTAMP);

	sql_create_func(sa, "century", "mtime", "century", FALSE, SCALE_FIX, 0, INT, 1, TMESTAMPTZ);
	sql_create_func(sa, "decade", "mtime", "decade", FALSE, SCALE_FIX, 0, INT, 1, TMESTAMPTZ);
	sql_create_func(sa, "year", "mtime", "year", FALSE, SCALE_FIX, 0, INT, 1, TMESTAMPTZ);
	sql_create_func(sa, "quarter", "mtime", "quarter", FALSE, SCALE_FIX, 0, INT, 1, TMESTAMPTZ);
	sql_create_func(sa, "month", "mtime", "month", FALSE, SCALE_FIX, 0, INT, 1, TMESTAMPTZ);
	sql_create_func(sa, "day", "mtime", "day", FALSE, SCALE_FIX, 0, INT, 1, TMESTAMPTZ);
	sql_create_func(sa, "hour", "mtime", "hours", FALSE, SCALE_FIX, 0, INT, 1, TMESTAMPTZ);
	sql_create_func(sa, "minute", "mtime", "minutes", FALSE, SCALE_FIX, 0, INT, 1, TMESTAMPTZ);
	sql_create_func(sa, "second", "mtime", "sql_seconds", FALSE, SCALE_NONE, 6, DEC, 1, TMESTAMPTZ);

	sql_create_func(sa, "year", "mtime", "year", FALSE, SCALE_NONE, 0, INT, 1, MONINT);
	sql_create_func(sa, "month", "mtime", "month", FALSE, SCALE_NONE, 0, INT, 1, MONINT);
	sql_create_func(sa, "day", "mtime", "day", FALSE, SCALE_NONE, 0, LNG, 1, SECINT);
	sql_create_func(sa, "hour", "mtime", "hours", FALSE, SCALE_NONE, 0, INT, 1, SECINT);
	sql_create_func(sa, "minute", "mtime", "minutes", FALSE, SCALE_NONE, 0, INT, 1, SECINT);
	sql_create_func(sa, "second", "mtime", "seconds", FALSE, SCALE_NONE, 0, INT, 1, SECINT);

	sql_create_func(sa, "next_value_for", "sql", "next_value", TRUE, SCALE_NONE, 0, LNG, 2, STR, STR);
	sql_create_func(sa, "get_value_for", "sql", "get_value", FALSE, SCALE_NONE, 0, LNG, 2, STR, STR);
	sql_create_func(sa, "restart", "sql", "restart", FALSE, SCALE_NONE, 0, LNG, 3, STR, STR, LNG);
	for (t = strings; t < numerical; t++) {
		sql_create_func(sa, "index", "calc", "index", FALSE, SCALE_NONE, 0, BTE, 2, *t, BIT);
		sql_create_func(sa, "index", "calc", "index", FALSE, SCALE_NONE, 0, SHT, 2, *t, BIT);
		sql_create_func(sa, "index", "calc", "index", FALSE, SCALE_NONE, 0, INT, 2, *t, BIT);
		sql_create_func(sa, "strings", "calc", "strings", FALSE, SCALE_NONE, 0, *t, 1, *t);

		sql_create_func(sa, "locate", "str", "locate", FALSE, SCALE_NONE, 0, INT, 2, *t, *t);
		sql_create_func(sa, "locate", "str", "locate", FALSE, SCALE_NONE, 0, INT, 3, *t, *t, INT);
		sql_create_func(sa, "charindex", "str", "locate", FALSE, SCALE_NONE, 0, INT, 2, *t, *t);
		sql_create_func(sa, "charindex", "str", "locate", FALSE, SCALE_NONE, 0, INT, 3, *t, *t, INT);
		sql_create_func(sa, "splitpart", "str", "splitpart", FALSE, INOUT, 0, *t, 3, *t, *t, INT);
		sql_create_func(sa, "substring", "str", "substring", FALSE, INOUT, 0, *t, 2, *t, INT);
		sql_create_func(sa, "substring", "str", "substring", FALSE, INOUT, 0, *t, 3, *t, INT, INT);
		sql_create_func(sa, "substr", "str", "substring", FALSE, INOUT, 0, *t, 2, *t, INT);
		sql_create_func(sa, "substr", "str", "substring", FALSE, INOUT, 0, *t, 3, *t, INT, INT);
		/*
		sql_create_func(sa, "like", "algebra", "like", FALSE, SCALE_NONE, 0, BIT, 2, *t, *t);
		sql_create_func3(sa, "like", "algebra", "like", FALSE, SCALE_NONE, 0, BIT, 3, *t, *t, *t);
		sql_create_func(sa, "ilike", "algebra", "ilike", FALSE, SCALE_NONE, 0, BIT, 2, *t, *t);
		sql_create_func3(sa, "ilike", "algebra", "ilike", FALSE, SCALE_NONE, 0, BIT, 3, *t, *t, *t);
		*/
		sql_create_func(sa, "not_like", "algebra", "not_like", FALSE, SCALE_NONE, 0, BIT, 2, *t, *t);
		sql_create_func(sa, "not_like", "algebra", "not_like", FALSE, SCALE_NONE, 0, BIT, 3, *t, *t, *t);
		sql_create_func(sa, "not_ilike", "algebra", "not_ilike", FALSE, SCALE_NONE, 0, BIT, 2, *t, *t);
		sql_create_func(sa, "not_ilike", "algebra", "not_ilike", FALSE, SCALE_NONE, 0, BIT, 3, *t, *t, *t);

		sql_create_func(sa, "patindex", "pcre", "patindex", FALSE, SCALE_NONE, 0, INT, 2, *t, *t);
		sql_create_func(sa, "truncate", "str", "stringleft", FALSE, SCALE_NONE, 0, *t, 2, *t, INT);
		sql_create_func(sa, "concat", "calc", "+", FALSE, DIGITS_ADD, 0, *t, 2, *t, *t);
		sql_create_func(sa, "ascii", "str", "ascii", FALSE, SCALE_NONE, 0, INT, 1, *t);
		sql_create_func(sa, "code", "str", "unicode", FALSE, SCALE_NONE, 0, *t, 1, INT);
		sql_create_func(sa, "length", "str", "length", FALSE, SCALE_NONE, 0, INT, 1, *t);
		sql_create_func(sa, "right", "str", "stringright", FALSE, SCALE_NONE, 0, *t, 2, *t, INT);
		sql_create_func(sa, "left", "str", "stringleft", FALSE, SCALE_NONE, 0, *t, 2, *t, INT);
		sql_create_func(sa, "upper", "str", "toUpper", FALSE, SCALE_NONE, 0, *t, 1, *t);
		sql_create_func(sa, "ucase", "str", "toUpper", FALSE, SCALE_NONE, 0, *t, 1, *t);
		sql_create_func(sa, "lower", "str", "toLower", FALSE, SCALE_NONE, 0, *t, 1, *t);
		sql_create_func(sa, "lcase", "str", "toLower", FALSE, SCALE_NONE, 0, *t, 1, *t);
		sql_create_func(sa, "trim", "str", "trim", FALSE, SCALE_NONE, 0, *t, 1, *t);
		sql_create_func(sa, "trim", "str", "trim", FALSE, SCALE_NONE, 0, *t, 2, *t, *t);
		sql_create_func(sa, "ltrim", "str", "ltrim", FALSE, SCALE_NONE, 0, *t, 1, *t);
		sql_create_func(sa, "ltrim", "str", "ltrim", FALSE, SCALE_NONE, 0, *t, 2, *t, *t);
		sql_create_func(sa, "rtrim", "str", "rtrim", FALSE, SCALE_NONE, 0, *t, 1, *t);
		sql_create_func(sa, "rtrim", "str", "rtrim", FALSE, SCALE_NONE, 0, *t, 2, *t, *t);

		sql_create_func(sa, "lpad", "str", "lpad", FALSE, SCALE_NONE, 0, *t, 2, *t, INT);
		sql_create_func(sa, "lpad", "str", "lpad", FALSE, SCALE_NONE, 0, *t, 3, *t, INT, *t);
		sql_create_func(sa, "rpad", "str", "rpad", FALSE, SCALE_NONE, 0, *t, 2, *t, INT);
		sql_create_func(sa, "rpad", "str", "rpad", FALSE, SCALE_NONE, 0, *t, 3, *t, INT, *t);

		sql_create_func(sa, "insert", "str", "insert", FALSE, SCALE_NONE, 0, *t, 4, *t, INT, INT, *t);
		sql_create_func(sa, "replace", "str", "replace", FALSE, SCALE_NONE, 0, *t, 3, *t, *t, *t);
		sql_create_func(sa, "repeat", "str", "repeat", FALSE, SCALE_NONE, 0, *t, 2, *t, INT);
		sql_create_func(sa, "space", "str", "space", FALSE, SCALE_NONE, 0, *t, 1, INT);
		sql_create_func(sa, "char_length", "str", "length", FALSE, SCALE_NONE, 0, INT, 1, *t);
		sql_create_func(sa, "character_length", "str", "length", FALSE, SCALE_NONE, 0, INT, 1, *t);
		sql_create_func(sa, "octet_length", "str", "nbytes", FALSE, SCALE_NONE, 0, INT, 1, *t);

		sql_create_func(sa, "soundex", "txtsim", "soundex", FALSE, SCALE_NONE, 0, *t, 1, *t);
		sql_create_func(sa, "difference", "txtsim", "stringdiff", FALSE, SCALE_NONE, 0, INT, 2, *t, *t);
		sql_create_func(sa, "editdistance", "txtsim", "editdistance", FALSE, SCALE_FIX, 0, INT, 2, *t, *t);
		sql_create_func(sa, "editdistance2", "txtsim", "editdistance2", FALSE, SCALE_FIX, 0, INT, 2, *t, *t);

		sql_create_func(sa, "similarity", "txtsim", "similarity", FALSE, SCALE_FIX, 0, DBL, 2, *t, *t);
		sql_create_func(sa, "qgramnormalize", "txtsim", "qgramnormalize", FALSE, SCALE_NONE, 0, *t, 1, *t);

		sql_create_func(sa, "levenshtein", "txtsim", "levenshtein", FALSE, SCALE_FIX, 0, INT, 2, *t, *t);
		sql_create_func(sa, "levenshtein", "txtsim", "levenshtein", FALSE, SCALE_FIX, 0, INT, 5, *t, *t, INT, INT, INT);
	}
	/* copyfrom fname (arg 12) */
	f = sql_create_union(sa, "copyfrom", "sql", "copy_from", FALSE, SCALE_FIX, 0, TABLE, 12, PTR, STR, STR, STR, STR, STR, STR, LNG, LNG, INT, INT, STR, INT);
	f->varres = 1;

	/* bincopyfrom */
	f = sql_create_union(sa, "copyfrom", "sql", "importTable", FALSE, SCALE_FIX, 0, TABLE, 3, STR, STR, INT);
	f->varres = 1;

	/* sys_update_schemas, sys_update_tables */
	sql_create_procedure(sa, "sys_update_schemas", "sql", "update_schemas", FALSE, 0);
	sql_create_procedure(sa, "sys_update_tables", "sql", "update_tables", FALSE, 0);
}

void
types_init(sql_allocator *sa)
{
	aliases = sa_list(sa);
	types = sa_list(sa);
	localtypes = sa_list(sa);
	funcs = sa_list(sa);
	MT_lock_set(&funcs->ht_lock);
	funcs->ht = hash_new(sa, 1024, (fkeyvalue)&base_key);
	MT_lock_unset(&funcs->ht_lock);
	sqltypeinit( sa );
}
