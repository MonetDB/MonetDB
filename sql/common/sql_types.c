/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0.  If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 *
 * Copyright 1997 - July 2008 CWI, August 2008 - 2021 MonetDB B.V.
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
#include "gdk_geomlogger.h"	/* for geomcatalogfix_get */

list *aliases = NULL;
list *types = NULL;
list *funcs = NULL;

static sql_type *BIT = NULL;
static list *localtypes = NULL;

unsigned int digits2bits(unsigned int digits)
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
	else if (digits < 19)
		return 64;
	return 128;
#else
	return 64;
#endif
}

unsigned int bits2digits(unsigned int bits)
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
	else if (bits <= 64)
		return 19;
	return 39;
#else
	return 19;
#endif
}

unsigned int type_digits_to_char_digits(sql_subtype *t)
{
	if (!t)
		return 0;
	switch (t->type->eclass) {
		case EC_BIT:
			return 1;
		case EC_POS:
		case EC_NUM:
		case EC_MONTH:
			return bits2digits(t->digits) + 1; /* add '-' */
		case EC_FLT:
			return bits2digits(t->digits) + 2; /* TODO for floating-points maybe more is needed */
		case EC_DEC:
		case EC_SEC:
			return t->digits + 2; /* add '-' and '.' */
		case EC_TIMESTAMP:
		case EC_TIMESTAMP_TZ:
			return 40; /* TODO this needs more tunning */
		case EC_TIME:
		case EC_TIME_TZ:
			return 20; /* TODO this needs more tunning */
		case EC_DATE:
			return 20; /* TODO this needs more tunning */
		case EC_BLOB:
			return t->digits * 2; /* TODO BLOBs don't have digits, so this is wrong */
		default:
			return t->digits; /* What to do with EC_GEOM? */
	}
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
/* EC_BLOB */		{ 0, 0, 0, 1, 1, 1, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0 },
/* EC_POS */		{ 0, 0, 2, 1, 1, 0, 1, 1, 0, 0, 1, 1, 0, 0, 0, 0, 0, 0, 0 },
/* EC_NUM */		{ 0, 0, 2, 1, 1, 0, 1, 1, 0, 0, 1, 1, 0, 0, 0, 0, 0, 0, 0 },
/* EC_MONTH*/		{ 0, 0, 0, 1, 1, 0, 0, 0, 1, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0 },
/* EC_SEC*/			{ 0, 0, 0, 1, 1, 0, 0, 0, 0, 1, 0, 0, 1, 1, 0, 0, 0, 0, 0 },
/* EC_DEC */		{ 0, 0, 0, 1, 1, 0, 1, 1, 0, 0, 1, 1, 0, 0, 0, 0, 0, 0, 0 },
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
	return strcmp("sql_add", fnm) == 0 || strcmp("sql_mul", fnm) == 0 || strcmp("scale_up", fnm) == 0;
}

void
base_init(sql_allocator *sa, sql_base * b, sqlid id, int flags, const char *name)
{
	*b = (sql_base) {
		.id = id,
		.flags = flags,
		.refcnt = 1,
		.name = (name) ? SA_STRDUP(sa, name) : NULL,
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
		nlt = TYPE_hge;
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
		localtype = TYPE_hge;
		if (digits >= 128)
			digits = 127;
#else
		localtype = TYPE_lng;
		if (digits >= 64)
			digits = 63;
#endif
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

	if (t1->type->eclass == t2->type->eclass && t1->type->eclass == EC_SEC)
		return 0;
	if (t1->type->eclass == t2->type->eclass && t1->type->eclass == EC_MONTH)
		return 0;
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

char *
sql_subtype_string(sql_allocator *sa, sql_subtype *t)
{
	char buf[BUFSIZ];

	if (t->digits && t->scale)
		snprintf(buf, BUFSIZ, "%s(%u,%u)", t->type->sqlname, t->digits, t->scale);
	else if (t->digits && t->type->radix != 2)
		snprintf(buf, BUFSIZ, "%s(%u)", t->type->sqlname, t->digits);
	else
		snprintf(buf, BUFSIZ, "%s", t->type->sqlname);
	return sa_strdup(sa, buf);
}

char *
subtype2string2(sql_allocator *sa, sql_subtype *tpe) /* distinguish char(n), decimal(n,m) from other SQL types */
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
			return sql_subtype_string(sa, tpe);
		default:
			snprintf(buf, BUFSIZ, "%s", tpe->type->sqlname);
	}
	return sa_strdup(sa, buf);
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
	if (!f->mod)
		return "";
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

				/* same scale as the input */
				if (member && member->scale > scale)
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
				if (member && (f->fix_scale == INOUT || r->type->eclass == EC_ANY))
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
				sql_subtype *opt = tn->data;

				if (s->type.type->eclass == EC_ANY) {
					if (!st || st->type->eclass == EC_ANY) /* if input parameter is ANY, skip validation */
						st = tn->data;
					else if (opt && subtype_cmp(st, opt))
						return NULL;
				}
			}
		}
	}
	return fres;
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

static sqlid local_id = 1;

static sql_type *
sql_create_type(sql_allocator *sa, const char *sqlname, unsigned int digits, unsigned int scale, unsigned char radix, sql_class eclass, const char *name)
{
	sql_type *t = SA_ZNEW(sa, sql_type);

	base_init(sa, &t->base, local_id++, 0, name);
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
sql_create_func_(sql_allocator *sa, const char *name, const char *mod, const char *imp, sql_ftype type, bit semantics, bit side_effect,
				 int fix_scale, unsigned int res_scale, sql_type *res, int nargs, va_list valist)
{
	list *ops = SA_LIST(sa, (fdestroy) &arg_destroy);
	sql_arg *fres = NULL;
	sql_func *t = SA_ZNEW(sa, sql_func);

	for (int i = 0; i < nargs; i++) {
		sql_type *tpe = va_arg(valist, sql_type*);
		list_append(ops, create_arg(sa, NULL, sql_create_subtype(sa, tpe, 0, 0), ARG_IN));
	}
	if (res)
		fres = create_arg(sa, NULL, sql_create_subtype(sa, res, 0, 0), ARG_OUT);
	base_init(sa, &t->base, local_id++, 0, name);

	t->imp = sa_strdup(sa, imp);
	t->mod = sa_strdup(sa, mod);
	t->ops = ops;
	t->type = type;
	if (fres) {
		if (res_scale)
			fres->type.scale = res_scale;
		t->res = list_append(SA_LIST(sa, (fdestroy) &arg_destroy), fres);
	} else
		t->res = NULL;
	t->nr = list_length(funcs);
	t->sql = 0;
	t->lang = FUNC_LANG_INT;
	t->semantics = semantics;
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
	res = sql_create_func_(sa, name, mod, imp, F_PROC, TRUE, side_effect, SCALE_NONE, 0, NULL, nargs, valist);
	va_end(valist);
	return res;
}

static sql_func *
sql_create_func(sql_allocator *sa, const char *name, const char *mod, const char *imp, bit semantics, bit side_effect, int fix_scale,
				unsigned int res_scale, sql_type *fres, int nargs, ...)
{
	sql_func *res;
	va_list valist;

	va_start(valist, nargs);
	res = sql_create_func_(sa, name, mod, imp, F_FUNC, semantics, side_effect, fix_scale, res_scale, fres, nargs, valist);
	va_end(valist);
	return res;
}

static sql_func *
sql_create_aggr(sql_allocator *sa, const char *name, const char *mod, const char *imp, bit semantics, sql_type *fres, int nargs, ...)
{
	sql_func *res;
	va_list valist;

	va_start(valist, nargs);
	res = sql_create_func_(sa, name, mod, imp, F_AGGR, semantics, FALSE, SCALE_NONE, 0, fres, nargs, valist);
	va_end(valist);
	return res;
}

static sql_func *
sql_create_filter(sql_allocator *sa, const char *name, const char *mod, const char *imp, bit semantics, bit side_effect, int fix_scale,
				unsigned int res_scale, int nargs, ...)
{
	sql_func *res;
	va_list valist;

	va_start(valist, nargs);
	res = sql_create_func_(sa, name, mod, imp, F_FILT, semantics, side_effect, fix_scale, res_scale, BIT, nargs, valist);
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
	res = sql_create_func_(sa, name, mod, imp, F_UNION, TRUE, side_effect, fix_scale, res_scale, fres, nargs, valist);
	va_end(valist);
	return res;
}

static sql_func *
sql_create_analytic(sql_allocator *sa, const char *name, const char *mod, const char *imp, int fix_scale, sql_type *fres, int nargs, ...)
{
	sql_func *res;
	va_list valist;

	va_start(valist, nargs);
	res = sql_create_func_(sa, name, mod, imp, F_ANALYTIC, TRUE, FALSE, fix_scale, 0, fres, nargs, valist);
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
	sql_type **decimals, **floats, **dates, **t;
	sql_type *STR, *BTE, *SHT, *INT, *LNG, *OID, *FLT, *DBL, *DEC;
#ifdef HAVE_HGE
	sql_type *HGE = NULL;
#endif
	sql_type *SECINT, *DAYINT, *MONINT, *DTE;
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
	/* create clob type first, so functions by default will bind to the clob version which doesn't require length validation on some cases */
	STR = *t++ = sql_create_type(sa, "CLOB",    0, 0, 0, EC_STRING, "str");
	*t++ = sql_create_type(sa, "VARCHAR", 0, 0, 0, EC_STRING, "str");
	*t++ = sql_create_type(sa, "CHAR",    0, 0, 0, EC_CHAR,   "str");

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
	LargestINT =
		HGE = *t++ = sql_create_type(sa, "HUGEINT",  128, SCALE_FIX, 2, EC_NUM, "hge");
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
	LargestDEC =
		*t++ = sql_create_type(sa, "DECIMAL", 38, SCALE_FIX, 10, EC_DEC, "hge");
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
	MONINT = *t++ = sql_create_type(sa, "MONTH_INTERVAL", 3, 0, 10, EC_MONTH, "int"); /* 1 .. 13 enumerates the 13 different interval types */
	DAYINT = *t++ = sql_create_type(sa, "DAY_INTERVAL", 4, 0, 10, EC_SEC, "lng");
	SECINT = *t++ = sql_create_type(sa, "SEC_INTERVAL", 13, SCALE_FIX, 10, EC_SEC, "lng");
	TME = *t++ = sql_create_type(sa, "TIME", 7, 0, 0, EC_TIME, "daytime");
	TMETZ = *t++ = sql_create_type(sa, "TIMETZ", 7, SCALE_FIX, 0, EC_TIME_TZ, "daytime");
	DTE = *t++ = sql_create_type(sa, "DATE", 0, 0, 0, EC_DATE, "date");
	TMESTAMP = *t++ = sql_create_type(sa, "TIMESTAMP", 7, 0, 0, EC_TIMESTAMP, "timestamp");
	TMESTAMPTZ = *t++ = sql_create_type(sa, "TIMESTAMPTZ", 7, SCALE_FIX, 0, EC_TIMESTAMP_TZ, "timestamp");

	BLOB = *t++ = sql_create_type(sa, "BLOB", 0, 0, 0, EC_BLOB, "blob");

	sql_create_func(sa, "length", "blob", "nitems", FALSE, FALSE, SCALE_NONE, 0, INT, 1, BLOB);
	sql_create_func(sa, "octet_length", "blob", "nitems", FALSE, FALSE, SCALE_NONE, 0, INT, 1, BLOB);

	if (backend_has_module(&(int){0}, "geom")) { /* not the old version, change into check for module existence */
		// the geom module is loaded
		GEOM = *t++ = sql_create_type(sa, "GEOMETRY", 0, SCALE_NONE, 0, EC_GEOM, "wkb");
		/*POINT =*/ //*t++ = sql_create_type(sa, "POINT", 0, SCALE_FIX, 0, EC_GEOM, "wkb");
		// TODO: The GEOMETRYA  and MBR types should actually also be part of EC_GEOM. However this requires more (bat)calc.<convert> functions.
		*t++ = sql_create_type(sa, "GEOMETRYA", 0, SCALE_NONE, 0, EC_EXTERNAL, "wkba");

		MBR = *t++ = sql_create_type(sa, "MBR", 0, SCALE_NONE, 0, EC_EXTERNAL, "mbr");

		/* mbr operator functions */
		sql_create_func(sa, "mbr_overlap", "geom", "mbrOverlaps", TRUE, FALSE, SCALE_FIX, 0, BIT, 2, GEOM, GEOM);
		sql_create_func(sa, "mbr_overlap", "geom", "mbrOverlaps", TRUE, FALSE, SCALE_FIX, 0, BIT, 2, MBR, MBR);
		sql_create_func(sa, "mbr_above", "geom", "mbrAbove", TRUE, FALSE, SCALE_FIX, 0, BIT, 2, GEOM, GEOM);
		sql_create_func(sa, "mbr_above", "geom", "mbrAbove", TRUE, FALSE, SCALE_FIX, 0, BIT, 2, MBR, MBR);
		sql_create_func(sa, "mbr_below", "geom", "mbrBelow", TRUE, FALSE, SCALE_FIX, 0, BIT, 2, GEOM, GEOM);
		sql_create_func(sa, "mbr_below", "geom", "mbrBelow", TRUE, FALSE, SCALE_FIX, 0, BIT, 2, MBR, MBR);
		sql_create_func(sa, "mbr_right", "geom", "mbrRight", TRUE, FALSE, SCALE_FIX, 0, BIT, 2, GEOM, GEOM);
		sql_create_func(sa, "mbr_right", "geom", "mbrRight", TRUE, FALSE, SCALE_FIX, 0, BIT, 2, MBR, MBR);
		sql_create_func(sa, "mbr_left", "geom", "mbrLeft", TRUE, FALSE, SCALE_FIX, 0, BIT, 2, GEOM, GEOM);
		sql_create_func(sa, "mbr_left", "geom", "mbrLeft", TRUE, FALSE, SCALE_FIX, 0, BIT, 2, MBR, MBR);
		sql_create_func(sa, "mbr_overlap_or_above", "geom", "mbrOverlapOrAbove", TRUE, FALSE, SCALE_FIX, 0, BIT, 2, GEOM, GEOM);
		sql_create_func(sa, "mbr_overlap_or_above", "geom", "mbrOverlapOrAbove", TRUE, FALSE, SCALE_FIX, 0, BIT, 2, MBR, MBR);
		sql_create_func(sa, "mbr_overlap_or_below", "geom", "mbrOverlapOrBelow", TRUE, FALSE, SCALE_FIX, 0, BIT, 2, GEOM, GEOM);
		sql_create_func(sa, "mbr_overlap_or_below", "geom", "mbrOverlapOrBelow", TRUE, FALSE, SCALE_FIX, 0, BIT, 2, MBR, MBR);
		sql_create_func(sa, "mbr_overlap_or_right", "geom", "mbrOverlapOrRight", TRUE, FALSE, SCALE_FIX, 0, BIT, 2, GEOM, GEOM);
		sql_create_func(sa, "mbr_overlap_or_right", "geom", "mbrOverlapOrRight", TRUE, FALSE, SCALE_FIX, 0, BIT, 2, MBR, MBR);
		sql_create_func(sa, "mbr_overlap_or_left", "geom", "mbrOverlapOrLeft", TRUE, FALSE, SCALE_FIX, 0, BIT, 2, GEOM, GEOM);
		sql_create_func(sa, "mbr_overlap_or_left", "geom", "mbrOverlapOrLeft", TRUE, FALSE, SCALE_FIX, 0, BIT, 2, MBR, MBR);
		sql_create_func(sa, "mbr_contains", "geom", "mbrContains", TRUE, FALSE, SCALE_FIX, 0, BIT, 2, GEOM, GEOM);
		sql_create_func(sa, "mbr_contains", "geom", "mbrContains", TRUE, FALSE, SCALE_FIX, 0, BIT, 2, MBR, MBR);
		sql_create_func(sa, "mbr_contained", "geom", "mbrContained", TRUE, FALSE, SCALE_FIX, 0, BIT, 2, GEOM, GEOM);
		sql_create_func(sa, "mbr_contained", "geom", "mbrContained", TRUE, FALSE, SCALE_FIX, 0, BIT, 2, MBR, MBR);
		sql_create_func(sa, "mbr_equal", "geom", "mbrEqual", TRUE, FALSE, SCALE_FIX, 0, BIT, 2, GEOM, GEOM);
		sql_create_func(sa, "mbr_equal", "geom", "mbrEqual", TRUE, FALSE, SCALE_FIX, 0, BIT, 2, MBR, MBR);
		sql_create_func(sa, "mbr_distance", "geom", "mbrDistance", TRUE, FALSE, SCALE_FIX, 0, DBL, 2, GEOM, GEOM);
		sql_create_func(sa, "mbr_distance", "geom", "mbrDistance", TRUE, FALSE, SCALE_FIX, 0, DBL, 2, MBR, MBR);
		sql_create_func(sa, "left_shift", "geom", "mbrLeft", TRUE, FALSE, SCALE_FIX, 0, BIT, 2, GEOM, GEOM);
		sql_create_func(sa, "left_shift", "geom", "mbrLeft", TRUE, FALSE, SCALE_FIX, 0, BIT, 2, MBR, MBR);
		sql_create_func(sa, "right_shift", "geom", "mbrRight", TRUE, FALSE, SCALE_FIX, 0, BIT, 2, GEOM, GEOM);
		sql_create_func(sa, "right_shift", "geom", "mbrRight", TRUE, FALSE, SCALE_FIX, 0, BIT, 2, MBR, MBR);
	}

	*t = NULL;

//	sql_create_func(sa, "st_pointfromtext", "geom", "st_pointformtext", FALSE, SCALE_NONE, 0, OID, 1, OID);
	/* The grouping aggregate doesn't have a backend implementation. It gets replaced at rel_unnest */
	sql_create_aggr(sa, "grouping", "", "", TRUE, BTE, 1, ANY);
	sql_create_aggr(sa, "grouping", "", "", TRUE, SHT, 1, ANY);
	sql_create_aggr(sa, "grouping", "", "", TRUE, INT, 1, ANY);
	sql_create_aggr(sa, "grouping", "", "", TRUE, LNG, 1, ANY);
#ifdef HAVE_HGE
	sql_create_aggr(sa, "grouping", "", "", TRUE, HGE, 1, ANY);
#endif

	sql_create_aggr(sa, "not_unique", "aggr", "not_unique", TRUE, BIT, 1, OID);
	/* well to be precise it does reduce and map */

	/* functions needed for all types */
	sql_create_func(sa, "hash", "mkey", "hash", TRUE, FALSE, SCALE_FIX, 0, LNG, 1, ANY);
	sql_create_func(sa, "rotate_xor_hash", "calc", "rotate_xor_hash", TRUE, FALSE, SCALE_NONE, 0, LNG, 3, LNG, INT, ANY);
	sql_create_func(sa, "=", "calc", "=", FALSE, FALSE, SCALE_FIX, 0, BIT, 2, ANY, ANY);
	sql_create_func(sa, "<>", "calc", "!=", FALSE, FALSE, SCALE_FIX, 0, BIT, 2, ANY, ANY);
	sql_create_func(sa, "isnull", "calc", "isnil", TRUE, FALSE, SCALE_FIX, 0, BIT, 1, ANY);
	sql_create_func(sa, "isnotnull", "calc", "isnotnil", TRUE, FALSE, SCALE_FIX, 0, BIT, 1, ANY);
	sql_create_func(sa, ">", "calc", ">", FALSE, FALSE, SCALE_FIX, 0, BIT, 2, ANY, ANY);
	sql_create_func(sa, ">=", "calc", ">=", FALSE, FALSE, SCALE_FIX, 0, BIT, 2, ANY, ANY);
	sql_create_func(sa, "<", "calc", "<", FALSE, FALSE, SCALE_FIX, 0, BIT, 2, ANY, ANY);
	sql_create_func(sa, "<=", "calc", "<=", FALSE, FALSE, SCALE_FIX, 0, BIT, 2, ANY, ANY);
	sql_create_func(sa, "between", "calc", "between", FALSE, FALSE, SCALE_FIX, 0, BIT, 8, ANY, ANY, ANY, BIT, BIT, BIT, BIT, BIT);
	sql_create_aggr(sa, "zero_or_one", "aggr", "zero_or_one", TRUE, ANY, 1, ANY);
	sql_create_aggr(sa, "all", "aggr", "all", TRUE, ANY, 1, ANY);
	sql_create_aggr(sa, "null", "aggr", "null", TRUE, BIT, 1, ANY);
	sql_create_func(sa, "any", "sql", "any", TRUE, FALSE, SCALE_NONE, 0, BIT, 3, BIT, BIT, BIT);
	sql_create_func(sa, "all", "sql", "all", TRUE, FALSE, SCALE_NONE, 0, BIT, 3, BIT, BIT, BIT);
	sql_create_aggr(sa, "anyequal", "aggr", "anyequal", TRUE, BIT, 1, ANY); /* needs 3 arguments (l,r,nil)(ugh) */
	sql_create_aggr(sa, "allnotequal", "aggr", "allnotequal", TRUE, BIT, 1, ANY); /* needs 3 arguments (l,r,nil)(ugh) */
	sql_create_func(sa, "sql_anyequal", "aggr", "anyequal", TRUE, FALSE, SCALE_NONE, 0, BIT, 2, ANY, ANY);
	sql_create_func(sa, "sql_not_anyequal", "aggr", "not_anyequal", TRUE, FALSE, SCALE_NONE, 0, BIT, 2, ANY, ANY);
	sql_create_aggr(sa, "exist", "aggr", "exist", TRUE, BIT, 1, ANY);
	sql_create_aggr(sa, "not_exist", "aggr", "not_exist", TRUE, BIT, 1, ANY);
	sql_create_func(sa, "sql_exists", "aggr", "exist", TRUE, FALSE, SCALE_NONE, 0, BIT, 1, ANY);
	sql_create_func(sa, "sql_not_exists", "aggr", "not_exist", TRUE, FALSE, SCALE_NONE, 0, BIT, 1, ANY);
	/* needed for relational version */
	sql_create_func(sa, "identity", "calc", "identity", TRUE, FALSE, SCALE_NONE, 0, OID, 1, ANY);
	sql_create_func(sa, "rowid", "calc", "identity", TRUE, FALSE, SCALE_NONE, 0, INT, 1, ANY);
	/* needed for indices/clusters oid(schema.table,val) returns max(head(schema.table))+1 */
	sql_create_func(sa, "rowid", "calc", "rowid", TRUE, FALSE, SCALE_NONE, 0, OID, 3, ANY, STR, STR);
	sql_create_aggr(sa, "min", "aggr", "min", FALSE, ANY, 1, ANY);
	sql_create_aggr(sa, "max", "aggr", "max", FALSE, ANY, 1, ANY);
	sql_create_func(sa, "sql_min", "calc", "min", FALSE, FALSE, SCALE_FIX, 0, ANY, 2, ANY, ANY);
	sql_create_func(sa, "sql_max", "calc", "max", FALSE, FALSE, SCALE_FIX, 0, ANY, 2, ANY, ANY);
	sql_create_func(sa, "least", "calc", "min_no_nil", TRUE, FALSE, SCALE_FIX, 0, ANY, 2, ANY, ANY);
	sql_create_func(sa, "greatest", "calc", "max_no_nil", TRUE, FALSE, SCALE_FIX, 0, ANY, 2, ANY, ANY);
	sql_create_func(sa, "ifthenelse", "calc", "ifthenelse", TRUE, FALSE, SCALE_FIX, 0, ANY, 3, BIT, ANY, ANY);
	/* nullif, coalesce and casewhen don't have a backend implementation */
	sql_create_func(sa, "nullif", "", "", TRUE, FALSE, SCALE_FIX, 0, ANY, 2, ANY, ANY);
	sql_create_func(sa, "coalesce", "", "", TRUE, FALSE, SCALE_FIX, 0, ANY, 2, ANY, ANY);
	sql_create_func(sa, "casewhen", "", "", TRUE, FALSE, SCALE_FIX, 0, ANY, 2, ANY, ANY);
	/* needed for count(*) and window functions without input col */
	sql_create_func(sa, "star", "", "", TRUE, FALSE, SCALE_FIX, 0, ANY, 0);

	/* sum for numerical and decimals */
	sql_create_aggr(sa, "sum", "aggr", "sum", FALSE, LargestINT, 1, BTE);
	sql_create_aggr(sa, "sum", "aggr", "sum", FALSE, LargestINT, 1, SHT);
	sql_create_aggr(sa, "sum", "aggr", "sum", FALSE, LargestINT, 1, INT);
	//sql_create_aggr(sa, "sum", "aggr", "sum", LargestINT, 1, LNG, LargestINT);
#ifdef HAVE_HGE
	sql_create_aggr(sa, "sum", "aggr", "sum", FALSE, LargestINT, 1, HGE);
#endif
	sql_create_aggr(sa, "sum", "aggr", "sum", FALSE, LNG, 1, LNG);

	t = decimals; /* BTE */
	sql_create_aggr(sa, "sum", "aggr", "sum", FALSE, LargestDEC, 1, *(t));
	t++; /* SHT */
	sql_create_aggr(sa, "sum", "aggr", "sum", FALSE, LargestDEC, 1, *(t));
	t++; /* INT */
	sql_create_aggr(sa, "sum", "aggr", "sum", FALSE, LargestDEC, 1, *(t));
	t++; /* LNG */
	sql_create_aggr(sa, "sum", "aggr", "sum", FALSE, LargestDEC, 1, *(t));
#ifdef HAVE_HGE
	t++; /* HGE */
	sql_create_aggr(sa, "sum", "aggr", "sum", FALSE, LargestDEC, 1, *(t));
#endif

	/* prod for numerical and decimals */
	sql_create_aggr(sa, "prod", "aggr", "prod", FALSE, LargestINT, 1, BTE);
	sql_create_aggr(sa, "prod", "aggr", "prod", FALSE, LargestINT, 1, SHT);
	sql_create_aggr(sa, "prod", "aggr", "prod", FALSE, LargestINT, 1, INT);
	sql_create_aggr(sa, "prod", "aggr", "prod", FALSE, LargestINT, 1, LNG);
#ifdef HAVE_HGE
	sql_create_aggr(sa, "prod", "aggr", "prod", FALSE, LargestINT, 1, HGE);
#endif

#if 0
	/* prod for decimals introduce errors in the output scales */
	t = decimals; /* BTE */
	sql_create_aggr(sa, "prod", "aggr", "prod", FALSE, LargestDEC, 1, *(t));
	t++; /* SHT */
	sql_create_aggr(sa, "prod", "aggr", "prod", FALSE, LargestDEC, 1, *(t));
	t++; /* INT */
	sql_create_aggr(sa, "prod", "aggr", "prod", FALSE, LargestDEC, 1, *(t));
	t++; /* LNG */
	sql_create_aggr(sa, "prod", "aggr", "prod", FALSE, LargestDEC, 1, *(t));
#ifdef HAVE_HGE
	t++; /* HGE */
	sql_create_aggr(sa, "prod", "aggr", "prod", FALSE, LargestDEC, 1, *(t));
#endif
#endif

	for (t = numerical; t < dates; t++) {
		if (*t == OID)
			continue;
		sql_create_func(sa, "mod", "calc", "%", FALSE, FALSE, SCALE_FIX, 0, *t, 2, *t, *t);
	}

	for (t = floats; t < dates; t++) {
		sql_create_aggr(sa, "sum", "aggr", "sum", FALSE, *t, 1, *t);
		sql_create_aggr(sa, "prod", "aggr", "prod", FALSE, *t, 1, *t);
	}
	sql_create_aggr(sa, "sum", "aggr", "sum", FALSE, MONINT, 1, MONINT);
	sql_create_aggr(sa, "sum", "aggr", "sum", FALSE, DAYINT, 1, DAYINT);
	sql_create_aggr(sa, "sum", "aggr", "sum", FALSE, SECINT, 1, SECINT);
	/* do DBL first so that it is chosen as cast destination for
	 * unknown types */
	sql_create_aggr(sa, "avg", "aggr", "avg", FALSE, DBL, 1, DBL);
	sql_create_aggr(sa, "avg", "aggr", "avg", FALSE, DBL, 1, BTE);
	sql_create_aggr(sa, "avg", "aggr", "avg", FALSE, DBL, 1, SHT);
	sql_create_aggr(sa, "avg", "aggr", "avg", FALSE, DBL, 1, INT);
	sql_create_aggr(sa, "avg", "aggr", "avg", FALSE, DBL, 1, LNG);
#ifdef HAVE_HGE
	sql_create_aggr(sa, "avg", "aggr", "avg", FALSE, DBL, 1, HGE);
#endif
	sql_create_aggr(sa, "avg", "aggr", "avg", FALSE, DBL, 1, FLT);

	t = decimals; /* BTE */
	sql_create_aggr(sa, "avg", "aggr", "avg", FALSE, *(t), 1, *(t));
	t++; /* SHT */
	sql_create_aggr(sa, "avg", "aggr", "avg", FALSE, *(t), 1, *(t));
	t++; /* INT */
	sql_create_aggr(sa, "avg", "aggr", "avg", FALSE, *(t), 1, *(t));
	t++; /* LNG */
	sql_create_aggr(sa, "avg", "aggr", "avg", FALSE, *(t), 1, *(t));
#ifdef HAVE_HGE
	t++; /* HGE */
	sql_create_aggr(sa, "avg", "aggr", "avg", FALSE, *(t), 1, *(t));
#endif

	sql_create_aggr(sa, "avg", "aggr", "avg", FALSE, MONINT, 1, MONINT);
	sql_create_aggr(sa, "avg", "aggr", "avg", FALSE, DAYINT, 1, DAYINT);
	sql_create_aggr(sa, "avg", "aggr", "avg", FALSE, SECINT, 1, SECINT);

	sql_create_aggr(sa, "count_no_nil", "aggr", "count_no_nil", TRUE, LNG, 0);
	sql_create_aggr(sa, "count", "aggr", "count", TRUE, LNG, 1, ANY);

	for (t = strings; t < numerical; t++) {
		sql_create_aggr(sa, "listagg", "aggr", "str_group_concat", TRUE, *t, 1, *t);
		sql_create_aggr(sa, "listagg", "aggr", "str_group_concat", TRUE, *t, 2, *t, *t);
	}

	/* order based operators */
	sql_create_analytic(sa, "diff", "sql", "diff", SCALE_NONE, BIT, 1, ANY);
	sql_create_analytic(sa, "diff", "sql", "diff", SCALE_NONE, BIT, 2, BIT, ANY);
	for (t = numerical; *t != TME; t++) {
		if (*t == OID)
			continue;
		sql_create_analytic(sa, "window_bound", "sql", "window_bound", SCALE_NONE, OID, 5, ANY, INT, INT, INT, *t);
		sql_create_analytic(sa, "window_bound", "sql", "window_bound", SCALE_NONE, OID, 6, BIT, ANY, INT, INT, INT, *t);
	}

	sql_create_analytic(sa, "rank", "sql", "rank", SCALE_NONE, INT, 1, ANY);
	sql_create_analytic(sa, "dense_rank", "sql", "dense_rank", SCALE_NONE, INT, 1, ANY);
	sql_create_analytic(sa, "row_number", "sql", "row_number", SCALE_NONE, INT, 1, ANY);
	sql_create_analytic(sa, "percent_rank", "sql", "percent_rank", SCALE_NONE, DBL, 1, ANY);
	sql_create_analytic(sa, "cume_dist", "sql", "cume_dist", SCALE_NONE, DBL, 1, ANY);

	sql_create_analytic(sa, "ntile", "sql", "ntile", SCALE_NONE, BTE, 2, ANY, BTE);
	sql_create_analytic(sa, "ntile", "sql", "ntile", SCALE_NONE, SHT, 2, ANY, SHT);
	sql_create_analytic(sa, "ntile", "sql", "ntile", SCALE_NONE, INT, 2, ANY, INT);
	sql_create_analytic(sa, "ntile", "sql", "ntile", SCALE_NONE, LNG, 2, ANY, LNG);
#ifdef HAVE_HGE
	sql_create_analytic(sa, "ntile", "sql", "ntile", SCALE_NONE, HGE, 2, ANY, HGE);
#endif

	sql_create_analytic(sa, "lag", "sql", "lag", SCALE_NONE, ANY, 1, ANY);
	sql_create_analytic(sa, "lag", "sql", "lag", SCALE_NONE, ANY, 2, ANY, BTE);
	sql_create_analytic(sa, "lag", "sql", "lag", SCALE_NONE, ANY, 2, ANY, SHT);
	sql_create_analytic(sa, "lag", "sql", "lag", SCALE_NONE, ANY, 2, ANY, INT);
	sql_create_analytic(sa, "lag", "sql", "lag", SCALE_NONE, ANY, 2, ANY, LNG);
#ifdef HAVE_HGE
	sql_create_analytic(sa, "lag", "sql", "lag", SCALE_NONE, ANY, 2, ANY, HGE);
#endif
	sql_create_analytic(sa, "lag", "sql", "lag", SCALE_NONE, ANY, 3, ANY, BTE, ANY);
	sql_create_analytic(sa, "lag", "sql", "lag", SCALE_NONE, ANY, 3, ANY, SHT, ANY);
	sql_create_analytic(sa, "lag", "sql", "lag", SCALE_NONE, ANY, 3, ANY, INT, ANY);
	sql_create_analytic(sa, "lag", "sql", "lag", SCALE_NONE, ANY, 3, ANY, LNG, ANY);
#ifdef HAVE_HGE
	sql_create_analytic(sa, "lag", "sql", "lag", SCALE_NONE, ANY, 3, ANY, HGE, ANY);
#endif

	sql_create_analytic(sa, "lead", "sql", "lead", SCALE_NONE, ANY, 1, ANY);
	sql_create_analytic(sa, "lead", "sql", "lead", SCALE_NONE, ANY, 2, ANY, BTE);
	sql_create_analytic(sa, "lead", "sql", "lead", SCALE_NONE, ANY, 2, ANY, SHT);
	sql_create_analytic(sa, "lead", "sql", "lead", SCALE_NONE, ANY, 2, ANY, INT);
	sql_create_analytic(sa, "lead", "sql", "lead", SCALE_NONE, ANY, 2, ANY, LNG);
#ifdef HAVE_HGE
	sql_create_analytic(sa, "lead", "sql", "lead", SCALE_NONE, ANY, 2, ANY, HGE);
#endif
	sql_create_analytic(sa, "lead", "sql", "lead", SCALE_NONE, ANY, 3, ANY, BTE, ANY);
	sql_create_analytic(sa, "lead", "sql", "lead", SCALE_NONE, ANY, 3, ANY, SHT, ANY);
	sql_create_analytic(sa, "lead", "sql", "lead", SCALE_NONE, ANY, 3, ANY, INT, ANY);
	sql_create_analytic(sa, "lead", "sql", "lead", SCALE_NONE, ANY, 3, ANY, LNG, ANY);
#ifdef HAVE_HGE
	sql_create_analytic(sa, "lead", "sql", "lead", SCALE_NONE, ANY, 3, ANY, HGE, ANY);
#endif

	/* these analytic functions support frames */
	sql_create_analytic(sa, "first_value", "sql", "first_value", SCALE_NONE, ANY, 1, ANY);
	sql_create_analytic(sa, "last_value", "sql", "last_value", SCALE_NONE, ANY, 1, ANY);
	sql_create_analytic(sa, "nth_value", "sql", "nth_value", SCALE_NONE, ANY, 2, ANY, LNG);

	sql_create_analytic(sa, "count", "sql", "count", SCALE_NONE, LNG, 2, ANY, BIT);
	sql_create_analytic(sa, "min", "sql", "min", SCALE_NONE, ANY, 1, ANY);
	sql_create_analytic(sa, "max", "sql", "max", SCALE_NONE, ANY, 1, ANY);

	/* analytical sum for numerical and decimals */
	sql_create_analytic(sa, "sum", "sql", "sum", SCALE_NONE, LargestINT, 1, BTE);
	sql_create_analytic(sa, "sum", "sql", "sum", SCALE_NONE, LargestINT, 1, SHT);
	sql_create_analytic(sa, "sum", "sql", "sum", SCALE_NONE, LargestINT, 1, INT);
	sql_create_analytic(sa, "sum", "sql", "sum", SCALE_NONE, LargestINT, 1, LNG);
#ifdef HAVE_HGE
	sql_create_analytic(sa, "sum", "sql", "sum", SCALE_NONE, LargestINT, 1, HGE);
#endif

	t = decimals; /* BTE */
	sql_create_analytic(sa, "sum", "sql", "sum", SCALE_NONE, LargestDEC, 1, *(t));
	t++; /* SHT */
	sql_create_analytic(sa, "sum", "sql", "sum", SCALE_NONE, LargestDEC, 1, *(t));
	t++; /* INT */
	sql_create_analytic(sa, "sum", "sql", "sum", SCALE_NONE, LargestDEC, 1, *(t));
	t++; /* LNG */
	sql_create_analytic(sa, "sum", "sql", "sum", SCALE_NONE, LargestDEC, 1, *(t));
#ifdef HAVE_HGE
	t++; /* HGE */
	sql_create_analytic(sa, "sum", "sql", "sum", SCALE_NONE, LargestDEC, 1, *(t));
#endif

	/* analytical product for numerical and decimals */
	sql_create_analytic(sa, "prod", "sql", "prod", SCALE_NONE, LargestINT, 1, BTE);
	sql_create_analytic(sa, "prod", "sql", "prod", SCALE_NONE, LargestINT, 1, SHT);
	sql_create_analytic(sa, "prod", "sql", "prod", SCALE_NONE, LargestINT, 1, INT);
	sql_create_analytic(sa, "prod", "sql", "prod", SCALE_NONE, LargestINT, 1, LNG);
#ifdef HAVE_HGE
	sql_create_analytic(sa, "prod", "sql", "prod", SCALE_NONE, LargestINT, 1, HGE);
#endif

#if 0
	/* prod for decimals introduce errors in the output scales */
	t = decimals; /* BTE */
	sql_create_analytic(sa, "prod", "sql", "prod", SCALE_NONE, LargestDEC, 1, *(t));
	t++; /* SHT */
	sql_create_analytic(sa, "prod", "sql", "prod", SCALE_NONE, LargestDEC, 1, *(t));
	t++; /* INT */
	sql_create_analytic(sa, "prod", "sql", "prod", SCALE_NONE, LargestDEC, 1, *(t));
	t++; /* LNG */
	sql_create_analytic(sa, "prod", "sql", "prod", SCALE_NONE, LargestDEC, 1, *(t));
#ifdef HAVE_HGE
	t++; /* HGE */
	sql_create_analytic(sa, "prod", "sql", "prod", SCALE_NONE, LargestDEC, 1, *(t));
#endif
#endif

	for (t = floats; t < dates; t++) {
		sql_create_analytic(sa, "sum", "sql", "sum", SCALE_NONE, *t, 1, *t);
		sql_create_analytic(sa, "prod", "sql", "prod", SCALE_NONE, *t, 1, *t);
	}
	sql_create_analytic(sa, "sum", "sql", "sum", SCALE_NONE, MONINT, 1, MONINT);
	sql_create_analytic(sa, "sum", "sql", "sum", SCALE_NONE, DAYINT, 1, DAYINT);
	sql_create_analytic(sa, "sum", "sql", "sum", SCALE_NONE, SECINT, 1, SECINT);

	//analytical average for numerical types
	sql_create_analytic(sa, "avg", "sql", "avg", SCALE_NONE, DBL, 1, DBL);
	sql_create_analytic(sa, "avg", "sql", "avg", SCALE_NONE, DBL, 1, BTE);
	sql_create_analytic(sa, "avg", "sql", "avg", SCALE_NONE, DBL, 1, SHT);
	sql_create_analytic(sa, "avg", "sql", "avg", SCALE_NONE, DBL, 1, INT);
	sql_create_analytic(sa, "avg", "sql", "avg", SCALE_NONE, DBL, 1, LNG);
#ifdef HAVE_HGE
	sql_create_analytic(sa, "avg", "sql", "avg", SCALE_NONE, DBL, 1, HGE);
#endif

	sql_create_analytic(sa, "avg", "sql", "avg", SCALE_NONE, DBL, 1, FLT);

	t = decimals; /* BTE */
	sql_create_analytic(sa, "avg", "sql", "avg", SCALE_NONE, *(t), 1, *(t));
	t++; /* SHT */
	sql_create_analytic(sa, "avg", "sql", "avg", SCALE_NONE, *(t), 1, *(t));
	t++; /* INT */
	sql_create_analytic(sa, "avg", "sql", "avg", SCALE_NONE, *(t), 1, *(t));
	t++; /* LNG */
	sql_create_analytic(sa, "avg", "sql", "avg", SCALE_NONE, *(t), 1, *(t));
#ifdef HAVE_HGE
	t++; /* HGE */
	sql_create_analytic(sa, "avg", "sql", "avg", SCALE_NONE, *(t), 1, *(t));
#endif

	sql_create_analytic(sa, "avg", "sql", "avg", SCALE_NONE, MONINT, 1, MONINT);
	sql_create_analytic(sa, "avg", "sql", "avg", SCALE_NONE, DAYINT, 1, DAYINT);
	sql_create_analytic(sa, "avg", "sql", "avg", SCALE_NONE, SECINT, 1, SECINT);

	for (t = strings; t < numerical; t++) {
		sql_create_analytic(sa, "listagg", "sql", "str_group_concat", SCALE_NONE, *t, 1, *t);
		sql_create_analytic(sa, "listagg", "sql", "str_group_concat", SCALE_NONE, *t, 2, *t, *t);
	}
	sql_create_func(sa, "and", "calc", "and", TRUE, FALSE, SCALE_FIX, 0, BIT, 2, BIT, BIT);
	sql_create_func(sa, "or",  "calc",  "or", TRUE, FALSE, SCALE_FIX, 0, BIT, 2, BIT, BIT);
	sql_create_func(sa, "xor", "calc", "xor", FALSE, FALSE, SCALE_FIX, 0, BIT, 2, BIT, BIT);
	sql_create_func(sa, "not", "calc", "not", FALSE, FALSE, SCALE_FIX, 0, BIT, 1, BIT);

	/* functions for interval types */
	for (t = dates; *t != TME; t++) {
		sql_subtype *lt = sql_bind_localtype((*t)->base.name);

		sql_create_func(sa, "sql_sub", "calc", "-", FALSE, FALSE, SCALE_NONE, 0, *t, 2, *t, *t);
		sql_create_func(sa, "sql_add", "calc", "+", FALSE, FALSE, SCALE_NONE, 0, *t, 2, *t, *t);
		sql_create_func(sa, "sql_neg", "calc", "-", FALSE, FALSE, INOUT, 0, *t, 1, *t);
		sql_create_func(sa, "abs", "calc", "abs", FALSE, FALSE, SCALE_NONE, 0, *t, 1, *t);
		sql_create_func(sa, "sign", "calc", "sign", FALSE, FALSE, SCALE_NONE, 0, BTE, 1, *t);
		/* scale fixing for intervals */
		sql_create_func(sa, "scale_up", "calc", "*", FALSE, FALSE, SCALE_NONE, 0, *t, 2, *t, lt->type);
		sql_create_func(sa, "scale_down", "calc", "dec_round", FALSE, FALSE, SCALE_NONE, 0, *t, 2, *t, lt->type);
	}

	/* allow smaller types for arguments of mul/div */
	for (t = numerical, t++; t != floats; t++) {
		sql_type **u;
		for (u = numerical, u++; u != decimals; u++) {
			if (*t == OID)
				continue;
			if (t != u && (*t)->localtype >  (*u)->localtype) {
				sql_create_func(sa, "sql_mul", "calc", "*", FALSE, FALSE, SCALE_MUL, 0, *t, 2, *t, *u);
				sql_create_func(sa, "sql_mul", "calc", "*", FALSE, FALSE, SCALE_MUL, 0, *t, 2, *u, *t);
				sql_create_func(sa, "sql_div", "calc", "/", FALSE, FALSE, SCALE_DIV, 0, *t, 2, *t, *u);
			}
		}
	}
	for (t = decimals, t++; t != floats; t++) {
		sql_type **u;

		for (u = decimals, u++; u != floats; u++) {
			if (t != u && (*t)->localtype >  (*u)->localtype) {
				sql_create_func(sa, "sql_mul", "calc", "*", FALSE, FALSE, SCALE_MUL, 0, *t, 2, *t, *u);
				sql_create_func(sa, "sql_div", "calc", "/", FALSE, FALSE, SCALE_DIV, 0, *t, 2, *t, *u);
			}
		}
	}

	/* all numericals */
	for (t = numerical; t < dates; t++) {
		sql_subtype *lt;

		if (*t == OID)
			continue;

		lt = sql_bind_localtype((*t)->base.name);

		sql_create_func(sa, "sql_sub", "calc", "-", FALSE, FALSE, SCALE_FIX, 0, *t, 2, *t, *t);
		sql_create_func(sa, "sql_add", "calc", "+", FALSE, FALSE, SCALE_FIX, 0, *t, 2, *t, *t);
		sql_create_func(sa, "sql_mul", "calc", "*", FALSE, FALSE, SCALE_MUL, 0, *t, 2, *t, *t);
		sql_create_func(sa, "sql_div", "calc", "/", FALSE, FALSE, SCALE_DIV, 0, *t, 2, *t, *t);
		if (t < floats) {
			sql_create_func(sa, "bit_and", "calc", "and", FALSE, FALSE, SCALE_FIX, 0, *t, 2, *t, *t);
			sql_create_func(sa, "bit_or", "calc", "or", FALSE, FALSE, SCALE_FIX, 0, *t, 2, *t, *t);
			sql_create_func(sa, "bit_xor", "calc", "xor", FALSE, FALSE, SCALE_FIX, 0, *t, 2, *t, *t);
			sql_create_func(sa, "bit_not", "calc", "not", FALSE, FALSE, SCALE_FIX, 0, *t, 1, *t);
			sql_create_func(sa, "left_shift", "calc", "<<", FALSE, FALSE, SCALE_FIX, 0, *t, 2, *t, INT);
			sql_create_func(sa, "right_shift", "calc", ">>", FALSE, FALSE, SCALE_FIX, 0, *t, 2, *t, INT);
		}
		sql_create_func(sa, "sql_neg", "calc", "-", FALSE, FALSE, INOUT, 0, *t, 1, *t);
		sql_create_func(sa, "abs", "calc", "abs", FALSE, FALSE, SCALE_FIX, 0, *t, 1, *t);
		sql_create_func(sa, "sign", "calc", "sign", FALSE, FALSE, SCALE_NONE, 0, BTE, 1, *t);
		/* scale fixing for all numbers */
		sql_create_func(sa, "scale_up", "calc", "*", FALSE, FALSE, SCALE_NONE, 0, *t, 2, *t, lt->type);
		sql_create_func(sa, "scale_down", "calc", "dec_round", FALSE, FALSE, SCALE_NONE, 0, *t, 2, *t, lt->type);
		/* numeric functions on INTERVALS */
		if (t >= floats || (*t)->localtype <= MONINT->localtype)
			sql_create_func(sa, "sql_mul", "calc", "*", FALSE, FALSE, SCALE_MUL, 0, MONINT, 2, MONINT, *t);
		if (t >= floats || (*t)->localtype <= DAYINT->localtype)
			sql_create_func(sa, "sql_mul", "calc", "*", FALSE, FALSE, SCALE_MUL, 0, DAYINT, 2, DAYINT, *t);
		if (t >= floats || (*t)->localtype <= SECINT->localtype)
			sql_create_func(sa, "sql_mul", "calc", "*", FALSE, FALSE, SCALE_MUL, 0, SECINT, 2, SECINT, *t);
		sql_create_func(sa, "sql_div", "calc", "/", FALSE, FALSE, SCALE_DIV, 0, MONINT, 2, MONINT, *t);
		sql_create_func(sa, "sql_div", "calc", "/", FALSE, FALSE, SCALE_DIV, 0, DAYINT, 2, DAYINT, *t);
		sql_create_func(sa, "sql_div", "calc", "/", FALSE, FALSE, SCALE_DIV, 0, SECINT, 2, SECINT, *t);
	}

	for (t = decimals, t++; t != floats; t++) {
		sql_type **u;
		for (u = numerical; u != floats; u++) {
			if (*u == OID)
				continue;
			if ((*t)->localtype > (*u)->localtype) {
				sql_create_func(sa, "sql_mul", "calc", "*", FALSE, FALSE, SCALE_MUL, 0, *t, 2, *t, *u);
				sql_create_func(sa, "sql_mul", "calc", "*", FALSE, FALSE, SCALE_MUL, 0, *t, 2, *u, *t);
			}
		}
	}

	for (t = decimals; t < dates; t++)
		sql_create_func(sa, "round", "calc", "round", FALSE, FALSE, INOUT, 0, *t, 2, *t, BTE);

	for (t = numerical; *t != TME; t++) {
		if (*t == OID || *t == FLT || *t == DBL)
			continue;
		for (sql_type **u = numerical; *u != TME; u++) {
			if (*u == OID || *u == FLT || *u == DBL)
				continue;
			if ((*t)->localtype > (*u)->localtype) {
				sql_create_func(sa, "scale_up", "calc", "*", FALSE, FALSE, SCALE_NONE, 0, *t, 2, *t, *u);
				sql_create_func(sa, "scale_up", "calc", "*", FALSE, FALSE, SCALE_NONE, 0, *t, 2, *u, *t);
			}
		}
	}

	for (t = floats; t < dates; t++) {
		sql_create_func(sa, "power", "mmath", "pow", FALSE, FALSE, SCALE_FIX, 0, *t, 2, *t, *t);
		sql_create_func(sa, "floor", "mmath", "floor", FALSE, FALSE, SCALE_FIX, 0, *t, 1, *t);
		sql_create_func(sa, "ceil", "mmath", "ceil", FALSE, FALSE, SCALE_FIX, 0, *t, 1, *t);
		sql_create_func(sa, "ceiling", "mmath", "ceil", FALSE, FALSE, SCALE_FIX, 0, *t, 1, *t);	/* JDBC */
		sql_create_func(sa, "sin", "mmath", "sin", FALSE, FALSE, SCALE_FIX, 0, *t, 1, *t);
		sql_create_func(sa, "cos", "mmath", "cos", FALSE, FALSE, SCALE_FIX, 0, *t, 1, *t);
		sql_create_func(sa, "tan", "mmath", "tan", FALSE, FALSE, SCALE_FIX, 0, *t, 1, *t);
		sql_create_func(sa, "asin", "mmath", "asin", FALSE, FALSE, SCALE_FIX, 0, *t, 1, *t);
		sql_create_func(sa, "acos", "mmath", "acos", FALSE, FALSE, SCALE_FIX, 0, *t, 1, *t);
		sql_create_func(sa, "atan", "mmath", "atan", FALSE, FALSE, SCALE_FIX, 0, *t, 1, *t);
		sql_create_func(sa, "atan", "mmath", "atan2", FALSE, FALSE, SCALE_FIX, 0, *t, 2, *t, *t);
		sql_create_func(sa, "sinh", "mmath", "sinh", FALSE, FALSE, SCALE_FIX, 0, *t, 1, *t);
		sql_create_func(sa, "cot", "mmath", "cot", FALSE, FALSE, SCALE_FIX, 0, *t, 1, *t);
		sql_create_func(sa, "cosh", "mmath", "cosh", FALSE, FALSE, SCALE_FIX, 0, *t, 1, *t);
		sql_create_func(sa, "tanh", "mmath", "tanh", FALSE, FALSE, SCALE_FIX, 0, *t, 1, *t);
		sql_create_func(sa, "sqrt", "mmath", "sqrt", FALSE, FALSE, SCALE_FIX, 0, *t, 1, *t);
		sql_create_func(sa, "cbrt", "mmath", "cbrt", FALSE, FALSE, SCALE_FIX, 0, *t, 1, *t);
		sql_create_func(sa, "exp", "mmath", "exp", FALSE, FALSE, SCALE_FIX, 0, *t, 1, *t);
		sql_create_func(sa, "log", "mmath", "log", FALSE, FALSE, SCALE_FIX, 0, *t, 1, *t);
		sql_create_func(sa, "ln", "mmath", "log", FALSE, FALSE, SCALE_FIX, 0, *t, 1, *t);
		sql_create_func(sa, "log", "mmath", "log2arg", FALSE, FALSE, SCALE_FIX, 0, *t, 2, *t, *t);
		sql_create_func(sa, "log10", "mmath", "log10", FALSE, FALSE, SCALE_FIX, 0, *t, 1, *t);
		sql_create_func(sa, "log2", "mmath", "log2", FALSE, FALSE, SCALE_FIX, 0, *t, 1, *t);
		sql_create_func(sa, "degrees", "mmath", "degrees", FALSE, FALSE, SCALE_FIX, 0, *t, 1, *t);
		sql_create_func(sa, "radians", "mmath", "radians", FALSE, FALSE, SCALE_FIX, 0, *t, 1, *t);
	}
	sql_create_func(sa, "pi", "mmath", "pi", FALSE, FALSE, SCALE_NONE, 0, DBL, 0);

	sql_create_func(sa, "rand", "mmath", "rand", TRUE, TRUE, SCALE_NONE, 0, INT, 0);
	sql_create_func(sa, "rand", "mmath", "sqlrand", TRUE, TRUE, SCALE_NONE, 0, INT, 1, INT);

	/* Date functions */
	sql_create_func(sa, "curdate", "mtime", "current_date", FALSE, FALSE, SCALE_NONE, 0, DTE, 0);
	sql_create_func(sa, "current_date", "mtime", "current_date", FALSE, FALSE, SCALE_NONE, 0, DTE, 0);
	sql_create_func(sa, "curtime", "mtime", "current_time", FALSE, FALSE, SCALE_NONE, 0, TMETZ, 0);
	sql_create_func(sa, "current_time", "mtime", "current_time", FALSE, FALSE, SCALE_NONE, 0, TMETZ, 0);
	sql_create_func(sa, "current_timestamp", "mtime", "current_timestamp", FALSE, FALSE, SCALE_NONE, 0, TMESTAMPTZ, 0);
	sql_create_func(sa, "localtime", "sql", "current_time", FALSE, FALSE, SCALE_NONE, 0, TME, 0);
	sql_create_func(sa, "localtimestamp", "sql", "current_timestamp", FALSE, FALSE, SCALE_NONE, 0, TMESTAMP, 0);

	sql_create_func(sa, "sql_sub", "mtime", "diff", FALSE, FALSE, SCALE_FIX, 0, DAYINT, 2, DTE, DTE);
	sql_create_func(sa, "sql_sub", "mtime", "diff", FALSE, FALSE, SCALE_NONE, 0, SECINT, 2, TMETZ, TMETZ);
	sql_create_func(sa, "sql_sub", "mtime", "diff", FALSE, FALSE, SCALE_FIX, 0, SECINT, 2, TME, TME);
	sql_create_func(sa, "sql_sub", "mtime", "diff", FALSE, FALSE, SCALE_NONE, 0, SECINT, 2, TMESTAMPTZ, TMESTAMPTZ);
	sql_create_func(sa, "sql_sub", "mtime", "diff", FALSE, FALSE, SCALE_FIX, 0, SECINT, 2, TMESTAMP, TMESTAMP);

	sql_create_func(sa, "sql_sub", "mtime", "date_sub_msec_interval", FALSE, FALSE, SCALE_NONE, 0, DTE, 2, DTE, SECINT);
	sql_create_func(sa, "sql_sub", "mtime", "date_sub_msec_interval", FALSE, FALSE, SCALE_NONE, 0, DTE, 2, DTE, DAYINT);
	sql_create_func(sa, "sql_sub", "mtime", "date_sub_month_interval", FALSE, FALSE, SCALE_NONE, 0, DTE, 2, DTE, MONINT);
	sql_create_func(sa, "sql_sub", "mtime", "time_sub_msec_interval", FALSE, FALSE, SCALE_NONE, 0, TME, 2, TME, SECINT);
	sql_create_func(sa, "sql_sub", "mtime", "time_sub_msec_interval", FALSE, FALSE, SCALE_NONE, 0, TMETZ, 2, TMETZ, SECINT);
	sql_create_func(sa, "sql_sub", "mtime", "timestamp_sub_msec_interval", FALSE, FALSE, SCALE_NONE, 0, TMESTAMP, 2, TMESTAMP, SECINT);
	sql_create_func(sa, "sql_sub", "mtime", "timestamp_sub_msec_interval", FALSE, FALSE, SCALE_NONE, 0, TMESTAMP, 2, TMESTAMP, DAYINT);
	sql_create_func(sa, "sql_sub", "mtime", "timestamp_sub_month_interval", FALSE, FALSE, SCALE_NONE, 0, TMESTAMP, 2, TMESTAMP, MONINT);
	sql_create_func(sa, "sql_sub", "mtime", "timestamp_sub_msec_interval", FALSE, FALSE, SCALE_NONE, 0, TMESTAMPTZ, 2, TMESTAMPTZ, SECINT);
	sql_create_func(sa, "sql_sub", "mtime", "timestamp_sub_msec_interval", FALSE, FALSE, SCALE_NONE, 0, TMESTAMPTZ, 2, TMESTAMPTZ, DAYINT);
	sql_create_func(sa, "sql_sub", "mtime", "timestamp_sub_month_interval", FALSE, FALSE, SCALE_NONE, 0, TMESTAMPTZ, 2, TMESTAMPTZ, MONINT);

	sql_create_func(sa, "sql_add", "mtime", "date_add_msec_interval", FALSE, FALSE, SCALE_NONE, 0, DTE, 2, DTE, SECINT);
	sql_create_func(sa, "sql_add", "mtime", "date_add_msec_interval", FALSE, FALSE, SCALE_NONE, 0, DTE, 2, DTE, DAYINT);
	sql_create_func(sa, "sql_add", "mtime", "addmonths", FALSE, FALSE, SCALE_NONE, 0, DTE, 2, DTE, MONINT);
	sql_create_func(sa, "sql_add", "mtime", "timestamp_add_msec_interval", FALSE, FALSE, SCALE_NONE, 0, TMESTAMP, 2, TMESTAMP, SECINT);
	sql_create_func(sa, "sql_add", "mtime", "timestamp_add_msec_interval", FALSE, FALSE, SCALE_NONE, 0, TMESTAMP, 2, TMESTAMP, DAYINT);
	sql_create_func(sa, "sql_add", "mtime", "timestamp_add_month_interval", FALSE, FALSE, SCALE_NONE, 0, TMESTAMP, 2, TMESTAMP, MONINT);
	sql_create_func(sa, "sql_add", "mtime", "timestamp_add_msec_interval", FALSE, FALSE, SCALE_NONE, 0, TMESTAMPTZ, 2, TMESTAMPTZ, SECINT);
	sql_create_func(sa, "sql_add", "mtime", "timestamp_add_msec_interval", FALSE, FALSE, SCALE_NONE, 0, TMESTAMPTZ, 2, TMESTAMPTZ, DAYINT);
	sql_create_func(sa, "sql_add", "mtime", "timestamp_add_month_interval", FALSE, FALSE, SCALE_NONE, 0, TMESTAMPTZ, 2, TMESTAMPTZ, MONINT);
	sql_create_func(sa, "sql_add", "mtime", "time_add_msec_interval", FALSE, FALSE, SCALE_NONE, 0, TME, 2, TME, SECINT);
	sql_create_func(sa, "sql_add", "mtime", "time_add_msec_interval", FALSE, FALSE, SCALE_NONE, 0, TMETZ, 2, TMETZ, SECINT);

	sql_create_func(sa, "local_timezone", "mtime", "local_timezone", FALSE, FALSE, SCALE_FIX, 0, SECINT, 0);

	sql_create_func(sa, "century", "mtime", "century", FALSE, FALSE, SCALE_FIX, 0, INT, 1, DTE);
	sql_create_func(sa, "decade", "mtime", "decade", FALSE, FALSE, SCALE_FIX, 0, INT, 1, DTE);
	sql_create_func(sa, "year", "mtime", "year", FALSE, FALSE, SCALE_FIX, 0, INT, 1, DTE);
	sql_create_func(sa, "quarter", "mtime", "quarter", FALSE, FALSE, SCALE_FIX, 0, INT, 1, DTE);
	sql_create_func(sa, "month", "mtime", "month", FALSE, FALSE, SCALE_FIX, 0, INT, 1, DTE);
	sql_create_func(sa, "day", "mtime", "day", FALSE, FALSE, SCALE_FIX, 0, INT, 1, DTE);
	sql_create_func(sa, "dayofyear", "mtime", "dayofyear", FALSE, FALSE, SCALE_FIX, 0, INT, 1, DTE);
	sql_create_func(sa, "weekofyear", "mtime", "weekofyear", FALSE, FALSE, SCALE_FIX, 0, INT, 1, DTE);
	sql_create_func(sa, "usweekofyear", "mtime", "usweekofyear", FALSE, FALSE, SCALE_FIX, 0, INT, 1, DTE);
	sql_create_func(sa, "dayofweek", "mtime", "dayofweek", FALSE, FALSE, SCALE_FIX, 0, INT, 1, DTE);
	sql_create_func(sa, "dayofmonth", "mtime", "day", FALSE, FALSE, SCALE_FIX, 0, INT, 1, DTE);
	sql_create_func(sa, "week", "mtime", "weekofyear", FALSE, FALSE, SCALE_FIX, 0, INT, 1, DTE);
	sql_create_func(sa, "epoch_ms", "mtime", "epoch_ms", FALSE, FALSE, SCALE_NONE, 0, LNG, 1, DTE);

	sql_create_func(sa, "hour", "mtime", "hours", FALSE, FALSE, SCALE_FIX, 0, INT, 1, TME);
	sql_create_func(sa, "minute", "mtime", "minutes", FALSE, FALSE, SCALE_FIX, 0, INT, 1, TME);
	sql_create_func(sa, "second", "mtime", "sql_seconds", FALSE, FALSE, SCALE_NONE, 6, DEC, 1, TME);
	sql_create_func(sa, "epoch_ms", "mtime", "epoch_ms", FALSE, FALSE, SCALE_NONE, 0, LNG, 1, TME);
	sql_create_func(sa, "hour", "mtime", "hours", FALSE, FALSE, SCALE_FIX, 0, INT, 1, TMETZ);
	sql_create_func(sa, "minute", "mtime", "minutes", FALSE, FALSE, SCALE_FIX, 0, INT, 1, TMETZ);
	sql_create_func(sa, "second", "mtime", "sql_seconds", FALSE, FALSE, SCALE_NONE, 6, DEC, 1, TMETZ);
	sql_create_func(sa, "epoch_ms", "mtime", "epoch_ms", FALSE, FALSE, SCALE_NONE, 0, LNG, 1, TMETZ);

	sql_create_func(sa, "century", "mtime", "century", FALSE, FALSE, SCALE_FIX, 0, INT, 1, TMESTAMP);
	sql_create_func(sa, "decade", "mtime", "decade", FALSE, FALSE, SCALE_FIX, 0, INT, 1, TMESTAMP);
	sql_create_func(sa, "year", "mtime", "year", FALSE, FALSE, SCALE_FIX, 0, INT, 1, TMESTAMP);
	sql_create_func(sa, "quarter", "mtime", "quarter", FALSE, FALSE, SCALE_FIX, 0, INT, 1, TMESTAMP);
	sql_create_func(sa, "month", "mtime", "month", FALSE, FALSE, SCALE_FIX, 0, INT, 1, TMESTAMP);
	sql_create_func(sa, "day", "mtime", "day", FALSE, FALSE, SCALE_FIX, 0, INT, 1, TMESTAMP);
	sql_create_func(sa, "hour", "mtime", "hours", FALSE, FALSE, SCALE_FIX, 0, INT, 1, TMESTAMP);
	sql_create_func(sa, "minute", "mtime", "minutes", FALSE, FALSE, SCALE_FIX, 0, INT, 1, TMESTAMP);
	sql_create_func(sa, "second", "mtime", "sql_seconds", FALSE, FALSE, SCALE_NONE, 6, DEC, 1, TMESTAMP);
	sql_create_func(sa, "epoch_ms", "mtime", "epoch_ms", FALSE, FALSE, SCALE_NONE, 0, LNG, 1, TMESTAMP);

	sql_create_func(sa, "century", "mtime", "century", FALSE, FALSE, SCALE_FIX, 0, INT, 1, TMESTAMPTZ);
	sql_create_func(sa, "decade", "mtime", "decade", FALSE, FALSE, SCALE_FIX, 0, INT, 1, TMESTAMPTZ);
	sql_create_func(sa, "year", "mtime", "year", FALSE, FALSE, SCALE_FIX, 0, INT, 1, TMESTAMPTZ);
	sql_create_func(sa, "quarter", "mtime", "quarter", FALSE, FALSE, SCALE_FIX, 0, INT, 1, TMESTAMPTZ);
	sql_create_func(sa, "month", "mtime", "month", FALSE, FALSE, SCALE_FIX, 0, INT, 1, TMESTAMPTZ);
	sql_create_func(sa, "day", "mtime", "day", FALSE, FALSE, SCALE_FIX, 0, INT, 1, TMESTAMPTZ);
	sql_create_func(sa, "hour", "mtime", "hours", FALSE, FALSE, SCALE_FIX, 0, INT, 1, TMESTAMPTZ);
	sql_create_func(sa, "minute", "mtime", "minutes", FALSE, FALSE, SCALE_FIX, 0, INT, 1, TMESTAMPTZ);
	sql_create_func(sa, "second", "mtime", "sql_seconds", FALSE, FALSE, SCALE_NONE, 6, DEC, 1, TMESTAMPTZ);
	sql_create_func(sa, "epoch_ms", "mtime", "epoch_ms", FALSE, FALSE, SCALE_NONE, 0, LNG, 1, TMESTAMPTZ);

	sql_create_func(sa, "year", "mtime", "year", FALSE, FALSE, SCALE_NONE, 0, INT, 1, MONINT);
	sql_create_func(sa, "month", "mtime", "month", FALSE, FALSE, SCALE_NONE, 0, INT, 1, MONINT);
	sql_create_func(sa, "day", "mtime", "day", FALSE, FALSE, SCALE_NONE, 0, LNG, 1, DAYINT);
	sql_create_func(sa, "hour", "mtime", "hours", FALSE, FALSE, SCALE_NONE, 0, INT, 1, DAYINT);
	sql_create_func(sa, "minute", "mtime", "minutes", FALSE, FALSE, SCALE_NONE, 0, INT, 1, DAYINT);
	sql_create_func(sa, "second", "mtime", "seconds", FALSE, FALSE, SCALE_NONE, 0, INT, 1, DAYINT);
	sql_create_func(sa, "epoch_ms", "mtime", "epoch_ms", FALSE, FALSE, SCALE_NONE, 0, LNG, 1, DAYINT);
	sql_create_func(sa, "day", "mtime", "day", FALSE, FALSE, SCALE_NONE, 0, LNG, 1, SECINT);
	sql_create_func(sa, "hour", "mtime", "hours", FALSE, FALSE, SCALE_NONE, 0, INT, 1, SECINT);
	sql_create_func(sa, "minute", "mtime", "minutes", FALSE, FALSE, SCALE_NONE, 0, INT, 1, SECINT);
	sql_create_func(sa, "second", "mtime", "seconds", FALSE, FALSE, SCALE_NONE, 0, INT, 1, SECINT);
	sql_create_func(sa, "epoch_ms", "mtime", "epoch_ms", FALSE, FALSE, SCALE_NONE, 0, LNG, 1, SECINT);

	for (t = strings; t < numerical; t++) {
		sql_create_func(sa, "next_value_for", "sql", "next_value", TRUE, TRUE, SCALE_NONE, 0, LNG, 2, *t, *t);
		sql_create_func(sa, "get_value_for", "sql", "get_value", TRUE, FALSE, SCALE_NONE, 0, LNG, 2, *t, *t);
		sql_create_func(sa, "restart", "sql", "restart", TRUE, TRUE, SCALE_NONE, 0, LNG, 3, *t, *t, LNG);

		sql_create_func(sa, "index", "sql", "index", TRUE, FALSE, SCALE_NONE, 0, BTE, 2, *t, BIT);
		sql_create_func(sa, "index", "sql", "index", TRUE, FALSE, SCALE_NONE, 0, SHT, 2, *t, BIT);
		sql_create_func(sa, "index", "sql", "index", TRUE, FALSE, SCALE_NONE, 0, INT, 2, *t, BIT);
		sql_create_func(sa, "strings", "sql", "strings", FALSE, FALSE, SCALE_NONE, 0, *t, 1, *t);

		sql_create_func(sa, "locate", "str", "locate", FALSE, FALSE, SCALE_NONE, 0, INT, 2, *t, *t);
		sql_create_func(sa, "locate", "str", "locate3", FALSE, FALSE, SCALE_NONE, 0, INT, 3, *t, *t, INT);
		sql_create_func(sa, "charindex", "str", "locate", FALSE, FALSE, SCALE_NONE, 0, INT, 2, *t, *t);
		sql_create_func(sa, "charindex", "str", "locate3", FALSE, FALSE, SCALE_NONE, 0, INT, 3, *t, *t, INT);
		sql_create_func(sa, "splitpart", "str", "splitpart", FALSE, FALSE, INOUT, 0, *t, 3, *t, *t, INT);
		sql_create_func(sa, "substring", "str", "substring", FALSE, FALSE, INOUT, 0, *t, 2, *t, INT);
		sql_create_func(sa, "substring", "str", "substring3", FALSE, FALSE, INOUT, 0, *t, 3, *t, INT, INT);
		sql_create_func(sa, "substr", "str", "substring", FALSE, FALSE, INOUT, 0, *t, 2, *t, INT);
		sql_create_func(sa, "substr", "str", "substring3", FALSE, FALSE, INOUT, 0, *t, 3, *t, INT, INT);

		sql_create_filter(sa, "like", "algebra", "like", FALSE, FALSE, SCALE_NONE, 0, 4, *t, *t, *t, BIT);
		sql_create_filter(sa, "not_like", "algebra", "not_like", FALSE, FALSE, SCALE_NONE, 0, 4, *t, *t, *t, BIT);

		sql_create_func(sa, "patindex", "pcre", "patindex", FALSE, FALSE, SCALE_NONE, 0, INT, 2, *t, *t);
		sql_create_func(sa, "truncate", "str", "stringleft", FALSE, FALSE, SCALE_NONE, 0, *t, 2, *t, INT);
		sql_create_func(sa, "concat", "calc", "+", FALSE, FALSE, DIGITS_ADD, 0, *t, 2, *t, *t);
		sql_create_func(sa, "ascii", "str", "ascii", TRUE, FALSE, SCALE_NONE, 0, INT, 1, *t); /* ascii of empty string is null */
		sql_create_func(sa, "code", "str", "unicode", FALSE, FALSE, SCALE_NONE, 0, *t, 1, INT);
		sql_create_func(sa, "length", "str", "length", FALSE, FALSE, SCALE_NONE, 0, INT, 1, *t);
		sql_create_func(sa, "right", "str", "stringright", FALSE, FALSE, SCALE_NONE, 0, *t, 2, *t, INT);
		sql_create_func(sa, "left", "str", "stringleft", FALSE, FALSE, SCALE_NONE, 0, *t, 2, *t, INT);
		sql_create_func(sa, "upper", "str", "toUpper", FALSE, FALSE, SCALE_NONE, 0, *t, 1, *t);
		sql_create_func(sa, "ucase", "str", "toUpper", FALSE, FALSE, SCALE_NONE, 0, *t, 1, *t);
		sql_create_func(sa, "lower", "str", "toLower", FALSE, FALSE, SCALE_NONE, 0, *t, 1, *t);
		sql_create_func(sa, "lcase", "str", "toLower", FALSE, FALSE, SCALE_NONE, 0, *t, 1, *t);
		sql_create_func(sa, "trim", "str", "trim", FALSE, FALSE, SCALE_NONE, 0, *t, 1, *t);
		sql_create_func(sa, "trim", "str", "trim2", FALSE, FALSE, SCALE_NONE, 0, *t, 2, *t, *t);
		sql_create_func(sa, "ltrim", "str", "ltrim", FALSE, FALSE, SCALE_NONE, 0, *t, 1, *t);
		sql_create_func(sa, "ltrim", "str", "ltrim2", FALSE, FALSE, SCALE_NONE, 0, *t, 2, *t, *t);
		sql_create_func(sa, "rtrim", "str", "rtrim", FALSE, FALSE, SCALE_NONE, 0, *t, 1, *t);
		sql_create_func(sa, "rtrim", "str", "rtrim2", FALSE, FALSE, SCALE_NONE, 0, *t, 2, *t, *t);

		sql_create_func(sa, "lpad", "str", "lpad", FALSE, FALSE, SCALE_NONE, 0, *t, 2, *t, INT);
		sql_create_func(sa, "lpad", "str", "lpad3", FALSE, FALSE, SCALE_NONE, 0, *t, 3, *t, INT, *t);
		sql_create_func(sa, "rpad", "str", "rpad", FALSE, FALSE, SCALE_NONE, 0, *t, 2, *t, INT);
		sql_create_func(sa, "rpad", "str", "rpad3", FALSE, FALSE, SCALE_NONE, 0, *t, 3, *t, INT, *t);

		sql_create_func(sa, "insert", "str", "insert", FALSE, FALSE, SCALE_NONE, 0, *t, 4, *t, INT, INT, *t);
		sql_create_func(sa, "replace", "str", "replace", FALSE, FALSE, SCALE_NONE, 0, *t, 3, *t, *t, *t);
		sql_create_func(sa, "repeat", "str", "repeat", TRUE, FALSE, SCALE_NONE, 0, *t, 2, *t, INT); /* repeat -1 times is null */
		sql_create_func(sa, "space", "str", "space", TRUE, FALSE, SCALE_NONE, 0, *t, 1, INT); /* space -1 times is null */
		sql_create_func(sa, "char_length", "str", "length", FALSE, FALSE, SCALE_NONE, 0, INT, 1, *t);
		sql_create_func(sa, "character_length", "str", "length", FALSE, FALSE, SCALE_NONE, 0, INT, 1, *t);
		sql_create_func(sa, "octet_length", "str", "nbytes", FALSE, FALSE, SCALE_NONE, 0, INT, 1, *t);

		sql_create_func(sa, "soundex", "txtsim", "soundex", FALSE, FALSE, SCALE_NONE, 0, *t, 1, *t);
		sql_create_func(sa, "difference", "txtsim", "stringdiff", TRUE, FALSE, SCALE_NONE, 0, INT, 2, *t, *t);
		sql_create_func(sa, "editdistance", "txtsim", "editdistance", TRUE, FALSE, SCALE_FIX, 0, INT, 2, *t, *t);
		sql_create_func(sa, "editdistance2", "txtsim", "editdistance2", TRUE, FALSE, SCALE_FIX, 0, INT, 2, *t, *t);

		sql_create_func(sa, "similarity", "txtsim", "similarity", TRUE, FALSE, SCALE_FIX, 0, DBL, 2, *t, *t);
		sql_create_func(sa, "qgramnormalize", "txtsim", "qgramnormalize", FALSE, FALSE, SCALE_NONE, 0, *t, 1, *t);

		sql_create_func(sa, "levenshtein", "txtsim", "levenshtein", TRUE, FALSE, SCALE_FIX, 0, INT, 2, *t, *t);
		sql_create_func(sa, "levenshtein", "txtsim", "levenshtein", TRUE, FALSE, SCALE_FIX, 0, INT, 5, *t, *t, INT, INT, INT);
	}
	/* copyfrom fname (arg 12) */
	f = sql_create_union(sa, "copyfrom", "sql", "copy_from", FALSE, SCALE_FIX, 0, TABLE, 12, PTR, STR, STR, STR, STR, STR, LNG, LNG, INT, STR, INT, INT);
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
	local_id = 1;
	aliases = sa_list(sa);
	types = sa_list(sa);
	localtypes = sa_list(sa);
	funcs = sa_list(sa);
	MT_lock_set(&funcs->ht_lock);
	funcs->ht = hash_new(sa, 1024, (fkeyvalue)&base_key);
	MT_lock_unset(&funcs->ht_lock);
	sqltypeinit( sa );
}
