/*
 * The contents of this file are subject to the MonetDB Public License
 * Version 1.1 (the "License"); you may not use this file except in
 * compliance with the License. You may obtain a copy of the License at
 * http://monetdb.cwi.nl/Legal/MonetDBLicense-1.1.html
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
 * Copyright August 2008-2011 MonetDB B.V.
 * All Rights Reserved.
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
#include <string.h>

#define END_SUBAGGR	1
#define END_AGGR	2
#define END_SUBTYPE	3
#define END_TYPE	4

list *aliases = NULL;
list *types = NULL;
list *aggrs = NULL;
list *funcs = NULL;

static list *localtypes = NULL;

static void sqltypeinit(void);

int digits2bits(int digits) 
{
	if (digits < 3) 
		return 8;
	else if (digits < 5) 
		return 16;
	else if (digits < 10) 
		return 32;
	else if (digits < 17) 
		return 51;
	return 64;
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
	else if (bits < 27) 
		return 8;
	else if (bits < 30) 
		return 9;
	else if (bits <= 32) 
		return 10;
	return 19;
}

/* 0 cannot convert */
/* 1 set operations have very limited coersion rules */
/* 2 automatic coersion (could still require dynamic checks for overflow) */
/* 3 casts are allowed (requires dynamic checks) (sofar not used) */
static int convert_matrix[EC_MAX][EC_MAX] = {

/* EC_ANY */	{ 0, 0, 0, 1, 1, 0, 0, 0, 0, 0, 0, 0, 0, 0 },
/* EC_TABLE */	{ 0, 0, 0, 1, 1, 0, 0, 0, 0, 0, 0, 0, 0, 0 },
/* EC_BIT */	{ 0, 0, 1, 1, 1, 0, 2, 2, 2, 0, 0, 0, 0, 0 },
/* EC_CHAR */	{ 2, 2, 2, 1, 1, 2, 2, 2, 2, 2, 2, 2, 2, 2 },
/* EC_STRING */	{ 2, 2, 2, 1, 1, 2, 2, 2, 2, 2, 2, 2, 2, 2 },
/* EC_BLOB */	{ 0, 0, 0, 0, 1, 1, 0, 0, 0, 0, 0, 0, 0, 0 },
/* EC_NUM */	{ 0, 0, 2, 1, 1, 0, 1, 1, 1, 1, 0, 0, 0, 0 },
/* EC_INTERVAL*/{ 0, 0, 0, 1, 1, 0, 1, 0, 0, 0, 1, 1, 1, 0 },
/* EC_DEC */	{ 0, 0, 0, 1, 1, 0, 1, 1, 1, 1, 0, 0, 0, 0 },
/* EC_FLT */	{ 0, 0, 0, 1, 1, 0, 1, 3, 1, 1, 0, 0, 0, 0 },
/* EC_TIME */	{ 0, 0, 0, 1, 1, 0, 0, 1, 0, 0, 1, 0, 0, 0 },
/* EC_DATE */	{ 0, 0, 0, 1, 1, 0, 0, 1, 0, 0, 0, 1, 3, 0 },
/* EC_TSTAMP */	{ 0, 0, 0, 1, 1, 0, 0, 0, 0, 0, 1, 1, 1, 0 },
/* EC_EXTERNAL*/{ 0, 0, 0, 1, 1, 0, 0, 0, 0, 0, 0, 0, 0, 0 }
};

int sql_type_convert (int from, int to) 
{
	int c = convert_matrix[from][to];
	return c;
}

int is_commutative(char *fnm)
{
	if (strcmp("sql_add", fnm) == 0 ||
	    strcmp("sql_mul", fnm) == 0)
	    	return 1;
	return 0;
}

void
base_init(sql_allocator *sa, sql_base * b, sqlid id, int flag, char *name)
{
	b->id = id;

	b->wtime = 0;
	b->rtime = 0;
	b->flag = flag;
	b->name = NULL;
	if (name)
		b->name = sa?sa_strdup(sa,name):_strdup(name);
}

void
base_set_name(sql_base * b, char *name)
{
	assert(name);
	if (b->name)
		_DELETE(b->name);
	b->name = _strdup(name);
}

void
base_destroy(sql_base * b)
{
	if (b->name)
		_DELETE(b->name);
}

void
sql_init_subtype(sql_subtype *res, sql_type *t, unsigned int digits, unsigned int scale)
{
	res->type = t;
	res->digits = digits ? digits : t->digits;
	if (t->digits && res->digits > t->digits)
		res->digits = t->digits;
	res->scale = scale;
	res->comp_type = NULL;
}

sql_subtype *
sql_create_subtype(sql_type *t, unsigned int digits, unsigned int scale)
{
	sql_subtype *res = ZNEW(sql_subtype);

	sql_init_subtype(res, t, digits, scale);
	return res;
}

int
localtypes_cmp(int nlt, int olt)
{
	if (nlt == TYPE_flt || nlt == TYPE_dbl) {
		nlt = TYPE_dbl;
	} else if (nlt == TYPE_bte || nlt == TYPE_sht || nlt == TYPE_int || nlt == TYPE_wrd || nlt == TYPE_lng) {
		nlt = TYPE_lng;
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
		localtype = TYPE_lng;
	}

	for (n = types->h; n; n = n->next) {
		sql_type *t = n->data;

		if (localtypes_cmp(t->localtype, localtype)) {
			if ((digits && t->digits >= digits) || (digits == t->digits)) {
				sql_init_subtype(r, t, digits, 0);
				return r;
			}
			for (m = n->next; m; m = m->next) {
				t = m->data;
				if (!localtypes_cmp(t->localtype, localtype)) {
					break;
				}
				n = m;
				if ((digits && t->digits >= digits) || (digits == t->digits)) {
					sql_init_subtype(r, t, digits, 0);
					return r;
				}
			}
		}
	}
	return NULL;
}

int 
sql_find_subtype(sql_subtype *res, char *name, unsigned int digits, unsigned int scale)
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
sql_bind_subtype(char *name, unsigned int digits, unsigned int scale)
{
	sql_subtype *res = ZNEW(sql_subtype);
	
	if (!sql_find_subtype(res, name, digits, scale)) {
		_DELETE(res);
		return NULL;
	}
	return res;
}

void
type_destroy(sql_type *t)
{
	base_destroy(&t->base);
	_DELETE(t->sqlname);
	_DELETE(t);
}

void
sql_subtype_destroy(sql_subtype *t)
{
	_DELETE(t);
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
	return _strdup(buf);
}

sql_subtype *
sql_bind_localtype(char *name)
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

sql_type *
sql_bind_type(char *name)
{
	node *n = types->h;

	while (n) {
		sql_type *t = n->data;

		if (strcmp(t->base.name, name) == 0) {
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
	       t1->type->eclass == EC_INTERVAL) &&
	      (t1->digits != t2->digits || t1->scale != t2->scale) )
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
	return _strdup(buf);
}

int 
subaggr_cmp( sql_subaggr *a1, sql_subaggr *a2)
{
	if (a1->aggr == a2->aggr) 
	    return subtype_cmp(&a1->res, &a2->res);
	return -1;
}

int 
subfunc_cmp( sql_subfunc *f1, sql_subfunc *f2)
{
	if (f1->func == f2->func) 
	    return subtype_cmp(&f1->res, &f2->res);
	return -1;
}

sql_subaggr *
sql_bind_aggr(sql_allocator *sa, sql_schema *s, char *sqlaname, sql_subtype *type)
{
	node *n = aggrs->h;

	(void)s;
	while (n) {
		sql_func *a = n->data;
		sql_arg *arg = NULL;

		if (a->ops->h)
			arg = a->ops->h->data;

		if (strcmp(a->base.name, sqlaname) == 0 && (!arg ||
		    arg->type.type->eclass == EC_ANY || 
		    (type && is_subtype(type, &arg->type )))) {
			int scale = 0;
			int digits = 0;
			sql_subaggr *ares = SA_ZNEW(sa, sql_subaggr);

			ares->aggr = a;
			digits = a->res.digits;
			scale = a->res.scale;
			/* same scale as the input */
			if (type) {
				digits = type->digits;
				scale = type->scale;
			}
			/* same type as the input */
			if (a->res.type->eclass == EC_ANY) 
				sql_init_subtype(&ares->res, type->type, digits, scale);
			else
				sql_init_subtype(&ares->res, a->res.type, digits, scale);
			return ares;
		}
		n = n->next;
	}
	if (s) {
		if (s->funcs.set) for (n=s->funcs.set->h; n; n = n->next) {
			sql_func *a = n->data;
			sql_arg *arg = NULL;

			if ((a->is_func && !a->res.type) || !a->aggr)
				continue;

			if (a->ops->h)
				arg = a->ops->h->data;

			if (strcmp(a->base.name, sqlaname) == 0 && (!arg ||
		    	 	arg->type.type->eclass == EC_ANY || 
		    		(type && is_subtype(type, &arg->type )))) {
				int scale = 0;
				int digits = 0;
				sql_subaggr *ares = SA_ZNEW(sa, sql_subaggr);
		
				ares->aggr = a;
				digits = a->res.digits;
				scale = a->res.scale;
				/* same scale as the input */
				if (type) {
					digits = type->digits;
					scale = type->scale;
				}
				/* same type as the input */
				if (a->res.type->eclass == EC_ANY) 
					sql_init_subtype(&ares->res, type->type, digits, scale);
				else
					sql_init_subtype(&ares->res, a->res.type, digits, scale);
				return ares;
			}
		}
	}
	return NULL;
}

sql_subaggr *
sql_find_aggr(sql_allocator *sa, sql_schema *s, char *sqlaname)
{
	node *n = aggrs->h;

	(void)s;
	while (n) {
		sql_func *a = n->data;

		if (strcmp(a->base.name, sqlaname) == 0) {
			int scale = 0;
			int digits = 0;
			sql_subaggr *ares = SA_ZNEW(sa, sql_subaggr);

			ares->aggr = a;
			digits = a->res.digits;
			scale = a->res.scale;
			sql_init_subtype(&ares->res, a->res.type, digits, scale);
			return ares;
		}
		n = n->next;
	}
	if (s) {
		if (s->funcs.set) for (n=s->funcs.set->h; n; n = n->next) {
			sql_func *a = n->data;

			if ((a->is_func && !a->res.type) || !a->aggr)
				continue;

			if (strcmp(a->base.name, sqlaname) == 0) {
				int scale = 0;
				int digits = 0;
				sql_subaggr *ares = SA_ZNEW(sa, sql_subaggr);
		
				ares->aggr = a;
				digits = a->res.digits;
				scale = a->res.scale;
				sql_init_subtype(&ares->res, a->res.type, digits, scale);
				return ares;
			}
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

static sql_subfunc *
func_cmp(sql_allocator *sa, sql_func *f, char *name, int nrargs) 
{
	if (strcmp(f->base.name, name) == 0) {
		if (list_length(f->ops) == nrargs) {
			sql_subfunc *fres = SA_ZNEW(sa, sql_subfunc);

			fres->func = f;
			if (f->res.type)
				sql_init_subtype(&fres->res, f->res.type, f->res.digits, f->res.scale);
			if (f->res.comp_type) 
				fres->res.comp_type = f->res.comp_type;
			return fres;
		}
	}
	return NULL;
}

sql_subfunc *
sql_find_func(sql_allocator *sa, sql_schema *s, char *sqlfname, int nrargs)
{
	node *n = funcs->h;
	sql_subfunc *fres;

	assert(nrargs);
	for (; n; n = n->next) {
		sql_func *f = n->data;

		if (!f->res.type)
			continue;
		if ((fres = func_cmp(sa, f, sqlfname, nrargs )) != NULL) {
			return fres;
		}
	}
	if (s) {
		if (s->funcs.set) for (n=s->funcs.set->h; n; n = n->next) {
			sql_func *f = n->data;

			if (f->aggr)
				continue;
			if ((fres = func_cmp(sa, f, sqlfname, nrargs )) != NULL) {
				return fres;
			}
		}
	}
	return NULL;
}


/* find function based on first argument */
sql_subfunc *
sql_bind_member(sql_allocator *sa, sql_schema *s, char *sqlfname, sql_subtype *tp, int nrargs)
{
	node *n;

	(void)s;
	assert(nrargs);
	for (n = funcs->h; n; n = n->next) {
		sql_func *f = n->data;

		if (!f->res.type)
			continue;
		if (strcmp(f->base.name, sqlfname) == 0) {
			if (list_length(f->ops) == nrargs && is_subtype(tp, &((sql_arg *) f->ops->h->data)->type)) {

				unsigned int scale = 0, digits;
				sql_subfunc *fres = SA_ZNEW(sa, sql_subfunc);

				fres->func = f;
				/* same scale as the input */
				if (tp && tp->scale > scale)
					scale = tp->scale;
				digits = f->res.digits;
				if (tp && f->fix_scale == INOUT)
					digits = tp->digits;
				sql_init_subtype(&fres->res, f->res.type, digits, scale);
				return fres;
			}
		}
	}
	return NULL;
}

sql_subfunc *
sql_bind_func(sql_allocator *sa, sql_schema *s, char *sqlfname, sql_subtype *tp1, sql_subtype *tp2)
{
	list *l = list_create((fdestroy)NULL);
	sql_subfunc *fres;

	if (tp1)
		list_append(l, tp1);
	if (tp2)
		list_append(l, tp2);

	fres = sql_bind_func_(sa, s, sqlfname, l);
	list_destroy(l);
	return fres;
}

sql_subfunc *
sql_bind_func3(sql_allocator *sa, sql_schema *s, char *sqlfname, sql_subtype *tp1, sql_subtype *tp2, sql_subtype *tp3)
{
	list *l = list_create((fdestroy)NULL);
	sql_subfunc *fres;

	if (tp1)
		list_append(l, tp1);
	if (tp2)
		list_append(l, tp2);
	if (tp3)
		list_append(l, tp3);

	fres = sql_bind_func_(sa, s, sqlfname, l);
	list_destroy(l);
	return fres;
}

int
arg_subtype_cmp(sql_arg *a, sql_subtype *t)
{
	if (a->type.type->eclass == EC_ANY)
		return 0;
	return (is_subtype(t, &a->type )?0:-1);
}

sql_subfunc *
sql_bind_func_(sql_allocator *sa, sql_schema *s, char *sqlfname, list *ops)
{
	node *n = funcs->h;

	(void)s;
	while (n) {
		sql_func *f = n->data;

		if (f->is_func && !f->res.type)
			continue;
		if (strcmp(f->base.name, sqlfname) == 0) {
			if (list_cmp(f->ops, ops, (fcmp) &arg_subtype_cmp) == 0) {
				unsigned int scale = 0, digits;
				sql_subfunc *fres = SA_ZNEW(sa, sql_subfunc);

				fres->func = f;
				/* fix the scale */
				digits = f->res.digits;
				if (f->fix_scale > SCALE_NONE) {
					for (n = ops->h; n; n = n->next) {
						sql_subtype *a = n->data;

						/* same scale as the input */
						if (a && a->scale > scale)
							scale = a->scale;
						if (a && f->fix_scale == INOUT)
							digits = a->digits;
					}
				} else if (f->res.scale) 
					scale = f->res.scale;
				/* same type as the first input */
				if (f->res.type->eclass == EC_ANY) {
					node *m;
					sql_subtype *a = NULL;
					for (n = ops->h, m = f->ops->h; n; n = n->next, m = m->next) {
						sql_arg *sa = m->data;
						if (sa->type.type->eclass == EC_ANY) {
							a = n->data;
						}
					}
					sql_init_subtype(&fres->res, a->type, digits, scale);
				} else {
					sql_init_subtype(&fres->res, f->res.type, digits, scale);
				}
				return fres;
			}
		}
		n = n->next;
	}
	if (s) {
		node *n;

		if (s->funcs.set) for (n=s->funcs.set->h; n; n = n->next) {
			sql_func *f = n->data;

			if (f->is_func && !f->res.type)
				continue;
			if (strcmp(f->base.name, sqlfname) == 0) {
				if (list_cmp(f->ops, ops, (fcmp) &arg_subtype_cmp) == 0) {
					unsigned int scale = 0;
					sql_subfunc *fres = SA_ZNEW(sa, sql_subfunc);

					fres->func = f;
					for (n = ops->h; n; n = n->next) {
						sql_subtype *a = n->data;

						/* same scale as the input */
						if (a && a->scale > scale)
							scale = a->scale;
					}
					if (f->is_func) {
						sql_init_subtype(&fres->res, f->res.type, f->res.digits, scale);
						if (f->res.comp_type) 
							fres->res.comp_type = f->res.comp_type;
					}
					return fres;
				}
			}
		}
	}
	return NULL;
}

sql_subfunc *
sql_bind_func_result(sql_allocator *sa, sql_schema *s, char *sqlfname, sql_subtype *tp1, sql_subtype *tp2, sql_subtype *res)
{
	list *l = list_create((fdestroy) NULL);
	sql_subfunc *fres;

	if (tp1)
		list_append(l, tp1);
	if (tp2)
		list_append(l, tp2);

	fres = sql_bind_func_result_(sa, s, sqlfname, l, res);
	list_destroy(l);
	return fres;
}

sql_subfunc *
sql_bind_func_result3(sql_allocator *sa, sql_schema *s, char *sqlfname, sql_subtype *tp1, sql_subtype *tp2, sql_subtype *tp3, sql_subtype *res)
{
	list *l = list_create((fdestroy) NULL);
	sql_subfunc *fres;

	if (tp1)
		list_append(l, tp1);
	if (tp2)
		list_append(l, tp2);
	if (tp3)
		list_append(l, tp3);

	fres = sql_bind_func_result_(sa, s, sqlfname, l, res);
	list_destroy(l);
	return fres;
}


sql_subfunc *
sql_bind_func_result_(sql_allocator *sa, sql_schema *s, char *sqlfname, list *ops, sql_subtype *res)
{
	node *n = funcs->h;

	(void)s;
	while (n) {
		sql_func *f = n->data;

		if (!f->res.type)
			continue;
		if (strcmp(f->base.name, sqlfname) == 0 && (is_subtype(&f->res, res) || f->res.type->eclass == EC_ANY) && list_cmp(f->ops, ops, (fcmp) &arg_subtype_cmp) == 0) {
			unsigned int scale = 0;
			sql_subfunc *fres = SA_ZNEW(sa, sql_subfunc);

			fres->func = f;
			for (n = ops->h; n; n = n->next) {
				sql_subtype *a = n->data;

				/* same scale as the input */
				if (a && a->scale > scale)
					scale = a->scale;
			}
			/* same type as the first input */
			if (f->res.type->eclass == EC_ANY) {
				node *m;
				sql_subtype *a = NULL;
				for (n = ops->h, m = f->ops->h; n; n = n->next, m = m->next) {
					sql_arg *s = m->data;
					if (s->type.type->eclass == EC_ANY) {
						a = n->data;
					}
				}
				sql_init_subtype(&fres->res, a->type, f->res.digits, scale);
			} else {
				sql_init_subtype(&fres->res, f->res.type, f->res.digits, scale);
				if (f->res.comp_type) 
					fres->res.comp_type = f->res.comp_type;
			}
			return fres;
		}
		n = n->next;
	}
	return NULL;
}

sql_subfunc *
sql_bind_proc(sql_allocator *sa, sql_schema *s, char *sqlfname, list *ops)
{
	node *n = funcs->h;

	(void)s;
	while (n) {
		sql_func *f = n->data;

		if (f->res.type)
			continue;
		if (strcmp(f->base.name, sqlfname) == 0 && list_cmp(f->ops, ops, (fcmp) &arg_subtype_cmp) == 0) {
			sql_subfunc *fres = SA_ZNEW(sa, sql_subfunc);

			fres->func = f;
			fres->res.type = NULL;
			return fres;
		}
		n = n->next;
	}
	return NULL;
}

void
func_destroy(sql_func *t)
{
	/* ugh */
	if (t->res.comp_type)
		return;
	base_destroy(&t->base);
	if (t->imp)
		_DELETE(t->imp);
	if (t->mod)
		_DELETE(t->mod);
	if (t->ops)
		list_destroy(t->ops);
	if (t->query)
		_DELETE(t->query);
	_DELETE(t);
}

void
sql_create_alias(char *name, char *alias)
{
	sql_alias *a = ZNEW(sql_alias);

	a->name = _strdup(name);
	a->alias = _strdup(alias);
	list_append(aliases, a);
	if (!keyword_exists(a->alias) )
		keywords_insert(a->alias, KW_ALIAS);
}

void
alias_destroy(sql_alias * a)
{
	_DELETE(a->name);
	_DELETE(a->alias);
	_DELETE(a);
}

char *
sql_bind_alias(char *alias)
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
sql_create_type(char *sqlname, unsigned int digits, unsigned int scale, unsigned char radix, unsigned char eclass, char *name)
{
	sql_type *t = ZNEW(sql_type);

	base_init(NULL, &t->base, store_next_oid(), TR_OLD, name);
	t->sqlname = _strdup(sqlname);
	t->digits = digits;
	t->scale = scale;
	t->localtype = ATOMindex(t->base.name);
	t->radix = radix;
	t->eclass = eclass;
	t->s = NULL;
	if (!keyword_exists(t->sqlname) )
		keywords_insert(t->sqlname, KW_TYPE);
	list_append(types, t);

	list_append(localtypes, sql_create_subtype(t, 0, 0));

	return t;
}

static sql_arg *
create_arg(char *name, sql_subtype *t)
{
	sql_arg *a = ZNEW(sql_arg);

	a->name = name;
	a->type = *t;
	sql_subtype_destroy(t);
	return a;
}

sql_arg *
arg_dup(sql_arg *oa)
{
	sql_arg *a = ZNEW(sql_arg);

	a->name = _strdup(oa->name);
	a->type = oa->type;
	return a;
}

void
arg_destroy(sql_arg *a)
{
	if (a->name)
		_DELETE(a->name);
	_DELETE(a);
}

sql_func *
sql_create_aggr(char *name, char *mod, char *imp, sql_type *tpe, sql_type *res)
{
	list *l = list_create((fdestroy) &arg_destroy);
	sql_subtype sres;

	if (tpe)
		list_append(l, create_arg(NULL, sql_create_subtype(tpe, 0, 0)));
	assert(res);
	sql_init_subtype(&sres, res, 0, 0);
	return sql_create_func_(name, mod, imp, l, &sres, FALSE, TRUE, SCALE_NONE);
}

sql_func *
sql_create_func(char *name, char *mod, char *imp, sql_type *tpe1, sql_type *tpe2, sql_type *res, int fix_scale)
{
	list *l = list_create((fdestroy) &arg_destroy);
	sql_subtype sres;

	if (tpe1)
		list_append(l,create_arg(NULL, sql_create_subtype(tpe1, 0, 0)));
	if (tpe2)
		list_append(l,create_arg(NULL, sql_create_subtype(tpe2, 0, 0)));

	sql_init_subtype(&sres, res, 0, 0);
	return sql_create_func_(name, mod, imp, l, &sres, FALSE, FALSE, fix_scale);
}

sql_func *
sql_create_funcSE(char *name, char *mod, char *imp, sql_type *tpe1, sql_type *tpe2, sql_type *res, int fix_scale)
{
	list *l = list_create((fdestroy) &arg_destroy);
	sql_subtype sres;

	if (tpe1)
		list_append(l,create_arg(NULL, sql_create_subtype(tpe1, 0, 0)));
	if (tpe2)
		list_append(l,create_arg(NULL, sql_create_subtype(tpe2, 0, 0)));

	sql_init_subtype(&sres, res, 0, 0);
	return sql_create_func_(name, mod, imp, l, &sres, TRUE, FALSE, fix_scale);
}


sql_func *
sql_create_func3(char *name, char *mod, char *imp, sql_type *tpe1, sql_type *tpe2, sql_type *tpe3, sql_type *res, int fix_scale)
{
	list *l = list_create((fdestroy) &arg_destroy);
	sql_subtype sres;

	list_append(l, create_arg(NULL, sql_create_subtype(tpe1, 0, 0)));
	list_append(l, create_arg(NULL, sql_create_subtype(tpe2, 0, 0)));
	list_append(l, create_arg(NULL, sql_create_subtype(tpe3, 0, 0)));

	sql_init_subtype(&sres, res, 0, 0);
	return sql_create_func_(name, mod, imp, l, &sres, FALSE, FALSE, fix_scale);
}

sql_func *
sql_create_func4(char *name, char *mod, char *imp, sql_type *tpe1, sql_type *tpe2, sql_type *tpe3, sql_type *tpe4, sql_type *res, int fix_scale)
{
	list *l = list_create((fdestroy) &arg_destroy);
	sql_subtype sres;

	list_append(l, create_arg(NULL, sql_create_subtype(tpe1, 0, 0)));
	list_append(l, create_arg(NULL, sql_create_subtype(tpe2, 0, 0)));
	list_append(l, create_arg(NULL, sql_create_subtype(tpe3, 0, 0)));
	list_append(l, create_arg(NULL, sql_create_subtype(tpe4, 0, 0)));

	sql_init_subtype(&sres, res, 0, 0);
	return sql_create_func_(name, mod, imp, l, &sres, FALSE, FALSE, fix_scale);
}


sql_func *
sql_create_func_(char *name, char *mod, char *imp, list *ops, sql_subtype *res, bit side_effect, bit aggr, int fix_scale)
{
	sql_func *t = ZNEW(sql_func);

	assert(res && ops);
	base_init(NULL, &t->base, store_next_oid(), TR_OLD, name);
	t->imp = _strdup(imp);
	t->mod = _strdup(mod);
	t->ops = ops;
	if (res) {	
		t->res = *res;
		t->is_func = 1;
	} else {
		t->res.type = NULL;
		t->is_func = 0;
	}
	t->nr = list_length(funcs);
	t->sql = 0;
	t->aggr = aggr;
	t->side_effect = side_effect;
	t->fix_scale = fix_scale;
	t->s = NULL;
	if (aggr)
		list_append(aggrs, t);
	else
		list_append(funcs, t);
	return t;
}

sql_func *
sql_create_sqlfunc(char *name, char *imp, list *ops, sql_subtype *res)
{
	sql_func *t = ZNEW(sql_func);

	assert(res && ops);
	base_init(NULL, &t->base, store_next_oid(), TR_OLD, name);
	t->imp = _strdup(imp);
	t->mod = _strdup("SQL");
	t->ops = ops;
	if (res) {	
		t->res = *res;
		t->is_func = 1;
	} else {
		t->res.type = NULL;
		t->is_func = 0;
	}
	t->nr = list_length(funcs);
	t->sql = 1;
	t->aggr = FALSE;
	t->side_effect = FALSE;
	list_append(funcs, t);
	return t;
}

void
types_init(int debug)
{
	(void)debug;
	aliases = list_create((fdestroy) &alias_destroy);
	types = list_create((fdestroy) &type_destroy);
	localtypes = list_create((fdestroy) &sql_subtype_destroy);
	aggrs = list_create((fdestroy) &func_destroy);
	funcs = list_create((fdestroy) &func_destroy);
	sqltypeinit();
}

/* SQL service initialization
This C-code version initializes the
parser catalogs with typing information. Although, in principle,
many of the function signatures can be obtained from the underlying
database kernel, we have chosen for this explicit scheme for one
simple reason. The SQL standard dictates the types and we have to
check their availability in the kernel only. The kernel itself could
include manyfunctions for which their is no standard.
lead to unexpected
*/

void
sqltypeinit(void)
{
	sql_type *ts[100];
	sql_type **strings, **numerical;
	sql_type **decimals, **floats, **dates, **end, **t;
	sql_type *STR, *BTE, *SHT, *INT, *LNG, *OID, *BIT, *DBL, *WRD, *DEC;
	sql_type *SECINT, *MONINT, *DTE; 
	sql_type *TME, *TMETZ, *TMESTAMP, *TMESTAMPTZ;
	sql_type *ANY, *TABLE;
	sql_func *f;

	ANY = sql_create_type("ANY", 0, 0, 0, EC_ANY, "any");

	t = ts;
	TABLE = *t++ = sql_create_type("TABLE", 0, 0, 0, EC_TABLE, "bat");
	*t++ = sql_create_type("PTR", 0, 0, 0, EC_TABLE, "ptr");

	BIT = *t++ = sql_create_type("BOOLEAN", 1, 0, 2, EC_BIT, "bit");
	sql_create_alias(BIT->sqlname, "BOOL");

	strings = t;
	*t++ = sql_create_type("CHAR",    0, 0, 0, EC_CHAR,   "str");
	STR = *t++ = sql_create_type("VARCHAR", 0, 0, 0, EC_STRING, "str");
	*t++ = sql_create_type("CLOB",    0, 0, 0, EC_STRING, "str");

	numerical = t;

	BTE = *t++ = sql_create_type("TINYINT",   8, SCALE_FIX, 2, EC_NUM, "bte");
	SHT = *t++ = sql_create_type("SMALLINT", 16, SCALE_FIX, 2, EC_NUM, "sht");
	OID = *t++ = sql_create_type("OID", 31, 0, 2, EC_NUM, "oid");
	INT = *t++ = sql_create_type("INT",      32, SCALE_FIX, 2, EC_NUM, "int");
	if (sizeof(wrd) == sizeof(int))
		WRD = *t++ = sql_create_type("WRD", 32, SCALE_FIX, 2, EC_NUM, "wrd");
	LNG = *t++ = sql_create_type("BIGINT",   64, SCALE_FIX, 2, EC_NUM, "lng");
	if (sizeof(wrd) == sizeof(lng))
		WRD = *t++ = sql_create_type("WRD", 64, SCALE_FIX, 2, EC_NUM, "wrd");

	decimals = t;
	/* decimal(d,s) (d indicates nr digits,
	   s scale indicates nr of digits after the dot .) */
	*t++ = sql_create_type("DECIMAL",  2, SCALE_FIX, 10, EC_DEC, "bte");
	*t++ = sql_create_type("DECIMAL",  4, SCALE_FIX, 10, EC_DEC, "sht");
	DEC = *t++ = sql_create_type("DECIMAL",  9, SCALE_FIX, 10, EC_DEC, "int");
	*t++ = sql_create_type("DECIMAL", 19, SCALE_FIX, 10, EC_DEC, "lng");

	/* float(n) (n indicates precision of atleast n digits) */
	/* ie n <= 23 -> flt */
	/*    n <= 51 -> dbl */
	/*    n <= 62 -> long long dbl (with -ieee) (not supported) */
	/* this requires a type definition */

	floats = t;
	*t++ = sql_create_type("REAL", 24, SCALE_NOFIX, 2, EC_FLT, "flt");
	DBL = *t++ = sql_create_type("DOUBLE", 53, SCALE_NOFIX, 2, EC_FLT, "dbl");

	dates = t;
	MONINT = *t++ = sql_create_type("MONTH_INTERVAL", 32, 0, 2, EC_INTERVAL, "int");
	SECINT = *t++ = sql_create_type("SEC_INTERVAL", 19, SCALE_FIX, 10, EC_INTERVAL, "lng");
	TME = *t++ = sql_create_type("TIME", 7, 0, 0, EC_TIME, "daytime");
	TMETZ = *t++ = sql_create_type("TIMETZ", 7, SCALE_FIX, 0, EC_TIME, "daytime");
	DTE = *t++ = sql_create_type("DATE", 0, 0, 0, EC_DATE, "date");
	TMESTAMP = *t++ = sql_create_type("TIMESTAMP", 7, 0, 0, EC_TIMESTAMP, "timestamp");
	TMESTAMPTZ = *t++ = sql_create_type("TIMESTAMPTZ", 7, SCALE_FIX, 0, EC_TIMESTAMP, "timestamp");

	*t++ = sql_create_type("BLOB", 0, 0, 0, EC_BLOB, "sqlblob");
	end = t;
	*t = NULL;

	sql_create_aggr("not_unique", "sql", "not_unique", OID, BIT);
	/* well to be precise it does reduce and map */
	sql_create_func("not_uniques", "sql", "not_uniques", WRD, NULL, OID, SCALE_NONE);
	sql_create_func("not_uniques", "sql", "not_uniques", OID, NULL, OID, SCALE_NONE);

	/* functions needed for all types */
	sql_create_func("hash", "calc", "hash", ANY, NULL, WRD, SCALE_FIX);
	sql_create_func3("rotate_xor_hash", "calc", "rotate_xor_hash", WRD, INT, ANY, WRD, SCALE_NONE);
	sql_create_func("=", "calc", "=", ANY, ANY, BIT, SCALE_FIX);
	sql_create_func("<>", "calc", "!=", ANY, ANY, BIT, SCALE_FIX);
	sql_create_func("isnull", "calc", "isnil", ANY, NULL, BIT, SCALE_FIX);
	sql_create_func(">", "calc", ">", ANY, ANY, BIT, SCALE_FIX);
	sql_create_func(">=", "calc", ">=", ANY, ANY, BIT, SCALE_FIX);
	sql_create_func("<", "calc", "<", ANY, ANY, BIT, SCALE_FIX);
	sql_create_func("<=", "calc", "<=", ANY, ANY, BIT, SCALE_FIX);
	sql_create_aggr("zero_or_one", "sql", "zero_or_one", ANY, ANY);
	sql_create_aggr("exist", "aggr", "exist", ANY, BIT);
	sql_create_aggr("not_exist", "aggr", "not_exist", ANY, BIT);
	/* needed for relational version */
	sql_create_func("in", "calc", "in", ANY, ANY, BIT, SCALE_NONE);
	sql_create_func("identity", "batcalc", "identity", ANY, NULL, OID, SCALE_NONE);
	/* needed for indices/clusters oid(schema.table,val) returns max(head(schema.table))+1 */
	sql_create_func3("rowid", "calc", "rowid", ANY, STR, STR, OID, SCALE_NONE);
	sql_create_aggr("min", "aggr", "min", ANY, ANY);
	sql_create_aggr("max", "aggr", "max", ANY, ANY);
	sql_create_func("sql_min", "calc", "min", ANY, ANY, ANY, SCALE_FIX);
	sql_create_func("sql_max", "calc", "max", ANY, ANY, ANY, SCALE_FIX);
	sql_create_func3("ifthenelse", "calc", "ifthenelse", BIT, ANY, ANY, ANY, SCALE_FIX);

	/* sum for numerical and decimals */
	sql_create_aggr("sum", "aggr", "sum", BTE, LNG);
	sql_create_aggr("sum", "aggr", "sum", SHT, LNG);
	sql_create_aggr("sum", "aggr", "sum", INT, LNG);
	sql_create_aggr("sum", "aggr", "sum", LNG, LNG);
	sql_create_aggr("sum", "aggr", "sum", WRD, WRD);

	t = decimals; /* BTE */
	sql_create_aggr("sum", "aggr", "sum", *(t), *(t+3));
	t++; /* SHT */
	sql_create_aggr("sum", "aggr", "sum", *(t), *(t+2));
	t++; /* INT */
	sql_create_aggr("sum", "aggr", "sum", *(t), *(t+1));
	t++; /* LNG */
	sql_create_aggr("sum", "aggr", "sum", *(t), *(t));

	/* prod for numerical and decimals */
	sql_create_aggr("prod", "aggr", "product", BTE, LNG);
	sql_create_aggr("prod", "aggr", "product", SHT, LNG);
	sql_create_aggr("prod", "aggr", "product", INT, LNG);
	sql_create_aggr("prod", "aggr", "product", LNG, LNG);
	/*sql_create_aggr("prod", "aggr", "product", WRD, WRD);*/

	t = decimals; /* BTE */
	sql_create_aggr("prod", "aggr", "product", *(t), *(t+3));
	t++; /* SHT */
	sql_create_aggr("prod", "aggr", "product", *(t), *(t+2));
	t++; /* INT */
	sql_create_aggr("prod", "aggr", "product", *(t), *(t+1));
	t++; /* LNG */
	sql_create_aggr("prod", "aggr", "product", *(t), *(t));

	for (t = numerical; t < floats; t++) 
		sql_create_func("mod", "calc", "%", *t, *t, *t, SCALE_FIX);

	for (t = floats; t < dates; t++) {
		sql_create_aggr("sum", "aggr", "sum", *t, *t);
		sql_create_aggr("prod", "aggr", "product", *t, *t);
		sql_create_func("mod", "calc", "fmod", *t, *t, *t, SCALE_FIX);
	}
	/*
	sql_create_aggr("avg", "aggr", "avg", BTE, DBL);
	sql_create_aggr("avg", "aggr", "avg", SHT, DBL);
	sql_create_aggr("avg", "aggr", "avg", INT, DBL);
	sql_create_aggr("avg", "aggr", "avg", LNG, DBL);
	*/
	sql_create_aggr("avg", "aggr", "avg", DBL, DBL);

	sql_create_aggr("count_no_nil", "aggr", "count_no_nil", NULL, WRD);
	sql_create_aggr("count", "aggr", "count", NULL, WRD);

	sql_create_func("rank", "calc", "rank_grp", ANY, NULL, INT, SCALE_NONE);
	sql_create_func("dense_rank", "calc", "dense_rank_grp", ANY, NULL, INT, SCALE_NONE);
	sql_create_func("percent_rank", "calc", "precent_rank_grp", ANY, NULL, INT, SCALE_NONE);
	sql_create_func("cume_dist", "calc", "cume_dist_grp", ANY, NULL, ANY, SCALE_NONE);
	sql_create_func("row_number", "calc", "mark_grp", ANY, NULL, INT, SCALE_NONE);

	sql_create_func3("rank", "calc", "rank_grp", ANY, OID, OID, INT, SCALE_NONE);
	sql_create_func3("dense_rank", "calc", "dense_rank_grp", ANY, OID, OID, INT, SCALE_NONE);
	sql_create_func3("percent_rank", "calc", "precent_rank_grp", ANY, OID, OID, INT, SCALE_NONE);
	sql_create_func3("cume_dist", "calc", "cume_dist_grp", ANY, OID, OID, ANY, SCALE_NONE);
	sql_create_func3("row_number", "calc", "mark_grp", ANY, OID, OID, INT, SCALE_NONE);

	sql_create_func("and", "calc", "and", BIT, BIT, BIT, SCALE_FIX);
	sql_create_func("or",  "calc",  "or", BIT, BIT, BIT, SCALE_FIX);
	sql_create_func("xor", "calc", "xor", BIT, BIT, BIT, SCALE_FIX);
	sql_create_func("not", "calc", "not", BIT, NULL,BIT, SCALE_FIX);

	/* all numericals */
	for (t = numerical; *t != TME; t++) {
		sql_subtype *lt = sql_bind_localtype((*t)->base.name);

		sql_create_func("sql_sub", "calc", "-", *t, *t, *t, SCALE_FIX);
		sql_create_func("sql_add", "calc", "+", *t, *t, *t, SCALE_FIX);
		sql_create_func("sql_mul", "calc", "*", *t, *t, *t, SCALE_MUL);
		sql_create_func("sql_div", "calc", "/", *t, *t, *t, SCALE_DIV);
		sql_create_func("bit_and", "calc", "and", *t, *t, *t, SCALE_FIX);
		sql_create_func("bit_or", "calc", "or", *t, *t, *t, SCALE_FIX);
		sql_create_func("bit_xor", "calc", "xor", *t, *t, *t, SCALE_FIX);
		sql_create_func("bit_not", "calc", "not", *t, NULL, *t, SCALE_FIX);
		if (t < floats) {
			sql_create_func("left_shift", "calc", "<<", *t, INT, *t, SCALE_FIX);
			sql_create_func("right_shift", "calc", ">>", *t, INT, *t, SCALE_FIX);
		}
		sql_create_func("sql_neg", "calc", "-", *t, NULL, *t, INOUT);
		sql_create_func("sql_pos", "calc", "+", *t, NULL, *t, INOUT);
		sql_create_func("abs", "calc", "abs", *t, NULL, *t, SCALE_FIX);
		sql_create_func("sign", "calc", "sign", *t, NULL, *t, SCALE_FIX);
		/* scale fixing for all numbers */
		sql_create_func("scale_up", "calc", "*", *t, lt->type, *t, SCALE_NONE);
		sql_create_func("scale_down", "sql", "dec_round", *t, lt->type, *t, SCALE_NONE);
		/* numeric function on INTERVALS */
		if (*t != MONINT && *t != SECINT){
			sql_create_func("sql_sub", "calc", "-", MONINT, *t, MONINT, SCALE_FIX);
			sql_create_func("sql_add", "calc", "+", MONINT, *t, MONINT, SCALE_FIX);
			sql_create_func("sql_mul", "calc", "*", MONINT, *t, MONINT, SCALE_MUL);
			sql_create_func("sql_div", "calc", "/", MONINT, *t, MONINT, SCALE_DIV);
			sql_create_func("sql_sub", "calc", "-", SECINT, *t, SECINT, SCALE_FIX);
			sql_create_func("sql_add", "calc", "+", SECINT, *t, SECINT, SCALE_FIX);
			sql_create_func("sql_mul", "calc", "*", SECINT, *t, SECINT, SCALE_MUL);
			sql_create_func("sql_div", "calc", "/", SECINT, *t, SECINT, SCALE_DIV);
		}
	}
	for (t = decimals, t++; t != floats; t++) {
		sql_type **u;
		for (u = numerical; u != floats; u++) {
			if (*u == OID)
				continue;
			if ((*t)->localtype >  (*u)->localtype) {
				sql_create_func("sql_mul", "calc", "*", *t, *u, *t, SCALE_MUL);
				sql_create_func("sql_mul", "calc", "*", *u, *t, *t, SCALE_MUL);
			}
		}
	}

	for (t = decimals; t < dates; t++) 
		sql_create_func("round", "sql", "round", *t, BTE, *t, INOUT);

	for (t = numerical; t < end; t++) {
		sql_type **u;

		for (u = numerical; u < end; u++) 
			sql_create_func("scale_up", "calc", "*", *u, *t, *t, SCALE_NONE);
	}

	for (t = dates-1; t >= floats; t--) {
		sql_create_func("power", "mmath", "pow", *t, *t, *t, SCALE_FIX);
		sql_create_func("floor", "mmath", "floor", *t, NULL, *t, SCALE_FIX);
		sql_create_func("ceil", "mmath", "ceil", *t, NULL, *t, SCALE_FIX);
		sql_create_func("ceiling", "mmath", "ceil", *t, NULL, *t, SCALE_FIX);	/* JDBC */
		sql_create_func("sin", "mmath", "sin", *t, NULL, *t, SCALE_FIX);
		sql_create_func("cos", "mmath", "cos", *t, NULL, *t, SCALE_FIX);
		sql_create_func("tan", "mmath", "tan", *t, NULL, *t, SCALE_FIX);
		sql_create_func("asin", "mmath", "asin", *t, NULL, *t, SCALE_FIX);
		sql_create_func("acos", "mmath", "acos", *t, NULL, *t, SCALE_FIX);
		sql_create_func("atan", "mmath", "atan", *t, NULL, *t, SCALE_FIX);
		sql_create_func("atan", "mmath", "atan2", *t, *t, *t, SCALE_FIX);
		sql_create_func("sinh", "mmath", "sinh", *t, NULL, *t, SCALE_FIX);
		sql_create_func("cot", "mmath", "cot", *t, NULL, *t, SCALE_FIX);
		sql_create_func("cosh", "mmath", "cosh", *t, NULL, *t, SCALE_FIX);
		sql_create_func("tanh", "mmath", "tanh", *t, NULL, *t, SCALE_FIX);
		sql_create_func("sqrt", "mmath", "sqrt", *t, NULL, *t, SCALE_FIX);
		sql_create_func("exp", "mmath", "exp", *t, NULL, *t, SCALE_FIX);
		sql_create_func("log", "mmath", "log", *t, NULL, *t, SCALE_FIX);
		sql_create_func("log10", "mmath", "log10", *t, NULL, *t, SCALE_FIX);
	}
	sql_create_func("pi", "mmath", "pi", NULL, NULL, DBL, SCALE_NONE);

	sql_create_funcSE("rand", "mmath", "rand", NULL, NULL, INT, SCALE_NONE);
	sql_create_funcSE("rand", "mmath", "srand", INT, NULL, INT, SCALE_NONE);

	/* Date functions */
	sql_create_func("curdate", "mtime", "current_date", NULL, NULL, DTE, SCALE_NONE);
	sql_create_func("current_date", "mtime", "current_date", NULL, NULL, DTE, SCALE_NONE);
	sql_create_func("curtime", "mtime", "current_time", NULL, NULL, TMETZ, SCALE_NONE);
	sql_create_func("current_time", "mtime", "current_time", NULL, NULL, TMETZ, SCALE_NONE);
	sql_create_func("current_timestamp", "mtime", "current_timestamp", NULL, NULL, TMESTAMPTZ, SCALE_NONE);
	sql_create_func("localtime", "mtime", "current_time", NULL, NULL, TME, SCALE_NONE);
	sql_create_func("localtimestamp", "mtime", "current_timestamp", NULL, NULL, TMESTAMP, SCALE_NONE);

	sql_create_func("sql_sub", "mtime", "date_sub_sec_interval", DTE, SECINT, DTE, SCALE_FIX);
	sql_create_func("sql_sub", "mtime", "date_sub_month_interval", DTE, MONINT, DTE, SCALE_FIX);
	sql_create_func("sql_sub", "mtime", "timestamp_sub_sec_interval", TMESTAMP, SECINT, TMESTAMP, SCALE_FIX);
	sql_create_func("sql_sub", "mtime", "timestamp_sub_month_interval", TMESTAMP, MONINT, TMESTAMP, SCALE_FIX);
	sql_create_func("sql_sub", "mtime", "timestamp_sub_sec_interval", TMESTAMPTZ, SECINT, TMESTAMPTZ, SCALE_FIX);
	sql_create_func("sql_sub", "mtime", "timestamp_sub_month_interval", TMESTAMPTZ, MONINT, TMESTAMPTZ, SCALE_FIX);
	sql_create_func("sql_sub", "mtime", "time_sub_sec_interval", TME, SECINT, TME, SCALE_FIX);

	sql_create_func("sql_sub", "mtime", "diff", DTE, DTE, INT, SCALE_FIX);
	sql_create_func("sql_sub", "mtime", "diff", TMESTAMP, TMESTAMP, LNG, SCALE_FIX);

	sql_create_func("sql_add", "mtime", "date_add_sec_interval", DTE, SECINT, DTE, SCALE_NONE);
	sql_create_func("sql_add", "mtime", "addmonths", DTE, MONINT, DTE, SCALE_NONE);
	sql_create_func("sql_add", "mtime", "timestamp_add_sec_interval", TMESTAMP, SECINT, TMESTAMP, SCALE_NONE);
	sql_create_func("sql_add", "mtime", "timestamp_add_month_interval", TMESTAMP, MONINT, TMESTAMP, SCALE_NONE);
	sql_create_func("sql_add", "mtime", "timestamp_add_sec_interval", TMESTAMPTZ, SECINT, TMESTAMPTZ, SCALE_NONE);
	sql_create_func("sql_add", "mtime", "timestamp_add_month_interval", TMESTAMPTZ, MONINT, TMESTAMPTZ, SCALE_NONE);
	sql_create_func("sql_add", "mtime", "time_add_sec_interval", TME, SECINT, TME, SCALE_NONE);
	sql_create_func("local_timezone", "mtime", "local_timezone", NULL, NULL, SECINT, SCALE_FIX);

	sql_create_func("year", "mtime", "year", DTE, NULL, INT, SCALE_FIX);
	sql_create_func("month", "mtime", "month", DTE, NULL, INT, SCALE_FIX);
	sql_create_func("day", "mtime", "day", DTE, NULL, INT, SCALE_FIX);
	sql_create_func("hour", "mtime", "hours", TME, NULL, INT, SCALE_FIX);
	sql_create_func("minute", "mtime", "minutes", TME, NULL, INT, SCALE_FIX);
	f = sql_create_func("second", "mtime", "sql_seconds", TME, NULL, DEC, SCALE_NONE);
	/* fix result type */
	f->res.scale = 3;

	sql_create_func("year", "mtime", "year", TMESTAMP, NULL, INT, SCALE_FIX);
	sql_create_func("month", "mtime", "month", TMESTAMP, NULL, INT, SCALE_FIX);
	sql_create_func("day", "mtime", "day", TMESTAMP, NULL, INT, SCALE_FIX);
	sql_create_func("hour", "mtime", "hours", TMESTAMP, NULL, INT, SCALE_FIX);
	sql_create_func("minute", "mtime", "minutes", TMESTAMP, NULL, INT, SCALE_FIX);
	f = sql_create_func("second", "mtime", "sql_seconds", TMESTAMP, NULL, DEC, SCALE_NONE);
	/* fix result type */
	f->res.scale = 3;

	sql_create_func("year", "mtime", "year", MONINT, NULL, INT, SCALE_NONE);
	sql_create_func("month", "mtime", "month", MONINT, NULL, INT, SCALE_NONE);
	sql_create_func("day", "mtime", "day", SECINT, NULL, LNG, SCALE_NONE);
	sql_create_func("hour", "mtime", "hours", SECINT, NULL, INT, SCALE_NONE);
	sql_create_func("minute", "mtime", "minutes", SECINT, NULL, INT, SCALE_NONE);
	sql_create_func("second", "mtime", "seconds", SECINT, NULL, INT, SCALE_NONE);

	sql_create_func("dayofyear", "mtime", "dayofyear", DTE, NULL, INT, SCALE_FIX);
	sql_create_func("weekofyear", "mtime", "weekofyear", DTE, NULL, INT, SCALE_FIX);
	sql_create_func("dayofweek", "mtime", "dayofweek", DTE, NULL, INT, SCALE_FIX);
	sql_create_func("dayofmonth", "mtime", "day", DTE, NULL, INT, SCALE_FIX);
	sql_create_func("week", "mtime", "weekofyear", DTE, NULL, INT, SCALE_FIX);

	sql_create_funcSE("next_value_for", "sql", "next_value", STR, STR, LNG, SCALE_NONE);
	sql_create_func("get_value_for", "sql", "get_value", STR, STR, LNG, SCALE_NONE);
	sql_create_func3("restart", "sql", "restart", STR, STR, LNG, LNG, SCALE_NONE);
	for (t = strings; t < numerical; t++) {
		sql_create_func("locate", "str", "locate", *t, *t, INT, SCALE_NONE);
		sql_create_func3("locate", "str", "locate", *t, *t, INT, INT, SCALE_NONE);
		sql_create_func("substring", "str", "substring", *t, INT, *t, INOUT);
		sql_create_func3("substring", "str", "substring", *t, INT, INT, *t, INOUT);
		sql_create_func("like", "str", "like", *t, *t, BIT, SCALE_NONE);
		sql_create_func3("like", "str", "like", *t, *t, *t, BIT, SCALE_NONE);
		sql_create_func("ilike", "str", "ilike", *t, *t, BIT, SCALE_NONE);
		sql_create_func3("ilike", "str", "ilike", *t, *t, *t, BIT, SCALE_NONE);
		sql_create_func("patindex", "pcre", "patindex", *t, *t, INT, SCALE_NONE);
		sql_create_func("truncate", "str", "stringleft", *t, INT, *t, SCALE_NONE);
		sql_create_func("concat", "calc", "+", *t, *t, *t, DIGITS_ADD);
		sql_create_func("ascii", "str", "ascii", *t, NULL, INT, SCALE_NONE);
		sql_create_func("code", "str", "unicode", INT, NULL, *t, SCALE_NONE);
		sql_create_func("length", "str", "stringlength", *t, NULL, INT, SCALE_NONE);
		sql_create_func("right", "str", "stringright", *t, INT, *t, SCALE_NONE);
		sql_create_func("left", "str", "stringleft", *t, INT, *t, SCALE_NONE);
		sql_create_func("upper", "str", "toUpper", *t, NULL, *t, SCALE_NONE);
		sql_create_func("ucase", "str", "toUpper", *t, NULL, *t, SCALE_NONE);
		sql_create_func("lower", "str", "toLower", *t, NULL, *t, SCALE_NONE);
		sql_create_func("lcase", "str", "toLower", *t, NULL, *t, SCALE_NONE);
		sql_create_func("trim", "str", "trim", *t, NULL, *t, SCALE_NONE);
		sql_create_func("ltrim", "str", "ltrim", *t, NULL, *t, SCALE_NONE);
		sql_create_func("rtrim", "str", "rtrim", *t, NULL, *t, SCALE_NONE);

		sql_create_func4("insert", "str", "insert", *t, INT, INT, *t, *t, SCALE_NONE);
		sql_create_func3("replace", "str", "replace", *t, *t, *t, *t, SCALE_NONE);
		sql_create_func("repeat", "str", "repeat", *t, INT, *t, SCALE_NONE);
		sql_create_func("space", "str", "space", INT, NULL, *t, SCALE_NONE);
		sql_create_func("char_length", "str", "length", *t, NULL, INT, SCALE_NONE);
		sql_create_func("character_length", "str", "length", *t, NULL, INT, SCALE_NONE);
		sql_create_func("octet_length", "str", "nbytes", *t, NULL, INT, SCALE_NONE);

		sql_create_func("soundex", "txtsim", "soundex", *t, NULL, *t, SCALE_NONE);
		sql_create_func("difference", "txtsim", "stringdiff", *t, *t, INT, SCALE_NONE);
		sql_create_func("editdistance", "txtsim", "editdistance", *t, *t, INT, SCALE_FIX);
		sql_create_func("editdistance2", "txtsim", "editdistance2", *t, *t, INT, SCALE_FIX);

		sql_create_func("similarity", "txtsim", "similarity", *t, *t, DBL, SCALE_FIX);
		sql_create_func("qgramnormalize", "txtsim", "qgramnormalize", *t, NULL, *t, SCALE_NONE);

		sql_create_func("levenshtein", "txtsim", "levenshtein", *t, *t, INT, SCALE_FIX);
		{ sql_subtype sres;
		sql_init_subtype(&sres, INT, 0, 0);
		sql_create_func_("levenshtein", "txtsim", "levenshtein",
				 list_append(list_append (list_append (list_append(list_append(list_create((fdestroy) &arg_destroy), create_arg(NULL, sql_create_subtype(*t, 0, 0))), create_arg(NULL, sql_create_subtype(*t, 0, 0))), create_arg(NULL, sql_create_subtype(INT, 0, 0))), create_arg(NULL, sql_create_subtype(INT, 0, 0))), create_arg(NULL, sql_create_subtype(INT, 0, 0))), &sres, FALSE, FALSE, SCALE_FIX);
		}
	}
	{ sql_subtype sres;
	sql_init_subtype(&sres, TABLE, 0, 0);
	/* copyfrom fname (arg 6) */
	sql_create_func_("copyfrom", "sql", "copy_from",
	 	list_append( list_append( list_append( list_append(list_append (list_append (list_append(list_append(list_create((fdestroy) &arg_destroy), 
			create_arg(NULL, sql_create_subtype(STR, 0, 0))), 
			create_arg(NULL, sql_create_subtype(STR, 0, 0))), 
			create_arg(NULL, sql_create_subtype(STR, 0, 0))), 
			create_arg(NULL, sql_create_subtype(STR, 0, 0))), 
			create_arg(NULL, sql_create_subtype(STR, 0, 0))), 
			create_arg(NULL, sql_create_subtype(STR, 0, 0))), 
			create_arg(NULL, sql_create_subtype(LNG, 0, 0))), 
			create_arg(NULL, sql_create_subtype(LNG, 0, 0))), &sres, FALSE, FALSE, SCALE_FIX);

	/* copyfrom stdin */
	sql_create_func_("copyfrom", "sql", "copyfrom",
	 	list_append( list_append( list_append(list_append (list_append (list_append(list_append(list_create((fdestroy) &arg_destroy), 
			create_arg(NULL, sql_create_subtype(STR, 0, 0))), 
			create_arg(NULL, sql_create_subtype(STR, 0, 0))), 
			create_arg(NULL, sql_create_subtype(STR, 0, 0))), 
			create_arg(NULL, sql_create_subtype(STR, 0, 0))), 
			create_arg(NULL, sql_create_subtype(STR, 0, 0))), 
			create_arg(NULL, sql_create_subtype(LNG, 0, 0))), 
			create_arg(NULL, sql_create_subtype(LNG, 0, 0))), &sres, FALSE, FALSE, SCALE_FIX);

	/* bincopyfrom */
	sql_create_func_("copyfrom", "sql", "importTable",
	 	list_append(list_append(list_create((fdestroy) &arg_destroy), 
			create_arg(NULL, sql_create_subtype(STR, 0, 0))), 
			create_arg(NULL, sql_create_subtype(STR, 0, 0))), &sres, FALSE, FALSE, SCALE_FIX);
	}
}

void
types_exit(void)
{
	list_destroy(aggrs);
	list_destroy(funcs);
	list_destroy(localtypes);
	list_destroy(aliases);
	list_destroy(types);
}
