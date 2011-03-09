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


#include "monetdb_config.h"
#include "sql_parser.h"
#include "sql_symbol.h"
#include "sql_statement.h"
#include "sql_semantic.h"
#include "sql_schema.h"
#include "sql_env.h"
#include "sql_privileges.h"
#include "sql_psm.h"
#include "sql_string.h"
#include "sql_atom.h"

#include <unistd.h>
#include <stdio.h>
#include <string.h>
#include <ctype.h>

#include <rel_semantic.h>
#include <rel_optimizer.h>
#include <rel_bin.h>

/* 
 * For debugging purposes we need to be able to convert sql-tokens to 
 * a string representation.
 *
 * !SQL ERROR <sqlerrno> : <details>
 * !SQL DEBUG  <details>
 * !SQL WARNING <details>
 * !SQL  <informative message, reserved for ...rows affected>
 */

void
sql_add_arg(mvc *sql, atom *v)
{
	if (sql->argc == sql->argmax) {
		sql->argmax *= 2;
		sql->args = RENEW_ARRAY(atom*,sql->args,sql->argmax);
	}
	sql->args[sql->argc++] = v;
}

void
sql_add_param(mvc *sql, char *name, sql_subtype *st)
{
	sql_arg *a = SA_NEW(sql->sa, sql_arg);

	a->name = NULL;
	if (name)
		a->name = sa_strdup(sql->sa, name);
	if (st)
		a->type = *st;
	else
		a->type.type = NULL;

	if (!sql->params)
		sql->params = list_new(sql->sa);
	list_append(sql->params, a);
}

sql_arg *
sql_bind_param(mvc *sql, char *name)
{
	node *n;

	if (sql->params) {
		for (n = sql->params->h; n; n = n->next) {
			sql_arg *a = n->data;

			if (a->name && strcmp(a->name, name) == 0) 
				return a;
		}
	}
	return NULL;
}

static sql_arg *
sql_bind_paramnr(mvc *sql, int nr)
{
	int i=0;
	node *n;

	if (sql->params && nr < list_length(sql->params)) {
		for (n = sql->params->h, i=0; n && i<nr; n = n->next, i++) 
			;

		return n->data;
	}
	return NULL;
}


atom *
sql_bind_arg(mvc *sql, int nr)
{
	if (nr < sql->argc)
		return sql->args[nr];
	return NULL;
}

void
sql_convert_arg(mvc *sql, int nr, sql_subtype *rt)
{
	atom *a = sql_bind_arg(sql, nr);

	if (atom_null(a)) {
		if (a->data.vtype != rt->type->localtype) {
			ptr p;

			a->data.vtype = rt->type->localtype;
			p = ATOMnilptr(a->data.vtype);
			VALset(&a->data, a->data.vtype, p);
		}
	}
	a->tpe = *rt;
}

void
sql_destroy_params(mvc *sql)
{
	sql->params = NULL;
}

void
sql_destroy_args(mvc *sql)
{
	sql->argc = 0;
}


sql_schema *
cur_schema(mvc *sql)
{
	return sql->session->schema;
}

sql_schema *
tmp_schema(mvc *sql)
{
	return mvc_bind_schema(sql, "tmp");
}

char *
qname_schema(dlist *qname)
{
	assert(qname && qname->h);

	if (dlist_length(qname) == 2) {
		return qname->h->data.sval;
	} else if (dlist_length(qname) == 3) {
		return qname->h->next->data.sval;
	}
	return NULL;
}

char *
qname_table(dlist *qname)
{
	assert(qname && qname->h);

	if (dlist_length(qname) == 1) {
		return qname->h->data.sval;
	} else if (dlist_length(qname) == 2) {
		return qname->h->next->data.sval;
	} else if (dlist_length(qname) == 3) {
		return qname->h->next->next->data.sval;
	}
	return "unknown";
}

char *
qname_catalog(dlist *qname)
{
	assert(qname && qname->h);

	if (dlist_length(qname) == 3) {
		return qname->h->data.sval;
	} 

	return NULL;
}

stmt *
sql_parse(mvc *m, sql_allocator *sa, char *query, char mode)
{
	mvc *o = NULL;
	stmt *sq = NULL;
	buffer *b;
	char *n;
	int len = _strlen(query);
	stream *buf;

 	if (THRhighwater())
		return sql_error(m, 10, "SELECT: too many nested operators");

	o = NEW(mvc);
	if (!o)
		return NULL;
	*o = *m;

	m->qc = NULL;
	m->last = NULL;

	m->caching = 0;
	m->emode = mode;

	b = (buffer*)GDKmalloc(sizeof(buffer));
	n = GDKmalloc(len + 1 + 1);
	strncpy(n, query, len);
	query = n;
	query[len] = '\n';
	query[len+1] = 0;
	len++;
	buffer_init(b, query, len);
	buf = buffer_rastream(b, "sqlstatement");
	scanner_init( &m->scanner, bstream_create(buf, b->len), NULL);
	m->scanner.mode = LINE_1; 
	bstream_next(m->scanner.rs);

	m->params = NULL;
	m->argc = 0;
	m->sym = NULL;
	m->errstr[0] = '\0';
	m->errstr[ERRSIZE-1] = '\0';
	/* via views we give access to protected objects */
	m->user_id = USER_MONETDB;

	/* create private allocator */
	m->sa = (sa)?sa:sa_create();

	if (sqlparse(m) || !m->sym) {
		/* oops an error */
		snprintf(m->errstr, ERRSIZE, "An error occurred when executing "
				"internal query: %s", query);
	} else {
		sql_rel *r = rel_semantic(m, m->sym);

		if (r) {
			r = rel_optimizer(m, r);
			sq = rel_bin(m, r);
		} else {
			sq = semantic(m, m->sym);
		}
	}

	GDKfree(query);
	GDKfree(b);
	bstream_destroy(m->scanner.rs);
	if (m->sa && m->sa != sa)
		sa_destroy(m->sa);
	m->sym = NULL;
	{
		char *e = NULL;
		int status = m->session->status;
		int sizevars = m->sizevars, topvars = m->topvars;
		sql_var *vars = m->vars;
		/* cascade list maybe removed */
		list *cascade_action = m->cascade_action;

		if (m->session->status || m->errstr[0]) {
			e = _strdup(m->errstr);
			if (!e) {
				_DELETE(o);
				return NULL;
			}
		}
		*m = *o;
		m->sizevars = sizevars;
		m->topvars = topvars;
		m->vars = vars;
		m->session->status = status;
		m->cascade_action = cascade_action;
		if (e) {
			strncpy(m->errstr, e, ERRSIZE);
			m->errstr[ERRSIZE - 1] = '\0';
			_DELETE(e);
		}
	}
	_DELETE(o);
	m->last = NULL;
	return sq;
}

static lng
scale2value(int scale)
{
	lng val = 1;

	if (scale < 0)
		scale = -scale;
	for (; scale; scale--) {
		val = val * 10;
	}
	return val;
}

stmt *
sum_scales(mvc *sql, sql_subfunc *f, stmt *ls, stmt *rs)
{
	if (strcmp(f->func->imp, "*") == 0 && 
	    f->func->res.type->scale == SCALE_FIX) {
		sql_subtype t;
		sql_subtype *lt = tail_type(ls);
		sql_subtype *rt = tail_type(rs);

		f->res.scale = lt->scale + rt->scale;
		f->res.digits = lt->digits + rt->digits;
		/* HACK alert: digits should be less then max */
		if (f->res.type->radix == 10 && f->res.digits > 19)
			f->res.digits = 19;
		if (f->res.type->radix == 2 && f->res.digits > 53)
			f->res.digits = 53;
		/* sum of digits may mean we need a bigger result type 
		 * as the function don't support this we need to
		 * make bigger input types!
		 */

		/* numeric types are fixed length */
		if (f->res.type->eclass == EC_NUM) {
			sql_find_numeric(&t, f->res.type->localtype, f->res.digits);
		} else {
			sql_find_subtype(&t, f->res.type->sqlname, f->res.digits, f->res.scale);
		}
		if (type_cmp(t.type, f->res.type) != 0) { 
			/* do we need to convert to the a larger localtype 
			   int * int may not fit in an int, so we need to 
			   convert to lng * int.
			 */
			sql_subtype nlt;

			sql_init_subtype(&nlt, t.type, f->res.digits, lt->scale);
			ls = check_types( sql, &nlt, ls, type_equal);
		}
		f->res = t;
	}
	return ls;
}

stmt *
scale_algebra(mvc *sql, sql_subfunc *f, stmt *ls, stmt *rs)
{
	sql_subtype *lt = tail_type(ls);
	sql_subtype *rt = tail_type(rs);

	/*
	 * Decimals are mapped on plain integers. This has impact on the
	 * implemantion of division. First the 'dividend' should be large 
	 * enough to prevent rounding errors. This is solved by a 
	 * multiplication with the 'scale' of the divisor.
	 * Second the result type of the division should be equal to the 
	 * 'dividend', with the maximum scale of the dividend and divisor.
	 *
	 * Example      1.0/0.1 mapped (int 1 dec(1,0) and int 1 dec(2,1))
	 *                              1 * 10 = 10 (scale of divisor)
	 *                              10/1 = 1 dec(1)
	 */

	if (lt->type->scale == SCALE_FIX && rt->scale && 
		strcmp(f->func->imp, "/") == 0) {
		int digits = rt->scale + lt->digits;
		sql_subtype nlt;

		/* HACK alert: digits should be less then max */
		if (f->res.type->radix == 10 && digits > 19)
			digits = 19;
		if (f->res.type->radix == 2 && digits > 53)
			digits = 53;

		/* scale fixing may require a larger type ! */
		sql_find_subtype(&nlt, lt->type->sqlname, digits, lt->scale+rt->scale);
		f->res.digits = digits;
		f->res.scale = lt->scale;
		
		ls = check_types( sql, &nlt, ls, type_equal );
	}
	return ls;
}

stmt *
fix_scale(mvc *sql, sql_subtype *ct, stmt *s, int both, int always)
{
	sql_subtype *st = tail_type(s);

	(void) sql;		/* Stefan: unused!? */

	if (ct->type->scale == SCALE_FIX && st->type->scale == SCALE_FIX) {
		int scale_diff = ((int) ct->scale - (int) st->scale);

		if (scale_diff) {
			sql_subtype *it = sql_bind_localtype(st->type->base.name);
			sql_subfunc *c = NULL;

			if (scale_diff < 0) {
				if (!both)
					return s;
				c = sql_bind_func(sql->sa, sql->session->schema, "scale_down", st, it);
			} else {
				c = sql_bind_func(sql->sa, sql->session->schema, "scale_up", st, it);
			}
			if (c) {
				lng val = scale2value(scale_diff);
				atom *a = atom_int(sql->sa, it, val);

				c->res.scale = (st->scale + scale_diff);
				return stmt_binop(sql->sa, s, stmt_atom(sql->sa, a), c);
			}
		}
	} else if (always && st->scale) {	/* scale down */
		int scale_diff = -(int) st->scale;
		sql_subtype *it = sql_bind_localtype(st->type->base.name);
		sql_subfunc *c = sql_bind_func(sql->sa, sql->session->schema, "scale_down", st, it);

		if (c) {
			lng val = scale2value(scale_diff);
			atom *a = atom_int(sql->sa, it, val);

			c->res.scale = 0;
			return stmt_binop(sql->sa, s, stmt_atom(sql->sa, a), c);
		} else {
			printf("scale_down mising (%s)\n", st->type->base.name);
		}
	}
	return s;
}

/* try to do an inplace convertion 
 * 
 * inplace conversion is only possible if the s is an variable.
 * This is only done to be able to map more cached queries onto the same 
 * interface.
 */
stmt *
inplace_convert(mvc *sql, sql_subtype *ct, stmt *s)
{
	atom *a;

	/* exclude named variables */
	if (s->type != st_var || s->op1->op4.aval->data.val.sval || 
		(ct->scale && ct->type->eclass != EC_FLT))
		return s;

	a = sql_bind_arg(sql, s->flag);
	if (atom_cast(a, ct)) {
		stmt *r = stmt_varnr(sql->sa, s->flag, ct);
		sql_convert_arg(sql, s->flag, ct);
		return r;
	}
	return s;
}

int
set_type_param(mvc *sql, sql_subtype *type, int nr)
{
	sql_arg *a = sql_bind_paramnr(sql, nr);

	if (!a) 
		return -1;
	a->type = *type;
	return 0;
}

int
stmt_set_type_param(mvc *sql, sql_subtype *type, stmt *param)
{
	if (!type || !param || param->type != st_var)
		return -1;

	if (set_type_param(sql, type, param->flag) == 0) {
		param->op4.typeval = *type;
		return 0;
	}
	return -1;
}


static stmt *
check_table_types(mvc *sql, sql_table *ct, stmt *s, check_type tpe)
{
	char *tname;
	stmt *tab = s;
	int temp = 0;

	if (s->type != st_table) {
		char *t = (ct->type==tt_generated)?"table":"unknown";
		return sql_error(
			sql, 03,
			"single value and complex type '%s' are not equal", t);
	}
	tab = s->op1;
	temp = s->flag;
	if (tab->type == st_var) {
		sql_table *tbl = tail_type(tab)->comp_type;
		stmt *base = stmt_basetable(sql->sa, tbl, tab->op1->op4.aval->data.val.sval);
		node *n, *m;
		list *l = list_new(sql->sa);
		
		stack_find_var(sql, tab->op1->op4.aval->data.val.sval);

		for (n = ct->columns.set->h, m = tbl->columns.set->h; 
			n && m; n = n->next, m = m->next) 
		{
			sql_column *c = n->data;
			sql_column *dtc = m->data;
			stmt *dtcs = stmt_bat(sql->sa, dtc, base, RDONLY);
			stmt *r = check_types(sql, &c->type, dtcs, tpe);
			if (!r) 
				return NULL;
			r = stmt_alias(sql->sa, r, sa_strdup(sql->sa, tbl->base.name), sa_strdup(sql->sa, c->base.name));
			list_append(l, r);
		}
	 	return stmt_table(sql->sa, stmt_list(sql->sa, l), temp);
	} else if (tab->type == st_list) {
		node *n, *m;
		list *l = list_new(sql->sa);
		for (n = ct->columns.set->h, m = tab->op4.lval->h; 
			n && m; n = n->next, m = m->next) 
		{
			sql_column *c = n->data;
			stmt *r = check_types(sql, &c->type, m->data, tpe);
			if (!r) 
				return NULL;
			tname = table_name(sql->sa, r);
			r = stmt_alias(sql->sa, r, tname, sa_strdup(sql->sa, c->base.name));
			list_append(l, r);
		}
		return stmt_table(sql->sa, stmt_list(sql->sa, l), temp);
	} else { /* single column/value */
		sql_column *c;
		stmt *r;
		sql_subtype *st = tail_type(tab);

		if (list_length(ct->columns.set) != 1) {
			stmt *res = sql_error(
				sql, 03,
				"single value of type %s and complex type '%s' are not equal",
				st->type->sqlname,
				(ct->type==tt_generated)?"table":"unknown"
			);
			return res;
		}
		c = ct->columns.set->h->data;
		r = check_types(sql, &c->type, tab, tpe);
		tname = table_name(sql->sa, r);
		r = stmt_alias(sql->sa, r, tname, sa_strdup(sql->sa, c->base.name));
		return stmt_table(sql->sa, r, temp);
	}
}

/* check_types tries to match the ct type with the type of s if they don't
 * match s is converted. Returns NULL on failure.
 */
stmt *
check_types(mvc *sql, sql_subtype *ct, stmt *s, check_type tpe)
{
	int c = 0;
	sql_subtype *t = NULL, *st = NULL;

	if (ct->comp_type) 
		return check_table_types(sql, ct->comp_type, s, tpe);

 	st = tail_type(s);
	if ((!st || !st->type) && stmt_set_type_param(sql, ct, s) == 0) {
		return s;
	} else if (!st) {
                return sql_error(sql, 02, "statement has no type information");
	}

	/* first try cheap internal (inplace) convertions ! */
	s = inplace_convert(sql, ct, s);
	t = st = tail_type(s);

	/* check if the types are the same */
	if (t && subtype_cmp(t, ct) != 0) {
		t = NULL;
	}

	if (!t) {	/* try to convert if needed */
		c = sql_type_convert(st->type->eclass, ct->type->eclass);
		if (!c || (c == 2 && tpe == type_set) || 
                   (c == 3 && tpe != type_cast)) { 
			s = NULL;
		} else {
			s = stmt_convert(sql->sa, s, st, ct);
		}
	} 
	if (!s) {
		stmt *res = sql_error(
			sql, 03,
			"types %s(%d,%d) (%s) and %s(%d,%d) (%s) are not equal",
			st->type->sqlname,
			st->digits,
			st->scale,
			st->type->base.name,
			ct->type->sqlname,
			ct->digits,
			ct->scale,
			ct->type->base.name
		);
		return res;
	}
	return s;
}

sql_subtype *
supertype(sql_subtype *super, sql_subtype *r, sql_subtype *i)
{
	/* first find super type */
	char *tpe = r->type->sqlname;
	int radix = r->type->radix;
	int digits = 0;
	int idigits = i->digits;
	int rdigits = r->digits;
	unsigned int scale = sql_max(i->scale, r->scale);

	*super = *r;
	if (i->type->base.id >r->type->base.id) {
		tpe = i->type->sqlname;
		radix = i->type->radix;
	}
	/* 
	 * Incase of different radix we should change one. 
	 */
	if (i->type->radix != r->type->radix) {
		if (radix == 10 || radix == 0 /* strings */) {
			/* change to radix 10 */
			if (i->type->radix == 2)
				idigits = bits2digits(idigits);
			if (r->type->radix == 2)
				rdigits = bits2digits(rdigits);
		} else if (radix == 2) { /* change to radix 2 */
			if (i->type->radix == 10)
				idigits = digits2bits(idigits);
			if (r->type->radix == 10)
				rdigits = digits2bits(rdigits);
		}
	}
	if (idigits && rdigits) {
		if (idigits > rdigits) {
			digits = idigits;
			if (i->scale < scale)
				digits += scale - i->scale;
		} else if (idigits < rdigits) {
			digits = rdigits;
			if (r->scale < scale)
				digits += scale - r->scale;
		} else {
			/* same number of digits */
			digits = idigits;
			if (i->scale < r->scale)
				digits += r->scale - i->scale;
			else
				digits += i->scale - r->scale;
		}
	}

	sql_find_subtype(super, tpe, digits, scale);
	return super;
}

/* convert_types convert both the stmt's L and R such that they get
 * matching types. On failure <0 is returned;
 */
int
convert_types(mvc *sql, stmt **L, stmt **R, int scale_fixing, check_type tpe)
{
	stmt *ls = *L;
	stmt *rs = *R;
	sql_subtype *lt = tail_type(ls);
	sql_subtype *rt = tail_type(rs);

	if (!rt && !lt) {
		sql_error(sql, 01, "Cannot have a parameter (?) on both sides of an expression");
		return -1;
	}

	if (rt && (!lt || !lt->type))
		return stmt_set_type_param(sql, rt, ls);

	if ((!rt || !rt->type) && lt)
		return stmt_set_type_param(sql, lt, rs);

	if (rt && lt) {
		sql_subtype *i = lt;
		sql_subtype *r = rt;

		if (subtype_cmp(lt, rt) != 0) {
			sql_subtype super;

			supertype(&super, r, i);
			if (scale_fixing) {
				/* convert ls to super type */
				ls = check_types(sql, &super, ls, tpe);
				/* convert rs to super type */
				rs = check_types(sql, &super, rs, tpe);
			} else {
				/* convert ls to super type */
				super.scale = lt->scale;
				ls = check_types(sql, &super, ls, tpe);
				/* convert rs to super type */
				super.scale = rt->scale;
				rs = check_types(sql, &super, rs, tpe);
			}
		/*
		} else if (scale_fixing) {
			ls = fix_scale(sql, r, ls, 0, 0);
			rs = fix_scale(sql, i, rs, 0, 0);
		*/
		}
		*L = ls;
		*R = rs;
		if (!ls || !rs) {
			return -1;
		}
		return 0;
	}
	return -1;
}

char * toUpperCopy(char *dest, const char *src) 
{
	int i, len;

	if (src == NULL) {
		*dest = '\0';
		return(dest);
	}

	len = _strlen(src);
	for (i = 0; i < len; i++)
		dest[i] = (char)toupper((int)src[i]);
	
	dest[i] = '\0';
	return(dest);
}

char *dlist2string(mvc *sql, dlist *l, char **err)
{
	char *b = NULL;
	dnode *n;

	for (n=l->h; n; n = n->next) {
		char *s = NULL;

		if (n->type == type_string && n->data.sval)
			s = _strdup(n->data.sval);
		else if (n->type == type_symbol)
			s = symbol2string(sql, n->data.sym, err);

		if (!s)
			return NULL;
		if (b) {
			char *o = b;
			b = strconcat(b,s);
			_DELETE(o);
			_DELETE(s);
		} else {
			b = s;
		}
	}
	return b;
}

char *symbol2string(mvc *sql, symbol *se, char **err)
{
	int len = 0;
	char buf[BUFSIZ];

	buf[0] = 0;
	switch (se->token) {
	case SQL_NOP: {
		dnode *lst = se->data.lval->h;
		dnode *ops = lst->next->data.lval->h;
		char *op = qname_fname(lst->data.lval);

		len = snprintf( buf+len, BUFSIZ-len, "%s(", op); 
		for (; ops; ops = ops->next) {
			char *tmp;
			len = snprintf( buf+len, BUFSIZ-len, "%s%s", 
				tmp = symbol2string(sql, ops->data.sym, err), 
				(ops->next)?",":"");
			_DELETE(tmp);
		}
		len = snprintf( buf+len, BUFSIZ-len, ")"); 
	} break;
	case SQL_BINOP: {
		dnode *lst = se->data.lval->h;
		char *op = qname_fname(lst->data.lval);
		char *l = symbol2string(sql, lst->next->data.sym, err);
		char *r = symbol2string(sql, lst->next->next->data.sym, err);
		len = snprintf( buf+len, BUFSIZ-len, "%s(%s,%s)", op, l, r); 
		_DELETE(l);
		_DELETE(r);
	} break;
	case SQL_OP: {
		dnode *lst = se->data.lval->h;
		char *op = qname_fname(lst->data.lval);
		len = snprintf( buf+len, BUFSIZ-len, "%s()", op ); 
	} break;
	case SQL_UNOP: {
		dnode *lst = se->data.lval->h;
		char *op = qname_fname(lst->data.lval);
		char *l = symbol2string(sql, lst->next->data.sym, err);
		len = snprintf( buf+len, BUFSIZ-len, "%s(%s)", op, l); 
		_DELETE(l);
		break;
	}
	case SQL_NULL:
		strcpy(buf,"NULL");
		break;
	case SQL_ATOM:{
		AtomNode *an = (AtomNode *) se;
		if (an && an->a) 
			return atom2sql(an->a);
		else
			strcpy(buf,"NULL");
		break;
	}
	case SQL_NEXT:{
		char *seq = qname_table(se->data.lval);
		char *sname = qname_schema(se->data.lval);
		char *s;
		
		if (!sname)
			sname = sql->session->schema->base.name;
		len = snprintf( buf+len, BUFSIZ-len, "next value for \"%s\".\"%s\"", sname, s=sql_escape_ident(seq)); 
		_DELETE(s);
	}	break;
	case SQL_COLUMN: {
		/* can only be variables */ 
		dlist *l = se->data.lval;
		assert(l->h->type != type_lng);
		if (dlist_length(l) == 1 && l->h->type == type_int) {
			atom *a = sql_bind_arg(sql, l->h->data.i_val);
			return atom2sql(a);
		} else {
			*err = dlist2string(sql, l, err);
		}
		return NULL;
	} 	
	case SQL_CAST: {
		dlist *dl = se->data.lval;
		char *val = symbol2string(sql, dl->h->data.sym, err);
		char *tpe = subtype2string(&dl->h->next->data.typeval);
	
		len = snprintf( buf+len, BUFSIZ-len, "cast ( %s as %s )",
				val, tpe);
		_DELETE(val);
		_DELETE(tpe);
		break;
	}
	case SQL_AGGR:
	case SQL_SELECT:
	case SQL_PARAMETER:
	case SQL_CASE:
	case SQL_COALESCE:
	case SQL_NULLIF:
	default:
		return NULL;
	}
	return _strdup(buf);
}


stmt *
semantic(mvc *sql, symbol *s)
{
	stmt *res = NULL;
	if (!s)
		return NULL;

	switch (s->token) {
	case SQL_CREATE_INDEX:
	case SQL_DROP_INDEX:
	case SQL_CREATE_USER:
	case SQL_DROP_USER:
	case SQL_ALTER_USER:
	case SQL_RENAME_USER:
	case SQL_CREATE_ROLE:
	case SQL_DROP_ROLE:
	case SQL_CREATE_TYPE:
	case SQL_CREATE_TRIGGER:
	case SQL_DROP_TRIGGER:
	case SQL_CONNECT:
	case SQL_DISCONNECT:
		res = schemas(sql, s);
		break;
	case SQL_CREATE_FUNC:
	case SQL_CREATE_PROC:
	case SQL_CREATE_AGGR:
	case SQL_DROP_FUNC:
	case SQL_DROP_PROC:
	case SQL_DECLARE:
	case SQL_CALL:
	case SQL_SET:
		res = psm(sql, s);
		break;
	default:
		res = sql_error(sql, 01, "sql_stmt symbol(" PTRFMT ")->token = %s", PTRFMTCAST s, token2string(s->token));
	}
	return res;
}

stmt *
output_semantic(mvc *sql, symbol *s)
{
	stmt *ret = semantic(sql, s);
	if (ret) {
		if (sql->type == Q_TABLE) {
			if (ret)
				ret = stmt_output(sql->sa, ret);
		}
	}
	return ret;
}
