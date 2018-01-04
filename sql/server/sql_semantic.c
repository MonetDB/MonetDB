/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0.  If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 *
 * Copyright 1997 - July 2008 CWI, August 2008 - 2018 MonetDB B.V.
 */

#include "monetdb_config.h"
#include "sql_parser.h"
#include "sql_symbol.h"
#include "sql_semantic.h"
#include "sql_env.h"
#include "sql_privileges.h"
#include "sql_string.h"
#include "sql_atom.h"

#include <unistd.h>
#include <stdio.h>
#include <string.h>
#include <ctype.h>

#include "rel_semantic.h"
#include "rel_optimizer.h"

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
sql_set_arg(mvc *sql, int nr, atom *v)
{
	if (nr >= sql->argmax) {
		sql->argmax *= 2;
		if (nr >= sql->argmax)
			sql->argmax = nr*2;
		sql->args = RENEW_ARRAY(atom*,sql->args,sql->argmax);
	}
	if (sql->argc < nr+1)
		sql->argc = nr+1;
	sql->args[nr] = v;
}

void
sql_add_param(mvc *sql, const char *name, sql_subtype *st)
{
	sql_arg *a = SA_ZNEW(sql->sa, sql_arg);

	if (name)
		a->name = sa_strdup(sql->sa, name);
	if (st && st->type)
		a->type = *st;
	a->inout = ARG_IN;
	if (name && strcmp(name, "*") == 0) 
		a->type = *sql_bind_localtype("int");
	if (!sql->params)
		sql->params = sa_list(sql->sa);
	list_append(sql->params, a);
}

sql_arg *
sql_bind_param(mvc *sql, const char *name)
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

		if (n)
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

int
set_type_param(mvc *sql, sql_subtype *type, int nr)
{
	sql_arg *a = sql_bind_paramnr(sql, nr);

	if (!a) 
		return -1;
	a->type = *type;
	return 0;
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
	sql_subtype lsuper;

	lsuper = *r;
	if (i->type->base.id > r->type->base.id || 
	    (EC_VARCHAR(i->type->eclass) && !EC_VARCHAR(r->type->eclass))) {
		lsuper = *i;
		radix = i->type->radix;
		tpe = i->type->sqlname;
	}
	if (!lsuper.type->localtype)
		tpe = "smallint";
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
	if (scale == 0 && (idigits == 0 || rdigits == 0)) {
		sql_find_subtype(&lsuper, tpe, 0, 0);
	} else {
		digits = sql_max(idigits - i->scale, rdigits - r->scale);
		sql_find_subtype(&lsuper, tpe, digits+scale, scale);
	}
	*super = lsuper;
	return super;
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
			s = _STRDUP(n->data.sval);
		else if (n->type == type_symbol)
			s = symbol2string(sql, n->data.sym, err);

		if (!s)
			return NULL;
		if (b) {
			char *o = b;
			b = strconcat(b,".");
			_DELETE(o);
			o = b;
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
			char *tmp = symbol2string(sql, ops->data.sym, err);
			if (tmp == NULL)
				return NULL;
			len = snprintf( buf+len, BUFSIZ-len, "%s%s", 
				tmp, 
				(ops->next)?",":"");
			_DELETE(tmp);
		}
		len = snprintf( buf+len, BUFSIZ-len, ")"); 
	} break;
	case SQL_BINOP: {
		dnode *lst = se->data.lval->h;
		char *op = qname_fname(lst->data.lval);
		char *l;
		char *r;
		l = symbol2string(sql, lst->next->data.sym, err);
		if (l == NULL)
			return NULL;
		r = symbol2string(sql, lst->next->next->data.sym, err);
		if (r == NULL) {
			_DELETE(l);
			return NULL;
		}
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
		if (l == NULL)
			return NULL;
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
		const char *seq = qname_table(se->data.lval);
		const char *sname = qname_schema(se->data.lval);
		const char *s;
		
		if (!sname)
			sname = sql->session->schema->base.name;
		len = snprintf( buf+len, BUFSIZ-len, "next value for \"%s\".\"%s\"", sname, s=sql_escape_ident(seq)); 
		c_delete(s);
	}	break;
	case SQL_COLUMN: {
		/* can only be variables */ 
		dlist *l = se->data.lval;
		assert(l->h->type != type_lng);
		if (dlist_length(l) == 1 && l->h->type == type_int) {
			atom *a = sql_bind_arg(sql, l->h->data.i_val);
			return atom2sql(a);
		} else {
			char *e = dlist2string(sql, l, err);
			if (e)
				*err = e;
		}
		return NULL;
	} 	
	case SQL_CAST: {
		dlist *dl = se->data.lval;
		char *val;
		char *tpe;

		val = symbol2string(sql, dl->h->data.sym, err);
		if (val == NULL)
			return NULL;
		tpe = subtype2string(&dl->h->next->data.typeval);
		if (tpe == NULL) {
			_DELETE(val);
			return NULL;
		}
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
	return _STRDUP(buf);
}
