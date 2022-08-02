/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0.  If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 *
 * Copyright 1997 - July 2008 CWI, August 2008 - 2020 MonetDB B.V.
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
#include <string.h>
#include <ctype.h>

#include "rel_semantic.h"
#include "rel_unnest.h"
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

atom *
sql_add_arg(mvc *sql, atom *v)
{
	atom** new_args;
	int next_size = sql->argmax;

	if (sql->argc == (1<<16)-1)
		sql->caching = 0;
	if (sql->caching && sql->argc == next_size) {
		next_size *= 2;
		new_args = RENEW_ARRAY(atom*,sql->args,next_size);
		if(new_args) {
			sql->args = new_args;
			sql->argmax = next_size;
		} else
			return NULL;
	}
	sql->args[sql->argc++] = v;
	return v;
}

atom *
sql_set_arg(mvc *sql, int nr, atom *v)
{
	atom** new_args;
	int next_size = sql->argmax;
	if (nr >= next_size) {
		next_size *= 2;
		if (nr >= next_size)
			next_size = nr*2;
		new_args = RENEW_ARRAY(atom*,sql->args,next_size);
		if(new_args) {
			sql->args = new_args;
			sql->argmax = next_size;
		} else
			return NULL;
	}
	if (sql->argc < nr+1)
		sql->argc = nr+1;
	sql->args[nr] = v;
	return v;
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
	 * In case of different radix we should change one.
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
	/* handle OID horror */
	if (i->type->radix == r->type->radix && i->type->base.id < r->type->base.id && strcmp(i->type->sqlname, "oid") == 0)
		tpe = i->type->sqlname;
	if (scale == 0 && (idigits == 0 || rdigits == 0)) {
		sql_find_subtype(&lsuper, tpe, 0, 0);
	} else {
		digits = sql_max(idigits - i->scale, rdigits - r->scale);
		sql_find_subtype(&lsuper, tpe, digits+scale, scale);
	}
	*super = lsuper;
	return super;
}

char *
toUpperCopy(char *dest, const char *src)
{
	size_t i, len;

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

char *
dlist2string(mvc *sql, dlist *l, int expression, char **err)
{
	char *b = NULL;
	dnode *n;

	for (n=l->h; n; n = n->next) {
		char *s = NULL;

		if (n->type == type_string && n->data.sval)
			s = _STRDUP(n->data.sval);
		else if (n->type == type_symbol)
			s = symbol2string(sql, n->data.sym, expression, err);

		if (!s) {
			_DELETE(b);
			return NULL;
		}
		if (b) {
			char *o = NEW_ARRAY(char, strlen(b) + strlen(s) + 2);
			if (o)
				stpcpy(stpcpy(stpcpy(o, b), "."), s);
			_DELETE(b);
			_DELETE(s);
			b = o;
			if (b == NULL)
				return NULL;
		} else {
			b = s;
		}
	}
	return b;
}

char *
symbol2string(mvc *sql, symbol *se, int expression, char **err) /**/
{
	switch (se->token) {
	case SQL_NOP: {
		dnode *lst = se->data.lval->h, *ops = lst->next->next->data.lval->h, *aux;
		const char *op = qname_fname(lst->data.lval), *sname = qname_schema(lst->data.lval);
		int i = 0, nargs = 0;
		char** inputs = NULL, *res;
		size_t inputs_length = 0;

		if (!sname)
			sname = sql->session->schema->base.name;

		for (aux = ops; aux; aux = aux->next) nargs++;
		if (!(inputs = GDKzalloc(nargs * sizeof(char**))))
			return NULL;

		for (aux = ops; aux; aux = aux->next) {
			if (!(inputs[i] = symbol2string(sql, aux->data.sym, expression, err))) {
				for (int j = 0; j < i; j++)
					GDKfree(inputs[j]);
				GDKfree(inputs);
				return NULL;
			}
			inputs_length += strlen(inputs[i]);
			i++;
		}

		if ((res = NEW_ARRAY(char, strlen(sname) + strlen(op) + inputs_length + 6 + (nargs - 1 /* commas */) + 2))) {
			char *concat = stpcpy(stpcpy(stpcpy(stpcpy(stpcpy(res, "\""), sname), "\".\""), op), "\"(");
			i = 0;
			for (aux = ops; aux; aux = aux->next) {
				concat = stpcpy(concat, inputs[i]);
				if (aux->next)
					concat = stpcpy(concat, ",");
				i++;
			}
			concat = stpcpy(concat, ")");
		}

		for (int j = 0; j < nargs; j++)
			GDKfree(inputs[j]);
		GDKfree(inputs);
		return res;
	} break;
	case SQL_BINOP: {
		dnode *lst = se->data.lval->h;
		const char *op = qname_fname(lst->data.lval), *sname = qname_schema(lst->data.lval);
		char *l = NULL, *r = NULL, *res;

		if (!sname)
			sname = sql->session->schema->base.name;
		if (!(l = symbol2string(sql, lst->next->next->data.sym, expression, err)) || !(r = symbol2string(sql, lst->next->next->next->data.sym, expression, err))) {
			_DELETE(l);
			_DELETE(r);
			return NULL;
		}

		if ((res = NEW_ARRAY(char, strlen(sname) + strlen(op) + strlen(l) + strlen(r) + 9)))
			stpcpy(stpcpy(stpcpy(stpcpy(stpcpy(stpcpy(stpcpy(stpcpy(stpcpy(res, "\""), sname), "\".\""), op), "\"("), l), ","), r), ")");

		_DELETE(l);
		_DELETE(r);
		return res;
	} break;
	case SQL_OP: {
		dnode *lst = se->data.lval->h;
		const char *op = qname_fname(lst->data.lval), *sname = qname_schema(lst->data.lval);
		char *res;

		if (!sname)
			sname = sql->session->schema->base.name;

		if ((res = NEW_ARRAY(char, strlen(sname) + strlen(op) + 8)))
			stpcpy(stpcpy(stpcpy(stpcpy(stpcpy(res, "\""), sname), "\".\""), op), "\"()");

		return res;
	} break;
	case SQL_UNOP: {
		dnode *lst = se->data.lval->h;
		const char *op = qname_fname(lst->data.lval), *sname = qname_schema(lst->data.lval);
		char *l = symbol2string(sql, lst->next->next->data.sym, expression, err), *res;

		if (!sname)
			sname = sql->session->schema->base.name;
		if (!l)
			return NULL;

		if ((res = NEW_ARRAY(char, strlen(sname) + strlen(op) + strlen(l) + 8)))
			stpcpy(stpcpy(stpcpy(stpcpy(stpcpy(stpcpy(stpcpy(res, "\""), sname), "\".\""), op), "\"("), l), ")");

		_DELETE(l);
		return res;
	}
	case SQL_PARAMETER:
		return _STRDUP("?");
	case SQL_NULL:
		return _STRDUP("NULL");
	case SQL_ATOM:{
		AtomNode *an = (AtomNode *) se;
		if (an && an->a)
			return atom2sql(an->a, sql->timezone);
		else
			return _STRDUP("NULL");
	}
	case SQL_NEXT: {
		const char *seq = qname_table(se->data.lval), *sname = qname_schema(se->data.lval);
		char *res;

		if (!sname)
			sname = sql->session->schema->base.name;

		if ((res = NEW_ARRAY(char, strlen("next value for \"") + strlen(sname) + strlen(seq) + 5)))
			stpcpy(stpcpy(stpcpy(stpcpy(stpcpy(res, "next value for \""), sname), "\".\""), seq), "\"");
		return res;
	}	break;
	case SQL_IDENT:
	case SQL_COLUMN: {
		/* can only be variables */
		dlist *l = se->data.lval;
		assert(l->h->type != type_lng);
		if (dlist_length(l) == 1 && l->h->type == type_int) {
			atom *a = sql_bind_arg(sql, l->h->data.i_val);
			return atom2sql(a, sql->timezone);
		} else if (expression && dlist_length(l) == 1 && l->h->type == type_string) {
			/* when compiling an expression, a column of a table might be present in the symbol, so we need this case */
			const char *op = l->h->data.sval;
			char *res;

			if ((res = NEW_ARRAY(char, strlen(op) + 3)))
				stpcpy(stpcpy(stpcpy(res, "\""), op), "\"");
			return res;
		} else if (expression && dlist_length(l) == 2 && l->h->type == type_string && l->h->next->type == type_string) {
			char *first = l->h->data.sval, *second = l->h->next->data.sval, *res;

			if (!first || !second)
				return NULL;
			if ((res = NEW_ARRAY(char, strlen(first) + strlen(second) + 6)))
				stpcpy(stpcpy(stpcpy(stpcpy(stpcpy(res, "\""), first), "\".\""), second), "\"");
			return res;
		} else {
			char *e = dlist2string(sql, l, expression, err);
			if (e)
				*err = e;
			return NULL;
		}
	}
	case SQL_CAST: {
		dlist *dl = se->data.lval;
		char *val = NULL, *tpe = NULL, *res;

		if (!(val = symbol2string(sql, dl->h->data.sym, expression, err)) || !(tpe = subtype2string2(&dl->h->next->data.typeval))) {
			_DELETE(val);
			_DELETE(tpe);
			return NULL;
		}
		if ((res = NEW_ARRAY(char, strlen(val) + strlen(tpe) + 11)))
			stpcpy(stpcpy(stpcpy(stpcpy(stpcpy(res, "cast("), val), " as "), tpe), ")");
		_DELETE(val);
		_DELETE(tpe);
		return res;
	}
	default: {
		const char *msg = "SQL feature not yet available for expressions and default values: ";
		char *tok_str = token2string(se->token);
		if ((*err = NEW_ARRAY(char, strlen(msg) + strlen(tok_str) + 1)))
			stpcpy(stpcpy(*err, msg), tok_str);
	}
	}
	return NULL;
}
