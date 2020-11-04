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

void
sql_destroy_params(mvc *sql)
{
	sql->params = NULL;
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

#define table_extra \
	do { \
		if (!res && strcmp(objstr, "table") == 0 && (res = stack_find_table(sql, name))) /* for tables, first try a declared table from the stack */ \
			return res; \
	} while (0)

sql_table *
find_table_or_view_on_scope(mvc *sql, sql_schema **s, const char *sname, const char *name, const char *error, bool isView)
{
	const char *objstr = isView ? "view" : "table";
	sql_table *res = NULL;

	search_object_on_path(res = mvc_bind_table(sql, found, name), table_extra, SQLSTATE(42S02));
	return res;
}

sql_sequence *
find_sequence_on_scope(mvc *sql, sql_schema **s, const char *sname, const char *name, const char *error)
{
	const char *objstr = "sequence";
	sql_sequence *res = NULL;

	search_object_on_path(res = find_sql_sequence(found, name), ;, SQLSTATE(42000));
	return res;
}

sql_idx *
find_idx_on_scope(mvc *sql, sql_schema **s, const char *sname, const char *name, const char *error)
{
	const char *objstr = "index";
	sql_idx *res = NULL;

	search_object_on_path(res = mvc_bind_idx(sql, found, name), ;, SQLSTATE(42S12));
	return res;
}

sql_type *
find_type_on_scope(mvc *sql, sql_schema **s, const char *sname, const char *name, const char *error)
{
	const char *objstr = "type";
	sql_type *res = NULL;

	search_object_on_path(res = schema_bind_type(sql, found, name), ;, SQLSTATE(42S01));
	return res;
}

sql_trigger *
find_trigger_on_scope(mvc *sql, sql_schema **s, const char *sname, const char *name, const char *error)
{
	const char *objstr = "trigger";
	sql_trigger *res = NULL;

	search_object_on_path(res = mvc_bind_trigger(sql, found, name), ;, SQLSTATE(42000));
	return res;
}

/* A variable can be any of the following, from the innermost to the outermost:
	- 'parameter of the function' (ie in the param list)
	- local variable, declared earlier
	- global variable, also declared earlier
*/
#define variable_extra \
	do { \
		if (!res) { \
			if ((*var = stack_find_var_frame(sql, name, level))) { /* check if variable is known from the stack */ \
				*tpe = &((*var)->var.tpe); \
				res = true; \
			} else if ((*a = sql_bind_param(sql, name))) { /* then if it is a parameter */ \
				*tpe = &((*a)->type); \
				*level = 1; \
				res = true; \
			} \
		} \
	} while (0)

#define var_find_on_global \
	do { \
		if ((*var = find_global_var(sql, found, name))) { /* then if it is a global var */ \
			*tpe = &((*var)->var.tpe); \
			*level = 0; \
			res = true; \
		} \
	} while (0)

bool
find_variable_on_scope(mvc *sql, sql_schema **s, const char *sname, const char *name, sql_var **var, sql_arg **a, sql_subtype **tpe, int *level, const char *error)
{
	const char *objstr = "variable";
	bool res = false;

	search_object_on_path(var_find_on_global, variable_extra, SQLSTATE(42000));
	return res;
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
qname_schema_object(dlist *qname)
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

/*
 * Find the result_datatype for certain combinations of values
 * (like case expressions or coumns in a result of a query expression).
 * See standaard pages 505-507 Result of data type combinations */
sql_subtype *
result_datatype(sql_subtype *super, sql_subtype *l, sql_subtype *r)
{
	int lclass = l->type->eclass, rclass = r->type->eclass;
	int lc=0, rc=0;

	/* case a strings */
	if (EC_VARCHAR(lclass) || EC_VARCHAR(rclass)) {
		char *tpe = "varchar";
		int digits = 0;
		if (!EC_VARCHAR(lclass)) {
				tpe = r->type->sqlname;
				digits = (!l->digits)?0:r->digits;
		} else if (!EC_VARCHAR(rclass)) {
				tpe = l->type->sqlname;
				digits = (!r->digits)?0:l->digits;
		} else { /* both */
				tpe = (l->type->base.id > r->type->base.id)?l->type->sqlname:r->type->sqlname;
				digits = (!l->digits||!r->digits)?0:sql_max(l->digits, r->digits);
		}
		sql_find_subtype(super, tpe, digits, 0);
	/* case b blob */
	} else if ((lc=strcmp(l->type->sqlname, "blob")) == 0 || (rc=strcmp(r->type->sqlname, "blob")) == 0) {
		if (!lc)
			*super = *l;
		else
			*super = *r;
	/* case c all exact numeric */
	} else if (EC_EXACTNUM(lclass) && EC_EXACTNUM(rclass)) {
		char *tpe = (l->type->base.id > r->type->base.id)?l->type->sqlname:r->type->sqlname;
		unsigned int digits = sql_max(l->digits, r->digits);
		int scale = sql_max(l->scale, r->scale);
		if (l->type->radix == 10 || r->type->radix == 10) {
			digits = 0;
			/* change to radix 10 */
			if (l->type->radix == 2 && r->type->radix == 10) {
				digits = bits2digits(l->type->digits);
				digits = sql_max(r->digits, digits);
				scale = r->scale;
			} else if (l->type->radix == 10 && r->type->radix == 2) {
				digits = bits2digits(r->type->digits);
				digits = sql_max(l->digits, digits);
				scale = l->scale;
			}
		}
		sql_find_subtype(super, tpe, digits, scale);
	/* case d approximate numeric */
	} else if (EC_APPNUM(lclass) || EC_APPNUM(rclass)) {
		if (!EC_APPNUM(lclass)) {
				*super = *r;
		} else if (!EC_APPNUM(rclass)) {
				*super = *l;
		} else { /* both */
				char *tpe = (l->type->base.id > r->type->base.id)?l->type->sqlname:r->type->sqlname;
				int digits = sql_max(l->digits, r->digits); /* bits precision */
				sql_find_subtype(super, tpe, digits, 0);
		}
	/* now its getting serious, ie e any 'case e' datetime data type */
	/* 'case f' interval types */
	/* 'case g' boolean */
	/* 'case h-l' compounds like row (tuple), etc */
	} else {
		return supertype(super, l, r);
	}
	return super;
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

static char * _symbol2string(mvc *sql, symbol *se, int expression, char **err);

static char *
dlist2string(mvc *sql, dlist *l, int expression, char **err)
{
	char *b = NULL;
	dnode *n;

	for (n=l->h; n; n = n->next) {
		char *s = NULL;

		if (n->type == type_string && n->data.sval)
			s = sa_strdup(sql->ta, n->data.sval);
		else if (n->type == type_symbol)
			s = _symbol2string(sql, n->data.sym, expression, err);

		if (!s)
			return NULL;
		if (b) {
			char *o = SA_NEW_ARRAY(sql->ta, char, strlen(b) + strlen(s) + 2);
			if (o)
				stpcpy(stpcpy(stpcpy(o, b), "."), s);
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
_symbol2string(mvc *sql, symbol *se, int expression, char **err) /**/
{
	/* inner symbol2string uses the temporary allocator */
	switch (se->token) {
	case SQL_NOP: {
		dnode *lst = se->data.lval->h, *ops = lst->next->next->data.lval->h, *aux;
		const char *op = qname_schema_object(lst->data.lval), *sname = qname_schema(lst->data.lval);
		int i = 0, nargs = 0;
		char** inputs = NULL, *res;
		size_t inputs_length = 0;

		if (!sname)
			sname = sql->session->schema->base.name;

		for (aux = ops; aux; aux = aux->next)
			nargs++;
		if (!(inputs = SA_ZNEW_ARRAY(sql->ta, char*, nargs)))
			return NULL;

		for (aux = ops; aux; aux = aux->next) {
			if (!(inputs[i] = _symbol2string(sql, aux->data.sym, expression, err))) {
				return NULL;
			}
			inputs_length += strlen(inputs[i]);
			i++;
		}

		if ((res = SA_NEW_ARRAY(sql->ta, char, strlen(sname) + strlen(op) + inputs_length + 6 + (nargs - 1 /* commas */) + 2))) {
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
		return res;
	} break;
	case SQL_BINOP: {
		dnode *lst = se->data.lval->h;
		const char *op = qname_schema_object(lst->data.lval), *sname = qname_schema(lst->data.lval);
		char *l = NULL, *r = NULL, *res;

		if (!sname)
			sname = sql->session->schema->base.name;
		if (!(l = _symbol2string(sql, lst->next->next->data.sym, expression, err)) || !(r = _symbol2string(sql, lst->next->next->next->data.sym, expression, err)))
			return NULL;

		if ((res = SA_NEW_ARRAY(sql->ta, char, strlen(sname) + strlen(op) + strlen(l) + strlen(r) + 9)))
			stpcpy(stpcpy(stpcpy(stpcpy(stpcpy(stpcpy(stpcpy(stpcpy(stpcpy(res, "\""), sname), "\".\""), op), "\"("), l), ","), r), ")");

		return res;
	} break;
	case SQL_OP: {
		dnode *lst = se->data.lval->h;
		const char *op = qname_schema_object(lst->data.lval), *sname = qname_schema(lst->data.lval);
		char *res;

		if (!sname)
			sname = sql->session->schema->base.name;

		if ((res = SA_NEW_ARRAY(sql->ta, char, strlen(sname) + strlen(op) + 8)))
			stpcpy(stpcpy(stpcpy(stpcpy(stpcpy(res, "\""), sname), "\".\""), op), "\"()");

		return res;
	} break;
	case SQL_UNOP: {
		dnode *lst = se->data.lval->h;
		const char *op = qname_schema_object(lst->data.lval), *sname = qname_schema(lst->data.lval);
		char *l = _symbol2string(sql, lst->next->next->data.sym, expression, err), *res;

		if (!sname)
			sname = sql->session->schema->base.name;
		if (!l)
			return NULL;

		if ((res = SA_NEW_ARRAY(sql->ta, char, strlen(sname) + strlen(op) + strlen(l) + 8)))
			stpcpy(stpcpy(stpcpy(stpcpy(stpcpy(stpcpy(stpcpy(res, "\""), sname), "\".\""), op), "\"("), l), ")");

		return res;
	}
	case SQL_PARAMETER:
		return sa_strdup(sql->ta, "?");
	case SQL_NULL:
		return sa_strdup(sql->ta, "NULL");
	case SQL_ATOM:{
		AtomNode *an = (AtomNode *) se;
		if (an && an->a)
			return atom2sql(sql->ta, an->a, sql->timezone);
		else
			return sa_strdup(sql->ta, "NULL");
	}
	case SQL_NEXT: {
		const char *seq = qname_schema_object(se->data.lval), *sname = qname_schema(se->data.lval);
		char *res;

		if (!sname)
			sname = sql->session->schema->base.name;

		if ((res = SA_NEW_ARRAY(sql->ta, char, strlen("next value for \"") + strlen(sname) + strlen(seq) + 5)))
			stpcpy(stpcpy(stpcpy(stpcpy(stpcpy(res, "next value for \""), sname), "\".\""), seq), "\"");
		return res;
	}	break;
	case SQL_IDENT:
	case SQL_COLUMN: {
		/* can only be variables */
		dlist *l = se->data.lval;
		assert(l->h->type != type_lng);
		if (expression && dlist_length(l) == 1 && l->h->type == type_string) {
			/* when compiling an expression, a column of a table might be present in the symbol, so we need this case */
			const char *op = l->h->data.sval;
			char *res;

			if ((res = SA_NEW_ARRAY(sql->ta, char, strlen(op) + 3)))
				stpcpy(stpcpy(stpcpy(res, "\""), op), "\"");
			return res;
		} else if (expression && dlist_length(l) == 2 && l->h->type == type_string && l->h->next->type == type_string) {
			char *first = l->h->data.sval, *second = l->h->next->data.sval, *res;

			if (!first || !second)
				return NULL;
			if ((res = SA_NEW_ARRAY(sql->ta, char, strlen(first) + strlen(second) + 6)))
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

		if (!(val = _symbol2string(sql, dl->h->data.sym, expression, err)) || !(tpe = subtype2string2(sql->ta, &dl->h->next->data.typeval))) {
			return NULL;
		}
		if ((res = SA_NEW_ARRAY(sql->ta, char, strlen(val) + strlen(tpe) + 11)))
			stpcpy(stpcpy(stpcpy(stpcpy(stpcpy(res, "cast("), val), " as "), tpe), ")");
		return res;
	}
	default: {
		const char *msg = "SQL feature not yet available for expressions and default values: ";
		char *tok_str = token2string(se->token);
		if ((*err = SA_NEW_ARRAY(sql->ta, char, strlen(msg) + strlen(tok_str) + 1)))
			stpcpy(stpcpy(*err, msg), tok_str);
	}
	}
	return NULL;
}

char *
symbol2string(mvc *sql, symbol *se, int expression, char **err)
{
	char *res = _symbol2string(sql, se, expression, err);

	if (res)
		res = sa_strdup(sql->sa, res);
	if (*err)
		*err = sa_strdup(sql->sa, *err);
	sa_reset(sql->ta);
	return res;
}
