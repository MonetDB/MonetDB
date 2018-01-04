/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0.  If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 *
 * Copyright 1997 - July 2008 CWI, August 2008 - 2018 MonetDB B.V.
 */

#include "monetdb_config.h"
#include "sql_mem.h"
#include "sql_symbol.h"
#include "sql_parser.h"

int symbol_debug = 0;

static symbol *
symbol_init(symbol *s, int token, symtype type )
{
	s->token = token;
	s->type = type;
	return s;
}

symbol *
symbol_create(sql_allocator *sa, int token, char *data)
{
	symbol *s = SA_NEW(sa, symbol);

	if (s) {
		symbol_init(s, token, type_string);
		s->data.sval = data;
		if (symbol_debug)
			fprintf(stderr, "" PTRFMT " = symbol_create_string(%s,%s)\n", PTRFMTCAST s, token2string(s->token), s->data.sval);
	}
	return s;
}

symbol *
symbol_create_list(sql_allocator *sa, int token, dlist *data)
{
	symbol *s = SA_NEW(sa, symbol);

	if (s) {
		symbol_init(s, token, type_list);
		s->data.lval = data;
		if (symbol_debug)
			fprintf(stderr, "" PTRFMT " = symbol_create_list(%s," PTRFMT ")\n", PTRFMTCAST s, token2string(s->token), PTRFMTCAST s->data.lval);
	}
	return s;
}

symbol *
symbol_create_int(sql_allocator *sa, int token, int data)
{
	symbol *s = SA_NEW(sa, symbol);

	if (s) {
		symbol_init(s, token, type_int);
		s->data.i_val = data;
		if (symbol_debug)
			fprintf(stderr, "" PTRFMT " = symbol_create_int(%s,%d)\n", PTRFMTCAST s, token2string(s->token), data);
	}
	return s;
}

symbol *
symbol_create_lng(sql_allocator *sa, int token, lng data)
{
	symbol *s = SA_NEW(sa, symbol);

	if (s) {
		symbol_init(s, token, type_lng);
		s->data.l_val = data;
		if (symbol_debug)
			fprintf(stderr, "" PTRFMT " = symbol_create_lng(%s,"LLFMT")\n", PTRFMTCAST s, token2string(s->token), data);
	}
	return s;
}

symbol *
symbol_create_symbol(sql_allocator *sa, int token, symbol *data)
{
	symbol *s = SA_NEW(sa, symbol);

	if (s) {
		symbol_init(s, token, type_symbol);
		s->data.sym = data;
		if (symbol_debug)
			fprintf(stderr, "" PTRFMT " = symbol_create_symbol(%s,%s)\n", PTRFMTCAST s, token2string(s->token), token2string(data->token));
	}
	return s;
}

static dnode *
dnode_create(sql_allocator *sa )
{
	dnode *n = SA_NEW(sa, dnode);

	if (n) 
		n->next = NULL;
	return n;
}

static dnode *
dnode_create_string(sql_allocator *sa, const char *data)
{
	dnode *n = dnode_create(sa);

	if (n) {
		n->data.sval = (char*)data;
		n->type = type_string;
	}
	return n;
}
static dnode *
dnode_create_list(sql_allocator *sa, dlist *data)
{
	dnode *n = dnode_create(sa);

	if (n) {
		n->data.lval = data;
		n->type = type_list;
	}
	return n;
}
static dnode *
dnode_create_int(sql_allocator *sa, int data)
{
	dnode *n = dnode_create(sa);

	if (n) {
		n->data.i_val = data;
		n->type = type_int;
	}
	return n;
}
static dnode *
dnode_create_lng(sql_allocator *sa, lng data)
{
	dnode *n = dnode_create(sa);

	if (n) {
		n->data.l_val = data;
		n->type = type_lng;
	}
	return n;
}
static dnode *
dnode_create_symbol(sql_allocator *sa, symbol *data)
{
	dnode *n = dnode_create(sa);

	if (n) {
		n->data.sym = data;
		n->type = type_symbol;
	}
	return n;
}

static dnode *
dnode_create_type(sql_allocator *sa, sql_subtype *data)
{
	dnode *n = dnode_create(sa);

	if (n) {
		if (data)
			n->data.typeval = *data;
		else
			n->data.typeval.type = NULL;
		n->type = type_type;
	}
	return n;
}

dlist *
dlist_create(sql_allocator *sa)
{
	dlist *l = SA_NEW(sa, dlist);

	if (l) {
		l->h = l->t = NULL;
		l->cnt = 0;
	}
	return l;
}

int
dlist_length(dlist *l)
{
	return l->cnt;
}

static dlist *
dlist_append_default(dlist *l, dnode *n)
{
	if (l->cnt) {
		l->t->next = n;
	} else {
		l->h = n;
	}
	l->t = n;
	l->cnt++;
	return l;
}

dlist *
dlist_append_string(sql_allocator *sa, dlist *l, const char *data)
{
	dnode *n = dnode_create_string(sa, data);
	
	if (!n)
		return NULL;
	return dlist_append_default(l, n);
}

dlist *
dlist_append_list(sql_allocator *sa, dlist *l, dlist *data)
{
	dnode *n = dnode_create_list(sa, data);

	if (!n)
		return NULL;
	return dlist_append_default(l, n);
}

dlist *
dlist_append_int(sql_allocator *sa, dlist *l, int data)
{
	dnode *n = dnode_create_int(sa, data);

	if (!n)
		return NULL;
	return dlist_append_default(l, n);
}

dlist *
dlist_append_lng(sql_allocator *sa, dlist *l, lng data)
{
	dnode *n = dnode_create_lng(sa, data);

	if (!n)
		return NULL;
	return dlist_append_default(l, n);
}

dlist *
dlist_append_symbol(sql_allocator *sa, dlist *l, symbol *data)
{
	dnode *n = dnode_create_symbol(sa, data);

	if (!n)
		return NULL;
	return dlist_append_default(l, n);
}

dlist *
dlist_append_type(sql_allocator *sa, dlist *l, sql_subtype *data)
{
	dnode *n = dnode_create_type(sa, data);

	if (!n)
		return NULL;
	return dlist_append_default(l, n);
}

symbol *
newSelectNode(sql_allocator *sa, int distinct, struct dlist *selection, struct dlist *into, symbol *from, symbol *where, symbol *groupby, symbol *having, symbol *orderby, symbol *name, symbol *limit, symbol *offset, symbol *sample)
{
	SelectNode *sn = SA_NEW(sa, SelectNode);
	symbol *s = (symbol *) sn;

	if (s) {
		symbol_init(s, SQL_SELECT, type_symbol);
		sn->distinct = distinct;
		sn->lateral = 0;
		sn->limit = limit;
		sn->offset = offset;
		sn->sample = sample;
		sn->selection = selection;
		sn->into = into;
		sn->from = from;
		sn->where = where;
		sn->groupby = groupby;
		sn->having = having;
		sn->orderby = orderby;
		sn->name = name;
	}
	return s;
}

symbol *
newAtomNode(sql_allocator *sa, atom *data)
{
	AtomNode *an = SA_NEW(sa, AtomNode);
	symbol *s = (symbol *) an;

	if (s) {
		symbol_init(s, SQL_ATOM, type_symbol);
		an->a = data;
	}
	return s;
}

static inline int dlist_cmp(dlist *l1, dlist *l2);

static inline int
dnode_cmp(dnode *d1, dnode *d2)
{
	if (d1 == d2)
		return 0;

	if (!d1 || !d2)
		return -1;

	if (d1->type == d2->type) {
		switch (d1->type) {
		case type_int:
			return (d1->data.i_val - d2->data.i_val);
		case type_lng: {
			lng c = d1->data.l_val - d2->data.l_val;
			assert((lng) GDK_int_min <= c && c <= (lng) GDK_int_max);
			return (int) c;
		}
		case type_string:
			if (d1->data.sval == d2->data.sval)
				return 0;
			if (!d1->data.sval || !d2->data.sval)
				return -1;
			return strcmp(d1->data.sval, d2->data.sval);
		case type_list:
			return dlist_cmp(d1->data.lval, d2->data.lval);
		case type_symbol:
			return symbol_cmp(d1->data.sym, d2->data.sym);
		case type_type:
			return subtype_cmp(&d1->data.typeval, &d2->data.typeval);
		default:
			assert(0);
		}
	}
	return -1;
}

static inline int
dlist_cmp(dlist *l1, dlist *l2)
{
	int res = 0;
	dnode *d1, *d2;

	if (l1 == l2)
		return 0;

	if (!l1 || !l2 || dlist_length(l1) != dlist_length(l2))
		return -1;

	for (d1 = l1->h, d2 = l2->h; !res && d1; d1 = d1->next, d2 = d2->next) {
		res = dnode_cmp(d1, d2);
	}
	return res;
}

static inline int
AtomNodeCmp(AtomNode *a1, AtomNode *a2)
{
	if (a1 == a2)
		return 0;
	if (!a1 || !a2)
		return -1;
	if (a1->a && a2->a)
		return atom_cmp(a1->a, a2->a);
	return -1;
}

static inline int
SelectNodeCmp(SelectNode *s1, SelectNode *s2)
{
	if (s1 == s2)
		return 0;
	if (!s1 || !s2)
		return -1;

	if (symbol_cmp(s1->limit, s2->limit) == 0 &&
			symbol_cmp(s1->offset, s2->offset) == 0 &&
			symbol_cmp(s1->sample, s2->sample) == 0 &&
			s1->distinct == s2->distinct &&
			s1->lateral == s2->lateral &&
			symbol_cmp(s1->name, s2->name) == 0 &&
			symbol_cmp(s1->orderby, s2->orderby) == 0 &&
			symbol_cmp(s1->having, s2->having) == 0 &&
			symbol_cmp(s1->groupby, s2->groupby) == 0 &&
			symbol_cmp(s1->where, s2->where) == 0 &&
			symbol_cmp(s1->from, s2->from) == 0 &&
			dlist_cmp(s1->selection, s2->selection) == 0)
		return 0;
	return -1;
}

static inline int
_symbol_cmp(symbol *s1, symbol *s2)
{
	if (s1 == s2)
		return 0;
	if (!s1 || !s2)
		return -1;
	if (s1->token != s2->token || s1->type != s2->type) 
		return -1;
	switch (s1->type) {
	case type_int:
		return (s1->data.i_val - s2->data.i_val);
	case type_lng: {
		lng c = s1->data.l_val - s2->data.l_val;
		assert((lng) GDK_int_min <= c && c <= (lng) GDK_int_max);
		return (int) c;
	}
	case type_string:
		if (s1->data.sval == s2->data.sval)
			return 0;
		if (!s1->data.sval || !s2->data.sval)
			return -1;
		return strcmp(s1->data.sval, s2->data.sval);
	case type_list:
		return dlist_cmp(s1->data.lval, s2->data.lval);
	case type_type:
		return subtype_cmp(&s1->data.typeval, &s2->data.typeval);
	case type_symbol:
		if (s1->token == SQL_SELECT) {
			return SelectNodeCmp((SelectNode *) s1, (SelectNode *) s2);
		} else if (s1->token != SQL_ATOM) {
			return symbol_cmp(s1->data.sym, s2->data.sym);
		} else {
			return AtomNodeCmp((AtomNode *) s1, (AtomNode *) s2);
		}
	default:
		assert(0);
	}
	return 0;		/* never reached, just to pacify compilers */
}

int
symbol_cmp(symbol *s1, symbol *s2)
{
	return _symbol_cmp(s1,s2);
}
