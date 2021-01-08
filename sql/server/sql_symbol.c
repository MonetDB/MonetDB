/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0.  If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 *
 * Copyright 1997 - July 2008 CWI, August 2008 - 2021 MonetDB B.V.
 */

#include "monetdb_config.h"
#include "sql_mem.h"
#include "sql_symbol.h"
#include "sql_parser.h"

static symbol *
symbol_init(symbol *s, tokens token, symtype type )
{
	s->token = token;
	s->type = type;
	return s;
}

symbol *
symbol_create(sql_allocator *sa, tokens token, char *data)
{
	symbol *s = SA_NEW(sa, symbol);

	if (s) {
		symbol_init(s, token, type_string);
		s->data.sval = data;
	}
	return s;
}

symbol *
symbol_create_list(sql_allocator *sa, tokens token, dlist *data)
{
	symbol *s = SA_NEW(sa, symbol);

	if (s) {
		symbol_init(s, token, type_list);
		s->data.lval = data;
	}
	return s;
}

symbol *
symbol_create_int(sql_allocator *sa, tokens token, int data)
{
	symbol *s = SA_NEW(sa, symbol);

	if (s) {
		symbol_init(s, token, type_int);
		s->data.i_val = data;
	}
	return s;
}

symbol *
symbol_create_lng(sql_allocator *sa, tokens token, lng data)
{
	symbol *s = SA_NEW(sa, symbol);

	if (s) {
		symbol_init(s, token, type_lng);
		s->data.l_val = data;
	}
	return s;
}

symbol *
symbol_create_symbol(sql_allocator *sa, tokens token, symbol *data)
{
	symbol *s = SA_NEW(sa, symbol);

	if (s) {
		symbol_init(s, token, type_symbol);
		s->data.sym = data;
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
newSelectNode(sql_allocator *sa, int distinct, struct dlist *selection, struct dlist *into, symbol *from, symbol *where, symbol *groupby, symbol *having, symbol *orderby, symbol *name, symbol *limit, symbol *offset, symbol *sample, symbol *seed, symbol *window)
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
		sn->seed = seed;
		sn->selection = selection;
		sn->into = into;
		sn->from = from;
		sn->where = where;
		sn->groupby = groupby;
		sn->having = having;
		sn->orderby = orderby;
		sn->name = name;
		sn->window = window;
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
