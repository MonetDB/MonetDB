
#include "sql.h"
#include "symbol.h"
#include "context.h"
#include "mem.h"

static void SelectNode_destroy(SelectNode *s);
static void AtomNode_destroy(AtomNode *a);

int symbol_debug = 0;

symbol *symbol_init(symbol *s, context * lc, int token)
{
	s->token = token;
	s->type = type_symbol;
	s->data.lval = NULL;

	s->filename = lc->filename;
	s->lineno = lc->lineno;
	s->sql = _strdup(lc->sql);
	return s;
}

symbol *symbol_create(context * lc, int token, char *data)
{
	symbol *s = NEW(symbol);
	symbol_init(s, lc, token);
	s->data.sval = data;
	s->type = type_string;
	if (symbol_debug)
		fprintf(stderr, "%ld = symbol_create_string(%s,%s)\n",
			(long) s, token2string(s->token), s->data.sval);
	return s;
}

symbol *symbol_create_list(context * lc, int token, dlist * data)
{
	symbol *s = NEW(symbol);
	symbol_init(s, lc, token);
	s->data.lval = data;
	s->type = type_list;
	if (symbol_debug)
		fprintf(stderr, "%ld = symbol_create_list(%s,%ld)\n",
			(long) s, token2string(s->token),
			(long) s->data.lval);
	return s;
}

symbol *symbol_create_int(context * lc, int token, int data)
{
	symbol *s = NEW(symbol);
	symbol_init(s, lc, token);
	s->data.ival = data;
	s->type = type_int;
	if (symbol_debug)
		fprintf(stderr, "%ld = symbol_create_int(%s,%d)\n",
			(long) s, token2string(s->token), data);
	return s;
}

symbol *symbol_create_symbol(context * lc, int token, symbol * data)
{
	symbol *s = NEW(symbol);
	symbol_init(s, lc, token);
	s->data.sym = data;
	if (symbol_debug)
		fprintf(stderr, "%ld = symbol_create_symbol(%s,%s)\n",
			(long) s, token2string(s->token),
			token2string(data->token));
	return s;
}

void symbol_destroy(symbol * s)
{
	if (symbol_debug)
		fprintf(stderr, "%ld = symbol_destroy(%s)\n",
			(long) s, token2string(s->token) );

	if (s) {
		switch (s->type) {
		case type_symbol:
			switch(s->token){
			case SQL_SELECT:
				SelectNode_destroy((SelectNode*)s);
				break;
			case SQL_ATOM:
				AtomNode_destroy((AtomNode*)s);
				break;
			default:
				if (s->data.sym)
					symbol_destroy(s->data.sym);
			}
			break;
		case type_list:
			if (s->data.lval) 
				dlist_destroy(s->data.lval);
			break;
		case type_string:
		case type_type:
			if (s->data.sval) 
				_DELETE(s->data.sval);
			break;
		case type_int:
			/* not used types */
		case type_stmt:
		case type_column:
		case type_table:
		case type_schema:
		case type_aggr:
		case type_func:
			break;
		}
		_DELETE(s->sql);
		_DELETE(s);
	}
}

void dnode_destroy(dnode * s)
{
	if (s->data.sval) {
		switch (s->type) {
		case type_symbol:
			symbol_destroy(s->data.sym);
			break;
		case type_list:
			dlist_destroy(s->data.lval);
			break;
		case type_type:
		case type_string:
			_DELETE(s->data.sval);
			break;
		case type_int:
			/* not used types */
		case type_stmt:
		case type_column:
		case type_table:
		case type_aggr:
		case type_func:
			break;
		}
	}
	_DELETE(s);
}

static dnode *dnode_create()
{
	dnode *n = NEW(dnode);
	n->next = NULL;
	n->data.sval = NULL;
	n->type = type_symbol;
	return n;
}

static dnode *dnode_create_string(char *data)
{
	dnode *n = dnode_create();
	n->data.sval = data;
	n->type = type_string;
	return n;
}
static dnode *dnode_create_list(dlist * data)
{
	dnode *n = dnode_create();
	n->data.lval = data;
	n->type = type_list;
	return n;
}
static dnode *dnode_create_int(int data)
{
	dnode *n = dnode_create();
	n->data.ival = data;
	n->type = type_int;
	return n;
}
static dnode *dnode_create_symbol(symbol * data)
{
	dnode *n = dnode_create();
	n->data.sym = data;
	n->type = type_symbol;
	return n;
}

static dnode *dnode_create_type(sql_subtype * data)
{
	dnode *n = dnode_create();
	n->data.typeval = data;
	n->type = type_type;
	return n;
}

dlist *dlist_create()
{
	dlist *l = NEW(dlist);
	l->h = l->t = NULL;
	l->cnt = 0;
	return l;
}

void dlist_destroy(dlist * l)
{
	if (l) {
		dnode *n = l->h;
		while (n) {
			dnode *t = n;
			n = n->next;
			dnode_destroy(t);
		}
		_DELETE(l);
	}
}

void dlist_destroy_keep_data(dlist * l)
{
	if (l) {
		dnode *n = l->h;
		while (n) {
			dnode *t = n;
			n = n->next;
			_DELETE(t);
		}
		_DELETE(l);
	}
}

int dlist_length(dlist * l)
{
	return l->cnt;
}

dlist *dlist_append_default(dlist * l, dnode * n)
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

dlist *dlist_append_string(dlist * l, char *data)
{
	dnode *n = dnode_create_string(data);
	return dlist_append_default(l, n);
}

dlist *dlist_append_list(dlist * l, dlist * data)
{
	dnode *n = dnode_create_list(data);
	return dlist_append_default(l, n);
}

dlist *dlist_append_int(dlist * l, int data)
{
	dnode *n = dnode_create_int(data);
	return dlist_append_default(l, n);
}

dlist *dlist_append_symbol(dlist * l, symbol * data)
{
	dnode *n = dnode_create_symbol(data);
	return dlist_append_default(l, n);
}

dlist *dlist_append_type(dlist * l, sql_subtype * data)
{
	dnode *n = dnode_create_type(data);
	return dlist_append_default(l, n);
}

symbol *newSelectNode( context *c,
	int distinct, 
	struct dlist *selection, 
	struct dlist *into, 
	symbol *from, 
	symbol *where, 
	symbol *groupby, 
	symbol *having, 
	symbol *orderby, 
	symbol *name)
{
	symbol *s;
	SelectNode *sn = NEW(SelectNode);

	sn->distinct = distinct;
	sn->selection = selection;
	sn->into = into;
	sn->from = from;
	sn->where = where;
	sn->groupby = groupby;
	sn->having = having;
	sn->orderby = orderby;
	sn->name = name;
	s = (symbol*)sn;
	symbol_init(s, c, SQL_SELECT);
	return s;
}

static void SelectNode_destroy(SelectNode *s)
{
	if (s->selection) dlist_destroy(s->selection);
	if (s->into) dlist_destroy(s->into);
	if (s->from) symbol_destroy(s->from);
	if (s->where) symbol_destroy(s->where);
	if (s->groupby) symbol_destroy(s->groupby);
	if (s->having) symbol_destroy(s->having);
	if (s->orderby) symbol_destroy(s->orderby);
	if (s->name) _DELETE(s->name);
}

symbol *newAtomNode(context *c, atom * data)
{
	symbol *s;
	AtomNode *an = NEW(AtomNode);

	an->a = data;
	s = (symbol*)an;
	symbol_init(s, c, SQL_ATOM);
	return s;
}

static void AtomNode_destroy(AtomNode *a)
{
	if (a->a)
		atom_destroy(a->a);
}
