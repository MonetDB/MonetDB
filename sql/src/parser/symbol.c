
#include "mem.h"
#include "sql.h"
#include "symbol.h"
#include "context.h"

int symbol_debug = 0;

symbol *symbol_create_default( context *lc, int token ){
	symbol *s = NEW(symbol);
	s->lexion = NULL;
	s->token = token;
	s->data.lval = NULL;
	s->type = type_list;

	s->filename = lc->filename;
	s->lineno = lc->lineno;
	s->sql = _strdup(lc->sql);
	return s;
}

symbol *symbol_create( context *lc, int token, char *data){
	symbol *s = symbol_create_default( lc, token );
	s->data.sval = data;
	s->type = type_string;
	if (symbol_debug)
		fprintf(stderr, "%d = symbol_create_string(%s,%s)\n", 
		(int)s, token2string(s->token), s->data.sval);
	return s;
}
symbol *symbol_create_list( context *lc, int token, dlist *data){
	symbol *s = symbol_create_default( lc, token );
	s->data.lval = data;
	s->type = type_list;
	if (symbol_debug)
		fprintf(stderr, "%d = symbol_create_list(%s,%d)\n", 
		(int)s, token2string(s->token), (int)s->data.lval);
	return s;
}
symbol *symbol_create_int( context *lc, int token, int data){
	symbol *s = symbol_create_default( lc, token );
	s->data.ival = data;
	s->type = type_int;
	if (symbol_debug)
		fprintf(stderr, "%d = symbol_create_int(%s,%d)\n", 
		(int)s, token2string(s->token), data);
	return s;
}
symbol *symbol_create_symbol( context *lc, int token, symbol *data){
	symbol *s = symbol_create_default( lc, token );
	s->data.sym = data;
	s->type = type_symbol;
	if (symbol_debug)
		fprintf(stderr, "%d = symbol_create_symbol(%s,%s)\n", 
		(int)s, token2string(s->token), token2string(data->token));
	return s;
}
symbol *symbol_create_atom( context *lc, int token, atom *data){
	symbol *s = symbol_create_default( lc, token );
	s->data.aval = data;
	s->type = type_atom;
	if (symbol_debug)
		fprintf(stderr, "%d = symbol_create_atom(%s,%s)\n", 
		(int)s, token2string(s->token), atom2string(data) );
	return s;
}

void symbol_destroy( symbol *s ){
	if (s->data.sval){
		switch (s->type){
		case type_atom: atom_destroy( s->data.aval ); break;
		case type_symbol: symbol_destroy( s->data.sym ); break;
		case type_list: dlist_destroy( s->data.lval ); break;
		case type_string: _DELETE( s->data.sval ); break;
		case type_int: 
		/* not used types */
		case type_statement: 
		case type_column: 
		case type_table: 
		case type_aggr: 
		case type_func: 
		case type_type: 
				 break;
		}
	}
	if (s){
		_DELETE(s->sql);
		_DELETE(s);
	}
}

void dnode_destroy( dnode *s ){
	if (s->data.sval){
		switch (s->type){
		case type_atom: atom_destroy( s->data.aval ); break;
		case type_symbol: symbol_destroy( s->data.sym ); break;
		case type_list: dlist_destroy( s->data.lval ); break;
		case type_string: _DELETE( s->data.sval ); break;
		case type_int: 
		/* not used types */
		case type_statement: 
		case type_column: 
		case type_table: 
		case type_aggr: 
		case type_func: 
		case type_type: 
				 break;
		}
	}
	_DELETE(s);
}

static dnode *dnode_create(){
	dnode *n = NEW(dnode); 
	n->next = NULL;
	n->data.sval = NULL;
	n->type = type_list;
	return n;
}

static dnode *dnode_create_string( char *data ){
	dnode *n = dnode_create();
	n->data.sval = data;
	n->type = type_string;
	return n;
}
static dnode *dnode_create_list( dlist *data ){
	dnode *n = dnode_create();
	n->data.lval = data;
	n->type = type_list;
	return n;
}
static dnode *dnode_create_int( int data ){
	dnode *n = dnode_create();
	n->data.ival = data;
	n->type = type_int;
	return n;
}
static dnode *dnode_create_symbol( symbol *data ){
	dnode *n = dnode_create();
	n->data.sym = data;
	n->type = type_symbol;
	return n;
}
static dnode *dnode_create_atom( atom *data ){
	dnode *n = dnode_create();
	n->data.aval = data;
	n->type = type_atom;
	return n;
}

dlist *dlist_create(){
	dlist *l = NEW(dlist); 
	l->h = l->t = NULL;
	l->cnt = 0;
	return l;
}

void dlist_destroy(dlist *l){
	if (l){
	    dnode *n = l->h;
	    while(n){
		dnode *t = n;
		n = n->next;
		dnode_destroy(t);
	    }
	    _DELETE(l);
	}
}

int dlist_length(dlist *l){
	return l->cnt;
}

dlist *dlist_append_default(dlist *l, dnode *n){
	if (l->cnt){
		l->t->next = n;
	} else {
		l->h = n;
	}
	l->t = n;
	l->cnt++;
	return l;
}
dlist *dlist_append_string(dlist *l, char *data){
	dnode *n = dnode_create_string(data);
	return dlist_append_default(l,n);
}
dlist *dlist_append_list(dlist *l, dlist *data){
	dnode *n = dnode_create_list(data);
	return dlist_append_default(l,n);
}
dlist *dlist_append_int(dlist *l, int data){
	dnode *n = dnode_create_int(data);
	return dlist_append_default(l,n);
}
dlist *dlist_append_symbol(dlist *l, symbol *data){
	dnode *n = dnode_create_symbol(data);
	return dlist_append_default(l,n);
}
dlist *dlist_append_atom(dlist *l, atom *data){
	dnode *n = dnode_create_atom(data);
	printf("dlist_append_atom\n");
	return dlist_append_default(l,n);
}
