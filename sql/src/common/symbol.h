#ifndef SYMBOL_H
#define SYMBOL_H

#include "sym.h"
#include "atom.h"
#include "context.h"

typedef union symbdata {
	int    ival;
	char   *sval;
	struct atom   *aval;
	struct dlist   *lval;
	struct symbol *sym;
} symbdata;

typedef struct dnode {
	struct dnode *next;
	symbdata data;
	symtype type;
} dnode;

typedef struct dlist {
	dnode *h;
	dnode *t;
	int cnt;
} dlist;

extern dlist *dlist_create();
extern void dlist_destroy(dlist *l);
extern int dlist_length(dlist *l);

extern dlist *dlist_append_string(dlist *l, char *data);
extern dlist *dlist_append_list(dlist *l, dlist *data);
extern dlist *dlist_append_int(dlist *l, int data);
extern dlist *dlist_append_symbol(dlist *l, struct symbol *data);
extern dlist *dlist_append_atom(dlist *l, struct atom *data);

typedef struct symbol {
	int 		token;
	char*		lexion;
	symtype 	type;
	symbdata	data;

	int 		lineno;
	char*		filename;
	char*		sql;
} symbol;

extern symbol *symbol_create( struct context *c, int token, char *data);

extern symbol *symbol_create_list( struct context *c, int token, dlist *data);
extern symbol *symbol_create_int( struct context *c, int token, int data);
extern symbol *symbol_create_symbol( struct context *c, int token, symbol *data);
extern symbol *symbol_create_atom( struct context *c, int token, atom *data);

#endif /*SYMBOL_H*/
