#ifndef SYMBOL_H
#define SYMBOL_H

#include "sym.h"
#include "atom.h"
#include "context.h"

typedef union symbdata {
	int ival;
	char *sval;
	struct dlist *lval;
	struct symbol *sym;
	struct sql_subtype *typeval;
	void *symv; /* temp version of symbol which can be easily casted */
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
extern void dlist_destroy(dlist * l);
extern void dlist_destroy_keep_data(dlist * l);
extern int dlist_length(dlist * l);

extern dlist *dlist_append_string(dlist * l, char *data);
extern dlist *dlist_append_list(dlist * l, dlist * data);
extern dlist *dlist_append_int(dlist * l, int data);
extern dlist *dlist_append_symbol(dlist * l, struct symbol *data);

typedef struct symbol {
	int lineno;
	char *filename;
	char *sql;

	int token;

	symtype type;
	symbdata data;
} symbol;

/*
typedef struct Symbol {
	int lineno;
	char *filename;
	char *sql;

	int token;
} Symbol;
*/

typedef struct SelectNode {
	symbol s;

	int distinct;
	struct dlist *selection;
	struct dlist *into; /* ?? */
	symbol * from;
	symbol * where;
	symbol * groupby;
	symbol * having;
	symbol * orderby;
	symbol * name;
} SelectNode;

typedef struct ListNode {
	symbol s;
	struct dlist *l;
} ListNode;

typedef struct AtomNode {
	symbol s;
	struct atom  *a;
} AtomNode;

extern symbol *symbol_init(symbol *s, struct context *c, int token );

extern symbol *symbol_create(struct context *c, int token, char *data);

extern symbol *symbol_create_list(struct context *c, int token,
				  dlist * data);
extern symbol *symbol_create_int(struct context *c, int token, int data);
extern symbol *symbol_create_symbol(struct context *c, int token,
				    symbol * data);
extern void symbol_destroy(symbol * sym);

extern symbol *newSelectNode( struct context* c, int distinct, struct dlist *selection, struct dlist *into, symbol *from, symbol *where, symbol *groupby, symbol *having, symbol *orderby, symbol *name);

extern symbol *newAtomNode( struct context *c, atom *a );


#endif				/*SYMBOL_H */
