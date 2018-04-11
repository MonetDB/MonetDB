/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0.  If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 *
 * Copyright 1997 - July 2008 CWI, August 2008 - 2018 MonetDB B.V.
 */

#ifndef SQL_SYMBOL_H
#define SQL_SYMBOL_H

#include "sql_types.h"
#include "sql_atom.h"

typedef enum symtype {
	type_int,
	type_lng,
	type_string,
	type_list,
	type_symbol,
	type_type
} symtype;

typedef union symbdata {
	int i_val;
	lng l_val;
	char *sval;
	struct dlist *lval;
	struct symbol *sym;
	struct sql_subtype typeval;
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

extern dlist *dlist_create(sql_allocator *sa);
extern int dlist_length(dlist *l);

extern dlist *dlist_append_string(sql_allocator *sa, dlist *l, const char *data);
extern dlist *dlist_append_list(sql_allocator *sa, dlist *l, dlist *data);
extern dlist *dlist_append_int(sql_allocator *sa, dlist *l, int data);
extern dlist *dlist_append_lng(sql_allocator *sa, dlist *l, lng data);
extern dlist *dlist_append_symbol(sql_allocator *sa, dlist *l, struct symbol *data);
extern dlist *dlist_append_type(sql_allocator *sa, dlist *l, struct sql_subtype *data);

typedef struct symbol {
	int token;
	symtype type;
	symbdata data;
} symbol;

typedef struct SelectNode {
	symbol s;

	symbol *limit;
	symbol *offset;
	symbol *sample;
	int distinct;
	int lateral;
	struct dlist *selection;
	struct dlist *into;	/* ?? */
	symbol *from;
	symbol *where;
	symbol *groupby;
	symbol *having;
	symbol *orderby;
	symbol *name;
} SelectNode;

typedef struct AtomNode {
	symbol s;
	struct atom *a;
} AtomNode;

extern symbol *symbol_create(sql_allocator *sa, int token, char *data);
extern symbol *symbol_create_list(sql_allocator *sa, int token, dlist *data);
extern symbol *symbol_create_int(sql_allocator *sa, int token, int data);
extern symbol *symbol_create_lng(sql_allocator *sa, int token, lng data);
extern symbol *symbol_create_symbol(sql_allocator *sa, int token, symbol *data);

extern int symbol_cmp(symbol *s1, symbol *s2);

extern symbol *newSelectNode(sql_allocator *sa, int distinct, struct dlist *selection, struct dlist *into, symbol *from, symbol *where, symbol *groupby, symbol *having, symbol *orderby, symbol *name, symbol *limit, symbol *offset, symbol *sample);

extern symbol *newAtomNode(sql_allocator *sa, atom *a);

#endif /* SQL_SYMBOL_H */

