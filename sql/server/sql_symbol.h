/*
 * SPDX-License-Identifier: MPL-2.0
 *
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0.  If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 *
 * Copyright 2024 MonetDB Foundation;
 * Copyright August 2008 - 2023 MonetDB B.V.;
 * Copyright 1997 - July 2008 CWI.
 */

#ifndef SQL_SYMBOL_H
#define SQL_SYMBOL_H

#include "sql_types.h"
#include "sql_atom.h"
#include "sql_tokens.h"

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
	lng lpair[2];
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

extern dlist *dlist_create(allocator *sa);
extern int dlist_length(dlist *l);

extern dlist *dlist_append_string(allocator *sa, dlist *l, const char *data);
extern dlist *dlist_append_list(allocator *sa, dlist *l, dlist *data);
extern dlist *dlist_append_int(allocator *sa, dlist *l, int data);
extern dlist *dlist_append_lng(allocator *sa, dlist *l, lng data);
extern dlist *dlist_append_symbol(allocator *sa, dlist *l, struct symbol *data);
extern dlist *dlist_append_type(allocator *sa, dlist *l, struct sql_subtype *data);

typedef struct symbol {
	tokens token;
	symtype type;
	symbdata data;
} symbol;

typedef struct SelectNode {
	symbol s;

	symbol *limit;
	symbol *offset;
	symbol *sample;
	symbol *seed;
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
	symbol *window;
} SelectNode;

typedef struct AtomNode {
	symbol s;
	struct atom *a;
} AtomNode;

extern symbol *symbol_create(allocator *sa, tokens token, char *data);
extern symbol *symbol_create_list(allocator *sa, tokens token, dlist *data);
extern symbol *symbol_create_int(allocator *sa, tokens token, int data);
extern symbol *symbol_create_lng(allocator *sa, tokens token, lng data);
extern symbol *symbol_create_symbol(allocator *sa, tokens token, symbol *data);

extern symbol *newSelectNode(allocator *sa, int distinct, struct dlist *selection, struct dlist *into, symbol *from, symbol *where, symbol *groupby, symbol *having, symbol *orderby, symbol *name, symbol *limit, symbol *offset, symbol *sample, symbol *seed, symbol *window);

extern symbol *newAtomNode(allocator *sa, atom *a);

#endif /* SQL_SYMBOL_H */

