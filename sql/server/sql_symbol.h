/*
 * The contents of this file are subject to the MonetDB Public License
 * Version 1.1 (the "License"); you may not use this file except in
 * compliance with the License. You may obtain a copy of the License at
 * http://www.monetdb.org/Legal/MonetDBLicense
 *
 * Software distributed under the License is distributed on an "AS IS"
 * basis, WITHOUT WARRANTY OF ANY KIND, either express or implied. See the
 * License for the specific language governing rights and limitations
 * under the License.
 *
 * The Original Code is the MonetDB Database System.
 *
 * The Initial Developer of the Original Code is CWI.
 * Portions created by CWI are Copyright (C) 1997-July 2008 CWI.
 * Copyright August 2008-2011 MonetDB B.V.
 * All Rights Reserved.
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
	wrd w_val;
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

extern dlist *dlist_append_string(sql_allocator *sa, dlist *l, char *data);
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
	int distinct;
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

extern symbol *newSelectNode(sql_allocator *sa, int distinct, struct dlist *selection, struct dlist *into, symbol *from, symbol *where, symbol *groupby, symbol *having, symbol *orderby, symbol *name, symbol *limit, symbol *offset);

extern symbol *newAtomNode(sql_allocator *sa, atom *a);

#endif /* SQL_SYMBOL_H */

