// The contents of this file are subject to the MonetDB Public License
// Version 1.1 (the "License"); you may not use this file except in
// compliance with the License. You may obtain a copy of the License at
// http://monetdb.cwi.nl/Legal/MonetDBLicense-1.1.html
//
// Software distributed under the License is distributed on an "AS IS"
// basis, WITHOUT WARRANTY OF ANY KIND, either express or implied. See the
// License for the specific language governing rights and limitations
// under the License.
//
// The Original Code is the MonetDB Database System.
//
// The Initial Developer of the Original Code is CWI.
// Portions created by CWI are Copyright (C) 1997-July 2008 CWI.
// Copyright August 2008-2011 MonetDB B.V.
// All Rights Reserved.

#include <monetdb_config.h>
#include "symtable.h"
#include <string.h>

Symtable::Symtable(size_t size)
{
	_next = 0;
	_size = size;
	_table = new const Symbol*[size];
}	

Symtable::~Symtable()
{
	delete [] _table;
}

void
Symtable::insert(const Symbol &s)
{
	_table[_next++] = &s;
}

const Symbol *
Symtable::find(int token, const char *name) const
{
	for (int i = 0; i < _next; i++) {
		const Symbol *sym = _table[i];
		if (sym->Token() == token &&
		   strcmp(sym->Name(), name) == 0) {
			return sym;
		}
	} 
	return NULL;
}

const Symbol *
Symtable::find(const char *name) const
{
	for (int i = 0; i < _next; i++) {
		const Symbol *sym = _table[i];
		if (strcmp(sym->Name(), name) == 0) {
			return sym;
		}
	} 
	return NULL;
}

void
Symtable::traverse(language *l, ostream &s)
{
	for (int i = 0; i < _next; i++) {
		const Symbol *sym = _table[i];
		sym->print(l,s);
	}
}
