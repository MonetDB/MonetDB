/*
 * The contents of this file are subject to the MonetDB Public License
 * Version 1.1 (the "License"); you may not use this file except in
 * compliance with the License. You may obtain a copy of the License at
 * http://monetdb.cwi.nl/Legal/MonetDBLicense-1.1.html
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

#ifndef _SYMTABLE_H_
#define _SYMTABLE_H_

#include "symbol.h"
#ifdef HAVE_IOSTREAM
#include <iostream>
using namespace std;
#else
#include <iostream.h>
#endif

class Symtable {

      public:
	Symtable(size_t size);
	~Symtable();
	void insert(const Symbol &s);
	const Symbol *find(int token, const char *name) const;
	const Symbol *find(const char *name) const;
	void traverse(language * l, ostream & s);
      private:
	int _next;
	size_t _size;
	const Symbol **_table;
};
#endif //_SYMTABLE_H_

