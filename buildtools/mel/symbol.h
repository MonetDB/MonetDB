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

#ifndef _SYMBOL_H_
#define _SYMBOL_H_

#ifdef HAVE_IOSTREAM
#include <iostream>
#else
#include <iostream.h>
#endif
#ifdef HAVE_CSTDIO
#include  <cstdio>
#else
#include <stdio.h>
#endif
#if defined(HAVE_IOSTREAM) || defined(HAVE_CSTDIO)
using namespace std;
#endif
#include  "list.h"

class language;
class Symbol {
      public:
	Symbol(int token, const char *name);
	virtual ~Symbol();

	int operator==(const Symbol &) const;
	int Token() const;
	const char *Name() const;
	virtual const char *toString() const;
	void Name(const char *name);
	virtual ostream & print(language * l, ostream &) const;
      private:
	int _token;
	char *_name;
};

#endif // _SYMBOL_H_

