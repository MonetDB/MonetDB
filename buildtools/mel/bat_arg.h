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

#ifndef _BATARG_H_
#define _BATARG_H_

#include "symbol.h"
#include "list.h"
#include "type_arg.h"


class BatArg:public Arg {
      public:
	BatArg(int t, char *n, const Arg * atom1, const Arg * atom2);

	const Arg *atom1() const;
	const Arg *atom2() const;

	int isFixed() const;
	virtual const char *toString() const;
	virtual ostream & print(language * l, ostream & o) const;
      private:
	const Arg *_atom1;
	const Arg *_atom2;
};
#endif // _BATARG_H_

