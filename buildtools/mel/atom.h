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

#ifndef _ATOM_H_
#define _ATOM_H_

#include "ops.h"
#include "list.h"
#include "type_arg.h"

class Atom:public Symbol {
      public:
	Atom(int t, char *n, int size = 0, int align = 0, Atom * parent = NULL, Ops ** cmds = NULL);
	 Atom(int t, char *n, int size = 0, int align = 0, Arg * type = NULL, Ops ** cmds = NULL);

	int isFixed() const;
	int size() const;
	int align() const;
	Atom *parent() const;
	Atom *top_parent() const;
	Arg *type() const;
	Arg *top_type() const;
	Ops **cmds() const;

	virtual ostream & print(language * l, ostream & o) const;
      private:
	int _size;
	int _align;
	Atom *_parent;
	Arg *_type;
	Ops **_cmds;
};
#endif

