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

#ifndef _ATOMOPS_H_
#define _ATOMOPS_H_

#include "ops.h"
#include "list.h"

enum atom_ops_t {
	OP_FIX,
	OP_UNFIX,
	OP_TOSTR,
	OP_FROMSTR,
	OP_READ,
	OP_WRITE,
	OP_COMP,
	OP_HASH,
	OP_NULL,
	OP_CONVERT,
	OP_PUT,
	OP_LEN,
	OP_DEL,
	OP_HEAP,
	OP_CHECK,
	OP_HCONVERT,
	OP_NEQUAL
};

class Atomops:public Ops {
      public:
	Atomops(int t, char *n, int op);

	static const char *string(int op);

	virtual ostream & print(language * l, ostream & o) const;
};
#endif // _ATOMOPS_H_

