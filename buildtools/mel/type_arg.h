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

#ifndef _TYPEARG_H_
#define _TYPEARG_H_

#include "symbol.h"
#include "list.h"

enum type_arg_types_t {
	TYPE_BIT,
	TYPE_CHR,
	TYPE_BTE,
	TYPE_SHT,
	TYPE_INT,
	TYPE_PTR,
	TYPE_OID,
	TYPE_WRD,
	TYPE_FLT,
	TYPE_DBL,
	TYPE_LNG,
	TYPE_STR
};

class Arg : public Symbol {
      public:
	Arg(int t, const char *n, char *v = NULL) : Symbol(t, n) {
		_value = v;
	}
	virtual int type() const {
		return 0;
	}
	virtual int isFixed() const {
		return 1;
	}
	char *value() const {
		return _value;
	};

      private:
	char *_value;
};

class TypeArg:public Arg {
      public:
	TypeArg(int t, const char *n, int type, char *v = NULL);

	virtual const char *toString() const;
	static ostream & gen_type(ostream & o, int t);
	int type() const;
	int isFixed() const;

	virtual ostream & print(language * l, ostream & o) const;
      private:
	int _type;
};
#endif // _TYPEARG_H_

