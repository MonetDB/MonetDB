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

#ifndef _COMMAND_H_
#define _COMMAND_H_

#include "symbol.h"
#include "list.h"
#include "mel.h"
#include "parser.h"

enum cmd_types {
	NORMAL,
	AGGREGATE,
	MULTIPLEX
};


class Command:public Symbol {
      public:
	Command(int t, char *n, char *fcn, int type, Symbol *result = NULL, List * args = NULL, char *hlp = NULL);

	char *fcn() const;
	Symbol *result() const;
	List *args() const;
	char *hlp() const;
	int type() const;
	int varargs() const;
	int Id() const;

	virtual const char *Token() const;
	virtual ostream & print(language * l, ostream & o) const;
      private:
	void set_arg_names(const char *base);

	char *_fcn;
	Symbol *_result;
	char *_hlp;
	int _type;
	int _id;
      protected:
	 List * _args;
};
#endif

