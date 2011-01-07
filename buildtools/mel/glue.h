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

#ifndef _GLUE_H_
#define _GLUE_H_

#include "language.h"

/*
 * The glue Class
 * --------------
 *
 * The glue class generates the code which glues extensions to Monet. It
 * generates the install and delete functions for the module and type check
 * and unpack functions for the commands/operators. For atoms 
 * the properties are set. It inherits form language so only the code
 * generation methods needed have to be re-implemented.
 * 
 */
class glue:public language {
      public:
	glue() {
		arg_nr = 0;
	}
	const char *get_name() const {
		return "glue";
	}
	int generate_code(ostream & o, Symbol *rootptr);
	int generate_table(ostream & o, Symtable * tableptr);

	ostream & gen_module(ostream & o, const Module & m);

      public:
	int arg_nr;
	Symtable *tableptr;
	ostream & gen_command_args(ostream & o, const Command & m, const char *sep = "");
	ostream & gen_operator_args(ostream & o, const Operator & m, const char *sep = "");
};

#endif //_GLUE_H_

