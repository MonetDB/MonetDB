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

#ifndef _HTML_H_
#define _HTML_H_

#include "language.h"

class html:public language {
      public:
	const char *get_name() const {
		return "html";
	}
	int generate_code(ostream & o, Symbol *root);

	ostream & gen_module(ostream & o, const Module & m);
	ostream & gen_dependency(ostream & o, const Dependency & m);
	ostream & gen_atom(ostream & o, const Atom & m);
	ostream & gen_atomops(ostream & o, const Atomops & m);
	ostream & gen_command(ostream & o, const Command & m);
	ostream & gen_operator(ostream & o, const Operator & m);
	ostream & gen_builtin(ostream & o, const Builtin & m);
	ostream & gen_iterator(ostream & o, const Iterator & m);
	ostream & gen_atom_arg(ostream & o, const AtomArg & m);
	ostream & gen_type_arg(ostream & o, const TypeArg & m);
	ostream & gen_var_arg(ostream & o, const VarArg & m);
	ostream & gen_bat_arg(ostream & o, const BatArg & m);
	ostream & gen_any_arg(ostream & o, const AnyArg & m);
	ostream & gen_prelude(ostream & o, const Prelude & m);
	ostream & gen_epilogue(ostream & o, const Epilogue & m);
      private:
	int result;
};

#endif //_HTML_H_

