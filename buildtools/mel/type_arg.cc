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
#include "type_arg.h"
#include "language.h"
#include <string.h>


TypeArg::TypeArg(int t, const char *n, int type, char *v) : Arg(t,n,v)
{
	_type = type;
}

const char *
TypeArg::toString() const
{
   	switch (_type){
	case TYPE_BIT:
		return "bit"; 
	case TYPE_CHR:
		return "chr"; 
	case TYPE_BTE:
		return "bte";
	case TYPE_SHT:
		return "sht";
	case TYPE_INT:
		return "int";
	case TYPE_PTR:
		return "ptr";
	case TYPE_OID:
		return "oid";
	case TYPE_WRD:
		return "wrd";
	case TYPE_FLT:
		return "flt";
	case TYPE_DBL:
		return "dbl";
	case TYPE_LNG:
		return "lng";
	case TYPE_STR:
		return "str";
	}
	return "void";
}

int
TypeArg::type() const
{
	return _type;
}

int
TypeArg::isFixed() const
{
	if (_type == TYPE_STR)
		return 0;
	return 1;
}

ostream &
TypeArg::gen_type(ostream &o, int t)
{
   	switch (t){
	case TYPE_BIT:
		o << "bit";
		break;
	case TYPE_CHR:
		o << "chr";
		break;
	case TYPE_BTE:
		o << "bte";
		break;
	case TYPE_SHT:
		o << "sht";
		break;
	case TYPE_INT:
		o << "int";
		break;
	case TYPE_PTR:
		o << "ptr";
		break;
	case TYPE_OID:
		o << "oid";
		break;
	case TYPE_WRD:
		o << "wrd";
		break;
	case TYPE_FLT:
		o << "flt";
		break;
	case TYPE_DBL:
		o << "dbl";
		break;
	case TYPE_LNG:
		o << "lng";
		break;
	case TYPE_STR:
		o << "str";
		break; 
	default:
		o << "void";
		break;
	}
	return o;
}

ostream &
TypeArg::print(language *l, ostream &o) const
{
	return l->gen_type_arg(o, *this);
}
