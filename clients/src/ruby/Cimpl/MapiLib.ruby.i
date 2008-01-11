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
// Portions created by CWI are Copyright (C) 1997-2008 CWI.
// All Rights Reserved.

%module MapiLib
%include "ruby/typemaps.i"
%include "exception.i"

%{
#include "mapilib/Mapi.h"
%}

// don't care for the guard symbol
%ignore _MAPI_H_INCLUDED;

// unimplementable in ruby
%ignore mapi_bind;
%ignore mapi_bind_var;
%ignore mapi_bind_numeric;
%ignore mapi_clear_bindings;
%ignore mapi_param;
%ignore mapi_param_type;
%ignore mapi_param_numeric;
%ignore mapi_param_string;
%ignore mapi_clear_params;
%ignore mapi_store_field;
%ignore mapi_quote;
%ignore mapi_unquote;

%apply (char *STRING, int LENGTH) {(const char *cmd, size_t size)}; 

// options string arg, i.e. arg can be a string or NULL (in Ruby: None).
%typemap(in,parse="z") char *OPTSTRING "";

%apply char *OPTSTRING {char *lang};
%apply char *OPTSTRING {char *password};
%apply char *OPTSTRING {char *username};
%apply char *OPTSTRING {char *host};

%apply char *OPTSTRING {char *cmd};

%include "mapilib/Mapi.h"
