// The contents of this file are subject to the MonetDB Public License
// Version 1.1 (the "License"); you may not use this file except in
// compliance with the License. You may obtain a copy of the License at
// http://www.monetdb.org/Legal/MonetDBLicense
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

%module MapiLib
%include "typemaps.i"
%include "exception.i"

%{
#include "monetdb_config.h"
#include "mapi.h"
%}

// don't care for the guard symbol
%ignore _MAPI_H_INCLUDED;

// unimplementable in Perl
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

// This tells SWIG to treat char ** as a special case 
%typemap(in) char ** { 
    AV *tempav; 
    I32 len; int i; 
    SV **tv; 
    if (!SvROK($input))
	croak("$input is not a reference."); 
    if (SvTYPE(SvRV($input)) != SVt_PVAV) 
	croak("$input is not an array."); 
    tempav = (AV*)SvRV($input);
    len = av_len(tempav); 
    $1 = (char **) malloc((len+2)*sizeof(char *)); 
    for (i = 0; i <= len; i++) { 
	tv = av_fetch(tempav, i, 0); 
	$1[i] = (char *) SvPV(*tv,PL_na); 
    } 
    $1[i] = 0;

}; 

// This cleans up our char ** array after the function call 
%typemap(freearg) char ** { 
    free($1);
} 

// Creates a new Perl array and places a char ** into it 
%typemap(out) char ** { 
    AV *myav; 
    SV **svs; 
    int i = 0,len = 0; 

    /* Figure out how many elements we have */ 
    while ($1[len]) len++; 
    svs = (SV **) malloc(len*sizeof(SV *)); 
    for (i = 0; i < len ; i++) { 
	svs[i] = sv_newmortal(); 
	sv_setpv((SV*)svs[i],$1[i]);
    };
    myav = av_make(len,svs); 
    free(svs); 
    $result = newRV((SV*)myav); 
    sv_2mortal($result);
}

%typemap(memberin) char [ANY] { strncpy($1,$input,$dim0); }

// options string arg, i.e. arg can be a string or NULL.
// %typemap(in,parse="z") char *OPTSTRING "";

// %apply char *OPTSTRING {char *lang};
// %apply char *OPTSTRING {char *password};
// %apply char *OPTSTRING {char *username};
// %apply char *OPTSTRING {char *host};
// %apply char *OPTSTRING {char *cmd};

// TODO:
// %typemap(out) char * { }

%typemap(in) PerlIO * {
    $1 = IoIFP(sv_2io($input));
}

%include "mapi.h"
