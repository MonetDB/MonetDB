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

%{
#include <monetdb_config.h>
#include "mel.h"
/* this prevents an extraneous declaration of malloc and free from happening */
#define YYINCLUDED_STDLIB_H
#include <string.h>
#ifdef HAVE_MALLOC_H
#include <malloc.h>
#endif
#ifdef HAVE_IOSTREAM
#include <iostream>
#else
#include <iostream.h>
#endif
#ifdef HAVE_CSTDIO
#include  <cstdio>
#else
#include <stdio.h>
#endif
#if defined(HAVE_IOSTREAM) || defined(HAVE_CSTDIO)
using namespace std;
#endif

#include "FileInfo.h"
#include "module.h"
#include "dependency.h"
#include "atom.h"
#include "atomops.h"
#include "command.h"
#include "operator.h"
#include "builtin.h"
#include "iterator.h"
#include "object.h"
#include "atom_arg.h"
#include "type_arg.h"
#include "var_arg.h"
#include "bat_arg.h"
#include "any_arg.h"
#include "prelude.h"
#include "epilogue.h"

#ifdef NATIVE_WIN32
/* The POSIX name for this item is deprecated. Instead, use the ISO
   C++ conformant name: _strdup. See online help for details. */
#define strdup _strdup
#endif

extern char *script;
%}

%token MEL_MODULE MEL_PRELUDE MEL_EPILOGUE MEL_END MEL_COMMAND 
%token MEL_OPERATOR0 MEL_OPERATOR MEL_OPERATOR1
%token MEL_FIXEDATOM MEL_ATOM MEL_USE MEL_ATOMOP MEL_STRING MEL_IDENTIFIER
%token MEL_USED
%token MEL_NUMBER
%token MEL_ANY MEL_BAT MEL_TYPE MEL_VOID MEL_BUILTIN MEL_OBJECT
%token MEL_VARARGS MEL_SEP MEL_ITERATOR 
%token '=' ';' ':' ',' '(' ')'
%%
module:
      { melset_lineno(fileinfo->LineNo());
	yyin = fileinfo->FilePtr(); }
	module_begin dependencies definitions module_end
      { FileInfo *f = fileinfo->Parent();
	if (f == NULL) {
	    root = new Module(MEL_MODULE, $2.s,
	    	fileinfo->FileName(), $3.l, $4.l);
	} else {
      	 	melset_lineno(f->LineNo());
		$$.sym = new Module(MEL_MODULE, $2.s,
	    		fileinfo->FileName(), $3.l, $4.l);
		close_and_change(f->Buffer(), f->FilePtr());
		delete(fileinfo);
		fileinfo = f;
	} }
    ;

module_begin:
    	MEL_MODULE ident ';'
      { $$.s = $2.s; }
    |	MEL_MODULE ident
      { $$.s = $2.s;
        yyerror("missing \';\'"); }
    |	MEL_MODULE ';'
      { $$.s = fileinfo->BaseName();
        yyerror("missing \'module name\'"); }
    ;

module_end:
	MEL_END ident ';'
      { $$.s = $2.s; }
    |	MEL_END ';'
      { $$.s = fileinfo->BaseName();
        yyerror("missing \'module name\'"); }
    ;

dependencies:
	ne_dependencies
    |  /* empty */
      { $$.l = NULL; }
    ;

ne_dependencies:
	ne_dependencies dependency
      { $$.l = $1.l;
	if ($2.l) {
		$$.l->insert($2.l);
		delete($2.l);
	} }
   |	dependency
      { $$.l = $1.l; }
    ;

dependency:
	MEL_USE modulelist ';'
      { $$.l = $2.l; }
    ;

modulelist:
	modulelist ',' modulename
       { $$.l = $1.l;
         if ($3.sym)
	 	$$.l->insert($3.sym); }
    |   modulename
       { $$.l = new List(50);
         if ($1.sym)
	 	$$.l->insert($1.sym); }
    ;

modulename:
	module_ident
      { fileinfo->Buffer(get_cur_buffer());
	fileinfo->LineNo(melget_lineno());
	char *filename = FileInfo::find_module($1.s, dirs);
	if (filename) {
		fileinfo = FileInfo::open(filename, $1.s, fileinfo);
		new_buffer(fileinfo->FilePtr());
		$1.sym = new Dependency(MEL_USE, $1.s, fileinfo->FileName());
	  	table->insert(*$1.sym);
	} else {
		yyerror("Module not found");
		$1.sym = NULL;
	} }
	module
	{ $$.sym = $1.sym;
	  ((Dependency*)$$.sym)->setModule($3.sym); }
    | 	MEL_USED
    	{ $$.sym = NULL; }
    ;

definitions:
     	ne_definitions
   |  /* empty */
      { $$.l = NULL; }
    ;

ne_definitions:
     	ne_definitions definition
      { $$.l = $1.l;
	if ($2.sym)
		$$.l->insert($2.sym); }
   |    definition
      { $$.l = new List(500);
	if ($1.sym)
		$$.l->insert($1.sym); }
    ;

definition:
   	atomdef
   |	command
   |	iterator
   |	builtin
   |	operator
   |	object
   |    MEL_PRELUDE '=' ident ';'
      { $$.sym = new Prelude(MEL_PRELUDE, $3.s); 	}
   |	MEL_EPILOGUE '=' ident ';'
      { $$.sym = new Epilogue(MEL_EPILOGUE, $3.s); 	}
    ;

iterator:
     	MEL_ITERATOR ident
	'(' args ')'
     	'=' ident ';' MEL_STRING
      { $$.sym = new Iterator(MEL_ITERATOR, $2.s, $7.s, $4.l, $9.s); }
    ;

builtin:
     	MEL_BUILTIN commandname { get_signature($2.s); } ident ';' MEL_STRING
      { $$.sym = new Builtin(MEL_BUILTIN, $2.s, $4.s, $6.s, script); }
    ;

command:
     	MEL_COMMAND commandname '(' args ')' returntype
	'=' ident ';' MEL_STRING
      { $$.sym = new Command(
      		MEL_COMMAND, $2.s, $8.s, NORMAL, $6.sym, $4.l, $10.s); }
   |	MEL_COMMAND '{' commandname '}' '(' args ')' returntype
	'=' ident ';' MEL_STRING
      { $$.sym = new Command(
      		MEL_COMMAND, $3.s, $10.s, AGGREGATE, $8.sym, $6.l, $12.s); }
   |	MEL_COMMAND '[' commandname ']' '(' args ')' returntype
	'=' ident ';' MEL_STRING
      { $$.sym = new Command(
      		MEL_COMMAND, $3.s, $10.s, MULTIPLEX, $8.sym, $6.l, $12.s); }
    ;

commandname:
      	ident
    |  	MEL_STRING
    ;

operator:
      { anynr_clear(); }
     	MEL_OPERATOR0 operand MEL_STRING operand returntype
	'=' ident ';' MEL_STRING
      { $$.sym = new Operator(
      		MEL_OPERATOR0, $4.s, $8.s, $6.sym, $3.sym, $5.sym, $10.s); }
    |
      { anynr_clear(); }
     	MEL_OPERATOR operand MEL_STRING operand returntype
	'=' ident ';' MEL_STRING
      { $$.sym = new Operator(
      		MEL_OPERATOR, $4.s, $8.s, $6.sym, $3.sym, $5.sym, $10.s); }
    |
      { anynr_clear(); }
     	MEL_OPERATOR1 operand MEL_STRING operand returntype
	'=' ident ';' MEL_STRING
      { $$.sym = new Operator(
      		MEL_OPERATOR1, $4.s, $8.s, $6.sym, $3.sym, $5.sym, $10.s); }
    ;

operand:
     	'(' arg ')'
      { $$.sym = $2.sym; }
   |  /* empty */
      { $$.sym = NULL; }
    ;

object:
     	MEL_OBJECT ident ';'
      { $$.sym = new Object(MEL_OBJECT, $2.s); }
   |    MEL_OBJECT ident '=' ident ';'
      { const Symbol *s = table->find(MEL_ATOM, $4.s);
        if (s) {
       	 	$$.sym = new Object(MEL_OBJECT, $2.s, s);
	} else {
	   	yyerror("object not found");
		$$.sym = NULL;
	} }
    ;

args:
      { anynr_clear(); }
	ne_args
      { $$ = $2; }
   |	/* empty */
      { $$.l = NULL; }
    ;

ne_args:
	ne_args ',' lastarg
       { $$.l = $1.l;
	 if ($3.sym)
	 	$$.l->insert($3.sym); }
    |	arg
       { $$.l = new List(20);
	 if ($1.sym)
	 	$$.l->insert($1.sym); }
    ;

lastarg:
	arg
   |	MEL_VARARGS atom MEL_VARARGS
      { $$.sym = new VarArg(MEL_VARARGS, NULL, $2.sym); }
   |    MEL_VARARGS
      { $$.sym = new VarArg(MEL_VARARGS, NULL); }
    ;

arg:
     	atom varname
      { if ($1.sym)
	 	$1.sym->Name($2.s);
	$$.sym = $1.sym; 		}
    ;

varname:
	ident
      { $$.s = $1.s; }
    |	/* empty */
      { $$.s = NULL; }
    ;

returntype:
     	':' atom
      { $$.sym = $2.sym; }
   |	/* empty */
      { $$.sym = NULL; }
    ;

atomdef:
   	MEL_ATOM ident '=' parentatom ';' atomops MEL_END ';'
      { $$.sym = new Atom(MEL_ATOM, $2.s, -1, 0, (Atom*)$4.sym, $6.a);
      	if (!table->find(MEL_ATOM, $2.s))
		table->insert(*$$.sym);
	else {
	   	yyerror("Atom redefined");
		semerror();
	} }
   |	MEL_ATOM ident '=' type ';' atomops MEL_END ';'
      { $$.sym = new Atom(MEL_ATOM, $2.s, -1, 0, (Arg*)$4.sym, $6.a);
      	if (!table->find(MEL_ATOM, $2.s))
		table->insert(*$$.sym);
	else {
	   	yyerror("Atom redefined");
		semerror();
	} }
   | 	MEL_ATOM ident fixedmeasures ';' atomops MEL_END ';'
      { $$.sym = new Atom(MEL_ATOM, $2.s, $3.r.i1, $3.r.i2, (Atom*)NULL, $5.a);
      	if (!table->find(MEL_ATOM, $2.s))
		table->insert(*$$.sym);
	else {
	   	yyerror("Atom redefined");
		semerror();
	} }
   | 	MEL_FIXEDATOM ident ';' atomops MEL_END ';'
      { $$.sym = new Atom(MEL_ATOM, $2.s, 0, 0, (Atom*)NULL, $4.a);
      	if (!table->find(MEL_ATOM, $2.s))
		table->insert(*$$.sym);
	else {
	   	yyerror("Atom redefined");
		semerror();
	} }
   | 	error
      { $$.sym = NULL; }
    ;

fixedmeasures:
    	'[' fixedspec ']'
      { $$.r.i1 = $2.r.i1;
	$$.r.i2 = $2.r.i2; }
   |  	/* empty */
      { $$.r.i1 = -1;
	$$.r.i2 = 0; }
    ;

fixedspec:
	MEL_NUMBER alignment
      { $$.r.i1 = $1.i;
	$$.r.i2 = $2.i; }
   |  	/* empty */
      { $$.r.i1 = -1;
	$$.r.i2 = 0; }
    ;

alignment:
	',' MEL_NUMBER
       { $$.i = $2.i; }
   |  /* empty */
       { $$.i = 0; }
    ;

atomops:
	ne_atomops
   |	/* empty */
      { $$.a = NULL; }
    ;

ne_atomops:
	ne_atomops atomop
       { $$.a = $1.a;
         if ($2.op)
	 	$$.a[$2.op->operation()] = $2.op;
	    	if ($2.op->operation() == OP_NEQUAL)
	 		$$.a[OP_COMP] = $2.op;
	}
    |   atomop
       { $$.a = new Ops*[20]; for(int i =0; i<20;i++) $$.a[i] = NULL;
         if ($1.op)
	 	$$.a[$1.op->operation()] = $1.op;
	    	if ($1.op->operation() == OP_NEQUAL)
	 		$$.a[OP_COMP] = $1.op; }
    ;

atomop:
   	MEL_ATOMOP '=' ident ';'
      { $$.op = new Atomops(MEL_ATOMOP, $3.s, $1.i); }
    ;


parentatom:
   	ident
      { const Symbol *s = table->find(MEL_ATOM, $1.s);
        if (s) {
        	$$.sym = (Symbol*)s;
	} else {
		yyerror("Atom not found");
		$$.sym = NULL;
	} }
    ;

trueatom:
   	ident
      { const Symbol *s = table->find(MEL_ATOM, $1.s);
        if (s)
        	$$.sym = new AtomArg(MEL_ATOM, "", (Atom*)s , NULL);
	else {
		yyerror("Atom not found");
		$$.sym = NULL;
	} }
    ;

type:
       MEL_TYPE
      { $$.sym = new TypeArg(MEL_TYPE, NULL, $1.i , NULL);   	}
   |   MEL_BAT
      { $$.sym = new BatArg(MEL_BAT, NULL, NULL, NULL); 	}
    ;

atom:
     	MEL_ANY	anyspec
      { $$.sym = new AnyArg(MEL_ANY, NULL, $2.i); }
   |    type
   |    trueatom
   |	MEL_BAT '[' atom ','  atom ']'
      { $$.sym = new BatArg(MEL_BAT, NULL, (Arg*) $3.sym, (Arg*) $5.sym); }
   |    MEL_VOID
      { $$.sym = NULL; }
    ;

anyspec:
	MEL_SEP MEL_NUMBER
      { $$.i = $2.i;
        if ($2.i >= ANY_MAX || $2.i < 0) {
	   yyerror("Wrong any number, should be in range [0..31]");
	   $$.i = -1;
	} }
   |	/* empty */
      { $$.i = -1; }
    ;

ident:
	MEL_IDENTIFIER
    |   MEL_USED
    |	MEL_BAT
	{ $$.s = strdup("bat"); }
    |   MEL_VOID
	{ $$.s = strdup("void"); }
    |   MEL_TYPE
	{ $$.s = (char*)type2str(yyval.i); }
    ;

module_ident:
	MEL_IDENTIFIER
    |	MEL_BAT
	{ $$.s = strdup("bat"); }
    |   MEL_VOID
	{ $$.s = strdup("void"); }
    |   MEL_TYPE
	{ $$.s = (char*)type2str(yyval.i); }
    ;
%%

void
semerror()
{
	if (progname) {
		cerr << progname << ": ";
	}
	if (fileinfo->FileName()) {
		cerr << "\"" << fileinfo->FileName() << "\", ";
	}
	if (melget_lineno() > 0) {
		cerr << "line " << (melget_lineno()) << ", ";
	}
	cerr << "semantic error: ";
	sem_error++;
}
