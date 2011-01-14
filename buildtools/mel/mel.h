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

#ifndef _MEL_H_
#define _MEL_H_

#ifndef YYSTYPE
#define YYSTYPE	yystype
#endif

#include <stdio.h>
#include "symtable.h"
#include "symbol.h"
#include "list.h"
#include "FileInfo.h"

#include "ops.h"
#include "atomops.h"
#include "type_arg.h"

#include <stdlib.h>

typedef union {
	int i;			// integer
	char *s;		// string
	Symbol *sym;		// symbol
	List *l;		// symbol list
	Ops **a;		// symbol array
	Ops *op;		// Operation
	struct {
		int i1, i2;
	} r;
} yystype;

#define yyerror(err)	mel_yyerror(err)


int yylex();
extern int yyparse();
extern void mel_yyerror(const char *);

extern void *get_cur_buffer();
extern void new_buffer(FILE *f);
extern void close_and_change(void *buf, FILE *f);
extern void semerror(void);
extern int melget_lineno(void);
extern void melset_lineno(int lineno);

extern const char *token2str(int token);
extern const char *type2str(int tpe);
extern const char *get_signature(char *fcn);


#define yywrap()	1

extern int parse_error;
extern int sem_error;
extern char *progname;

extern FILE *yyin;

extern Symtable *table;
extern Symbol *root;
extern FileInfo *fileinfo;
extern List *dirs;
extern const char *config_h;

#endif

