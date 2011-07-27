/*
 * The contents of this file are subject to the MonetDB Public License
 * Version 1.1 (the "License"); you may not use this file except in
 * compliance with the License. You may obtain a copy of the License at
 * http://www.monetdb.org/Legal/MonetDBLicense
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

#ifndef _REL_SEMANTIC_H_
#define _REL_SEMANTIC_H_

#include <sql_list.h>
#include "sql_symbol.h"
#include "sql_parser.h"
#include "sql_relation.h"
#include <stdarg.h>

extern sql_rel *rel_semantic(mvc *sql, symbol *sym);
extern sql_rel *rel_parse(mvc *m, char *query, char emode);

extern comp_type swap_compare( comp_type t );
extern comp_type range2lcompare( int r );
extern comp_type range2rcompare( int r );
extern int compare2range( int l, int r );

#endif /*_REL_SEMANTIC_H_*/

