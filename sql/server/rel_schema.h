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

#ifndef _REL_SCHEMA_H_
#define _REL_SCHEMA_H_

#include <stdio.h>
#include <stdarg.h>
#include <sql_list.h>
#include "sql_symbol.h"
#include "sql_statement.h"

extern sql_rel *rel_schemas(mvc *sql, symbol *sym);

extern sql_rel *rel_create_table(mvc *sql, sql_schema *ss, int temp, char *sname, char *name, symbol *table_elements_or_subquery, int commit_action, char *loc);
extern sql_rel *rel_list(sql_allocator *sa, sql_rel *l, sql_rel *r);
extern sql_table * mvc_create_table_as_subquery( mvc *sql, sql_rel *sq, sql_schema *s, char *tname, dlist *column_spec, int temp, int commit_action );

#endif /*_REL_SCHEMA_H_*/
