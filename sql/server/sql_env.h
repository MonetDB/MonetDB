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

#ifndef _SQL_ENV_H_
#define _SQL_ENV_H_

#include "sql_parser.h"
#include "sql_symbol.h"
#include "sql_statement.h"
#include "sql_mvc.h"

extern int mvc_debug_on(mvc *m, int flag);

extern str sql_update_var(mvc *sql, char *name);

extern int sql_create_env(mvc *sql, sql_schema *s);

#define NR_KEEPQUERY_ARGS 4
#define NR_KEEPCALL_ARGS 10

#endif /* _SQL_ENV_H_ */
