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

#ifndef _REL_SUBQUERY_H_
#define _REL_SUBQUERY_H_

#include "rel_semantic.h"
#include "sql_semantic.h"

extern stmt *select_into(mvc *sql, symbol *sq, exp_kind ek );

extern stmt *value_exp(mvc *sql, symbol *se, int f, exp_kind knd);
extern stmt *logical_value_exp(mvc *sql, symbol *sc, int f, exp_kind knd);

extern stmt *sql_unop_(mvc *sql, sql_schema *s, char *fname, stmt *rs);
extern stmt *sql_binop_(mvc *sql, sql_schema *s, char *fname, stmt *ls, stmt *rs);
extern stmt *sql_Nop_(mvc *sql, char *fname, stmt *a1, stmt *a2, stmt *a3, stmt *a4);

extern stmt *rel_parse_value(mvc *m, char *query, char emode);
extern sql_exp * rel_parse_val(mvc *m, char *query, char emode);

#endif /*_REL_SUBQUERY_H_*/

