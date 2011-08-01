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

#ifndef _SQL2MAL_H
#define _SQL2MAL_H

#include <sql.h>
#include <sql_atom.h>
#include <sql_statement.h>
#include <sql_env.h>
#include <sql_mvc.h>
#include <mal_function.h>

sql5_export Symbol backend_dumpproc(backend *be, Client c, cq *q, stmt *s);
sql5_export void backend_callinline(backend *be, Client c, stmt *s);
sql5_export void backend_call(backend *be, Client c, cq *q);
sql5_export void initSQLreferences(void);
sql5_export str backend_name(cq *cq);
sql5_export void monet5_create_table_function(ptr M, char *name, sql_rel *rel, sql_table *t);
sql5_export int monet5_resolve_function(ptr M, sql_func *f);
sql5_export void backend_create_func(backend *be, sql_func *f);

#endif /* _SQL2MAL_H */
