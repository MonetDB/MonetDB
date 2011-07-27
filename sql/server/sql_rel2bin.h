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


#ifndef _SQL_REL2BIN_H
#define _SQL_REL2BIN_H

#define PSEL(s) ((s->type == st_select || s->type == st_uselect) && !s->op2->nrcols)
#define RSEL(s) (s->type == st_select2 || s->type == st_uselect2)
#define USEL(s) (s->type == st_uselect || s->type == st_uselect2)

#include "sql_statement.h"
#include "sql_types.h"

extern stmt *rel2bin(mvc *c, stmt *s);
extern sql_column *basecolumn(stmt *st);

#endif /* _SQL_REL2BIN_H */

