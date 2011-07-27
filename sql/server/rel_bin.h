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

#ifndef _REL_BIN_H_
#define _REL_BIN_H_

#include "rel_semantic.h"
#include "sql_statement.h"

extern stmt * exp_bin(mvc *sql, sql_exp *e, stmt *left, stmt *right, group *grp, stmt *sel);
extern stmt * rel_bin(mvc *sql, sql_rel *rel);
extern stmt * output_rel_bin(mvc *sql, sql_rel *rel);

#endif /*_REL_BIN_H_*/
