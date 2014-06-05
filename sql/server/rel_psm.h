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
 * Copyright August 2008-2014 MonetDB B.V.
 * All Rights Reserved.
 */

#ifndef _REL_PSM_H_
#define _REL_PSM_H_

#include <stdio.h>
#include <stdarg.h>
#include <sql_list.h>
#include <sql_relation.h>
#include "sql_symbol.h"
#include "sql_mvc.h"

/* We need bit wise exclusive numbers as we merge the level also in the flag */
#define PSM_SET 1
#define PSM_VAR 2
#define PSM_RETURN 4
#define PSM_WHILE 8
#define PSM_IF 16
#define PSM_REL 32

#define SET_PSM_LEVEL(level)	(level<<8)
#define GET_PSM_LEVEL(level)	(level>>8)

extern sql_rel *rel_psm(mvc *sql, symbol *sym);
extern sql_rel *rel_select_with_into( mvc *sql, symbol *sq);

#endif /*_REL_PSM_H_*/
