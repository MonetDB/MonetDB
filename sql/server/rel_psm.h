/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0.  If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 *
 * Copyright 1997 - July 2008 CWI, August 2008 - 2016 MonetDB B.V.
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

extern sql_func *resolve_func( mvc *sql, sql_schema *s, const char *name, dlist *typelist, int type, char *op);
extern sql_rel *rel_psm(mvc *sql, symbol *sym);
extern sql_rel *rel_select_with_into( mvc *sql, symbol *sq);

#endif /*_REL_PSM_H_*/
