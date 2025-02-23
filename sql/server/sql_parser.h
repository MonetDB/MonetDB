/*
 * SPDX-License-Identifier: MPL-2.0
 *
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0.  If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 *
 * Copyright 2024, 2025 MonetDB Foundation;
 * Copyright August 2008 - 2023 MonetDB B.V.;
 * Copyright 1997 - July 2008 CWI.
 */

#ifndef _SQL_PARSER_H_
#define _SQL_PARSER_H_

#include "sql_tokens.h"
#include "sql_mvc.h"

/* the next define makes the parser output more specific error messages,
 * instead of only a dull 'parse error' */
#define YYERROR_VERBOSE 1
/* enable this to get an idea of what the parser is doing on stdout
#define YYDEBUG 1
*/

extern int find_subgeometry_type(mvc *m, char*);
extern char *token2string(tokens token);
extern int sqlparse(mvc *m);

//#include "mal_errors.h"		/* for SQLSTATE() */

#endif /*_SQL_PARSER_H_*/

