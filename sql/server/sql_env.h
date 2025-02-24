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

#ifndef _SQL_ENV_H_
#define _SQL_ENV_H_

#include "sql_parser.h"
#include "sql_symbol.h"
#include "sql_mvc.h"

extern str sql_update_var(mvc *m, sql_schema *s, const char *name, const ValRecord *ptr);

extern int sql_create_env(mvc *m, sql_schema *s);

#endif /* _SQL_ENV_H_ */
