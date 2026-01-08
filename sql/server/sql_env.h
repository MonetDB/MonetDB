/*
 * SPDX-License-Identifier: MPL-2.0
 *
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0.  If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 *
 * For copyright information, see the file debian/copyright.
 */

#ifndef _SQL_ENV_H_
#define _SQL_ENV_H_

#include "sql_parser.h"
#include "sql_symbol.h"
#include "sql_mvc.h"

extern str sql_update_var(mvc *m, sql_schema *s, const char *name, const ValRecord *ptr);

extern int sql_create_env(mvc *m, sql_schema *s);

#endif /* _SQL_ENV_H_ */
