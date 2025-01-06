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

#ifndef _REL_TRANS_H_
#define _REL_TRANS_H_

#include "sql_symbol.h"
#include "sql_mvc.h"
#include "sql_relation.h"
#include "sql_query.h"

extern sql_rel *rel_transactions(sql_query *query, symbol *sym);

#endif /*_REL_TRANS_H_*/

