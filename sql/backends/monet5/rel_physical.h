/*
 * SPDX-License-Identifier: MPL-2.0
 *
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0.  If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 *
 * Copyright 2024 MonetDB Foundation;
 * Copyright August 2008 - 2023 MonetDB B.V.;
 * Copyright 1997 - July 2008 CWI.
 */

#ifndef _REL_PHYSICAL_H_
#define _REL_PHYSICAL_H_

#include "sql_relation.h"
#include "sql_mvc.h"

extern sql_rel *rel_physical(mvc *sql, sql_rel *rel);

#endif /*_REL_PHYSICAL_H_*/
