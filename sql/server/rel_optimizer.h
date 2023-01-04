/*
 * SPDX-License-Identifier: MPL-2.0
 *
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0.  If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 *
 * Copyright 1997 - July 2008 CWI, August 2008 - 2023 MonetDB B.V.
 */

#ifndef _REL_OPTIMIZER_H_
#define _REL_OPTIMIZER_H_

#include "sql_mvc.h"
#include "sql_relation.h"

#define NSQLREWRITERS 29

extern sql_rel *rel_optimizer(mvc *sql, sql_rel *rel, int profile, int instantiate, int value_based_opt, int storage_based_opt);

#endif /*_REL_OPTIMIZER_H_*/
