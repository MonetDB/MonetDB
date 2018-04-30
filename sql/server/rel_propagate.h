/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0.  If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 *
 * Copyright 1997 - July 2008 CWI, August 2008 - 2018 MonetDB B.V.
 */

#ifndef _REL_PROPAGATE_H_
#define _REL_PROPAGATE_H_

#include "sql_relation.h"
#include "sql_mvc.h"

extern sql_rel* rel_propagate(mvc *sql, sql_rel *rel, int *changes);

#endif //_REL_PROPAGATE_H_
