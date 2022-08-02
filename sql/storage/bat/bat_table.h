/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0.  If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 *
 * Copyright 1997 - July 2008 CWI, August 2008 - 2020 MonetDB B.V.
 */

#ifndef BAT_TABLE_H
#define BAT_TABLE_H

#include "sql_storage.h"
#include "bat_storage.h"
#include "bat_utils.h"

/* initialize bat storage call back functions interface */
extern void bat_table_init( table_functions *tf );

#endif /*BAT_TABLE_H*/
