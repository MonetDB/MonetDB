/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0.  If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 *
 * Copyright 2008-2015 MonetDB B.V.
 */

#ifndef RESTRICT_TABLE_H
#define RESTRICT_TABLE_H

#include "sql_storage.h"

/* initialize restrict storage call back functions interface */
extern int su_table_init( table_functions *tf );
extern int ro_table_init( table_functions *tf );
extern int suro_table_init( table_functions *tf );

#endif /*RESTRICT_TABLE_H*/
