/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0.  If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 *
 * Copyright 1997 - July 2008 CWI, August 2008 - 2017 MonetDB B.V.
 */

#ifndef SQL_TIMESTAMPS_H
#define SQL_TIMESTAMPS_H

#include "monetdb_config.h"
#include "sql.h"

sql5_export str convert_atom_into_unix_timestamp(atom *a, lng* res);

#endif //SQL_TIMESTAMPS_H
