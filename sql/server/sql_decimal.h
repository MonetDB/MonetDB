/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0.  If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 *
 * Copyright 1997 - July 2008 CWI, August 2008 - 2018 MonetDB B.V.
 */

#ifndef _SQL_DECIMAL_H
#define _SQL_DECIMAL_H

#include "sql_mem.h"
#include "sql_types.h"
#include "gdk.h"

#ifdef HAVE_HGE
extern hge decimal_from_str(char *dec, char **end);
extern char * decimal_to_str(hge v, sql_subtype *t);
#else
extern lng decimal_from_str(char *dec, char **end);
extern char * decimal_to_str(lng v, sql_subtype *t);
#endif

#endif /* _SQL_DECIMAL_H */

