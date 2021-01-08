/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0.  If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 *
 * Copyright 1997 - July 2008 CWI, August 2008 - 2021 MonetDB B.V.
 */

#ifndef _SQL_DECIMAL_H
#define _SQL_DECIMAL_H

#include "sql_mem.h"
#include "sql_types.h"
#include "gdk.h"

#ifdef HAVE_HGE
#define DEC_TPE hge
#else
#define DEC_TPE lng
#endif

extern DEC_TPE decimal_from_str(char *dec, int* digits, int* scale, int* has_errors);
extern char * decimal_to_str(sql_allocator *sa, DEC_TPE v, sql_subtype *t);
DEC_TPE scale2value(int scale);

#endif /* _SQL_DECIMAL_H */
