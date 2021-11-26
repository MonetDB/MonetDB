/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0.  If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 *
 * Copyright 1997 - July 2008 CWI, August 2008 - 2021 MonetDB B.V.
 */

#ifndef _COPYINTO_H_
#define _COPYINTO_H_

#include "mal_client.h"
#include "sql_mvc.h"

extern str mvc_import_table(Client cntxt, BAT ***bats, mvc *c, bstream *s, sql_table *t, const char *sep, const char *rsep, const char *ssep, const char *ns, lng nr, lng offset, int best, bool from_stdin, bool escape, bool append_directly);

mal_export str COPYrejects(Client cntxt, MalBlkPtr mb, MalStkPtr stk, InstrPtr pci);
mal_export str COPYrejects_clear(Client cntxt, MalBlkPtr mb, MalStkPtr stk, InstrPtr pci);

#endif
