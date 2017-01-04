/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0.  If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 *
 * Copyright 1997 - July 2008 CWI, August 2008 - 2017 MonetDB B.V.
 */

#ifndef SQL_WLCR_H
#define SQL_WLCR_H

#include <streams.h>
#include <mal.h>
#include <mal_client.h>
#include <sql_mvc.h>
#include <sql_qc.h>

/*
 */
extern str WLCRreplay(Client cntxt, MalBlkPtr mb, MalStkPtr stk, InstrPtr pci);
extern str WLCRsynchronize(Client cntxt, MalBlkPtr mb, MalStkPtr stk, InstrPtr pci);

#endif /*SQL_WLCR_H*/
