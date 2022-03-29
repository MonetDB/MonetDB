/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0.  If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 *
 * Copyright 1997 - July 2008 CWI, August 2008 - 2022 MonetDB B.V.
 */

#include "monetdb_config.h"
#include "mal_backend.h"
#include "sql_strimps.h"
#include "sql_histogram.h"

str
sql_createhistogram(Client cntxt, MalBlkPtr mb, MalStkPtr stk, InstrPtr pci)
{
	BAT *b;
	gdk_return res;
	str msg = MAL_SUCCEED;

	if ((msg = sql_load_bat(cntxt, mb, stk, pci, "sql.createhistogram", &b)) != MAL_SUCCEED)
		return msg;

	res = HISTOGRAMcreate(b);
	BBPunfix(b->batCacheid);
	if (res != GDK_SUCCEED)
		throw(SQL, "sql.createhistogram", GDK_EXCEPTION);

	return MAL_SUCCEED;
}

str
sql_printhistogram(Client cntxt, MalBlkPtr mb, MalStkPtr stk, InstrPtr pci)
{
	BAT *b;
	str msg = MAL_SUCCEED, *res = getArgReference_str(stk, pci, 0);

	if ((msg = sql_load_bat(cntxt, mb, stk, pci, "sql.printhistogram", &b)) != MAL_SUCCEED)
		return msg;

	*res = HISTOGRAMprint(b);
	BBPunfix(b->batCacheid);
	if (!*res)
		throw(SQL, "sql.printhistogram", GDK_EXCEPTION);

	return MAL_SUCCEED;
}
