/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0.  If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 *
 * Copyright 1997 - July 2008 CWI, August 2008 - 2021 MonetDB B.V.
 */

/*
 * @f sql_scenario
 * @t SQL catwalk management
 * @a N. Nes, M.L. Kersten
 * @+ SQL scenario
 * The SQL scenario implementation is a derivative of the MAL session scenario.
 *
 * It is also the first version that uses state records attached to
 * the client record. They are initialized as part of the initialization
 * phase of the scenario.
 *
 */
/*
 * @+ Scenario routines
 * Before we are can process SQL statements the global catalog
 * should be initialized. Thereafter, each time a client enters
 * we update its context descriptor to denote an SQL scenario.
 */
#include "monetdb_config.h"
#include "mal_backend.h"
#include "sql_assert.h"
#include "sql_scenario.h"
/*
 * Assertion errors detected during the execution of a code block
 * raise an exception. A debugger dump is generated upon request
 * to ease debugging.
 */
static inline str
do_assert(bool flg, const char *msg)
{
	if (flg) {
		if (strlen(msg) > 6 &&
		    msg[5] == '!' &&
		    (isdigit((unsigned char) msg[0]) ||
		     isupper((unsigned char) msg[0])) &&
		    (isdigit((unsigned char) msg[1]) ||
		     isupper((unsigned char) msg[1])) &&
		    (isdigit((unsigned char) msg[2]) ||
		     isupper((unsigned char) msg[2])) &&
		    (isdigit((unsigned char) msg[3]) ||
		     isupper((unsigned char) msg[3])) &&
		    (isdigit((unsigned char) msg[4]) ||
		     isupper((unsigned char) msg[4])))
			throw(SQL, "assert", "%s", msg); /* includes state */
		throw(SQL, "assert", SQLSTATE(M0M29) "%s", msg);
	}
	return MAL_SUCCEED;
}
str
SQLassert(Client cntxt, MalBlkPtr mb, MalStkPtr stk, InstrPtr pci)
{
	(void) cntxt;
	(void) mb;
	return do_assert((bool) *getArgReference_bit(stk, pci, 1),
			 *getArgReference_str(stk, pci, 2));
}

str
SQLassertInt(Client cntxt, MalBlkPtr mb, MalStkPtr stk, InstrPtr pci)
{
	(void) cntxt;
	(void) mb;
	return do_assert((bool) *getArgReference_int(stk, pci, 1),
			 *getArgReference_str(stk, pci, 2));
}

str
SQLassertLng(Client cntxt, MalBlkPtr mb, MalStkPtr stk, InstrPtr pci)
{
	(void) cntxt;
	(void) mb;
	return do_assert((bool) *getArgReference_lng(stk, pci, 1),
			 *getArgReference_str(stk, pci, 2));
}

#ifdef HAVE_HGE
str
SQLassertHge(Client cntxt, MalBlkPtr mb, MalStkPtr stk, InstrPtr pci)
{
	(void) cntxt;
	(void) mb;
	return do_assert((bool) *getArgReference_hge(stk, pci, 1),
			 *getArgReference_str(stk, pci, 2));
}
#endif
