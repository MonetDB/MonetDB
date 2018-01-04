/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0.  If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 *
 * Copyright 1997 - July 2008 CWI, August 2008 - 2018 MonetDB B.V.
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
#include "sql_scenario.h"
/*
 * Assertion errors detected during the execution of a code block
 * raises an exception. An debugger dump is generated upon request
 * to ease debugging.
 */
str
SQLassert(Client cntxt, MalBlkPtr mb, MalStkPtr stk, InstrPtr pci)
{
	bit *flg = getArgReference_bit(stk, pci, 1);
	str *msg = getArgReference_str(stk, pci, 2);
	const char *sqlstate = SQLSTATE(M0M29) ;
	(void) sqlstate;
	(void) cntxt;
	(void) mb;
	if (*flg) {
		/* mdbDump(mb,stk,pci); */
		if (strlen(*msg) > 6 &&
		    (*msg)[5] == '!' &&
		    (('0' <= (*msg)[0] && (*msg)[0] <= '9') ||
		     ('A' <= (*msg)[0] && (*msg)[0] <= 'Z')) &&
		    (('0' <= (*msg)[1] && (*msg)[1] <= '9') ||
		     ('A' <= (*msg)[1] && (*msg)[1] <= 'Z')) &&
		    (('0' <= (*msg)[2] && (*msg)[2] <= '9') ||
		     ('A' <= (*msg)[2] && (*msg)[2] <= 'Z')) &&
		    (('0' <= (*msg)[3] && (*msg)[3] <= '9') ||
		     ('A' <= (*msg)[3] && (*msg)[3] <= 'Z')) &&
		    (('0' <= (*msg)[4] && (*msg)[4] <= '9') ||
		     ('A' <= (*msg)[4] && (*msg)[4] <= 'Z')))
			sqlstate = "";
		throw(SQL, "assert", SQLSTATE(M0M29) "%s", *msg);
	}
	return MAL_SUCCEED;
}

str
SQLassertInt(Client cntxt, MalBlkPtr mb, MalStkPtr stk, InstrPtr pci)
{
	int *flg = getArgReference_int(stk, pci, 1);
	str *msg = getArgReference_str(stk, pci, 2);
	const char *sqlstate = SQLSTATE(M0M29) ;
	(void) sqlstate;
	(void) cntxt;
	(void) mb;
	if (*flg) {
		/* mdbDump(mb,stk,pci); */
		if (strlen(*msg) > 6 &&
		    (*msg)[5] == '!' &&
		    (('0' <= (*msg)[0] && (*msg)[0] <= '9') ||
		     ('A' <= (*msg)[0] && (*msg)[0] <= 'Z')) &&
		    (('0' <= (*msg)[1] && (*msg)[1] <= '9') ||
		     ('A' <= (*msg)[1] && (*msg)[1] <= 'Z')) &&
		    (('0' <= (*msg)[2] && (*msg)[2] <= '9') ||
		     ('A' <= (*msg)[2] && (*msg)[2] <= 'Z')) &&
		    (('0' <= (*msg)[3] && (*msg)[3] <= '9') ||
		     ('A' <= (*msg)[3] && (*msg)[3] <= 'Z')) &&
		    (('0' <= (*msg)[4] && (*msg)[4] <= '9') ||
		     ('A' <= (*msg)[4] && (*msg)[4] <= 'Z')))
			sqlstate = "";
		throw(SQL, "assert", SQLSTATE(M0M29) "%s", *msg);
	}
	return MAL_SUCCEED;
}

str
SQLassertLng(Client cntxt, MalBlkPtr mb, MalStkPtr stk, InstrPtr pci)
{
	lng *flg = getArgReference_lng(stk, pci, 1);
	str *msg = getArgReference_str(stk, pci, 2);
	const char *sqlstate = SQLSTATE(M0M29) ;
	(void) sqlstate;
	(void) cntxt;
	(void) mb;
	if (*flg) {
		/* mdbDump(mb,stk,pci); */
		if (strlen(*msg) > 6 &&
		    (*msg)[5] == '!' &&
		    (('0' <= (*msg)[0] && (*msg)[0] <= '9') ||
		     ('A' <= (*msg)[0] && (*msg)[0] <= 'Z')) &&
		    (('0' <= (*msg)[1] && (*msg)[1] <= '9') ||
		     ('A' <= (*msg)[1] && (*msg)[1] <= 'Z')) &&
		    (('0' <= (*msg)[2] && (*msg)[2] <= '9') ||
		     ('A' <= (*msg)[2] && (*msg)[2] <= 'Z')) &&
		    (('0' <= (*msg)[3] && (*msg)[3] <= '9') ||
		     ('A' <= (*msg)[3] && (*msg)[3] <= 'Z')) &&
		    (('0' <= (*msg)[4] && (*msg)[4] <= '9') ||
		     ('A' <= (*msg)[4] && (*msg)[4] <= 'Z')))
			sqlstate = "";
		throw(SQL, "assert", SQLSTATE(M0M29) "%s", *msg);
	}
	return MAL_SUCCEED;
}

#ifdef HAVE_HGE
str
SQLassertHge(Client cntxt, MalBlkPtr mb, MalStkPtr stk, InstrPtr pci){
	hge *flg = getArgReference_hge(stk,pci, 1);
	str *msg = getArgReference_str(stk,pci, 2);
	(void) cntxt;
	(void)mb;
	if (*flg){
		const char *sqlstate = SQLSTATE(M0M29) ;
		(void) sqlstate;
		/* mdbDump(mb,stk,pci);*/
		if (strlen(*msg) > 6 && (*msg)[5] == '!' &&
		    (('0' <= (*msg)[0] && (*msg)[0] <= '9') ||
		     ('A' <= (*msg)[0] && (*msg)[0] <= 'Z')) &&
		    (('0' <= (*msg)[1] && (*msg)[1] <= '9') ||
		     ('A' <= (*msg)[1] && (*msg)[1] <= 'Z')) &&
		    (('0' <= (*msg)[2] && (*msg)[2] <= '9') ||
		     ('A' <= (*msg)[2] && (*msg)[2] <= 'Z')) &&
		    (('0' <= (*msg)[3] && (*msg)[3] <= '9') ||
		     ('A' <= (*msg)[3] && (*msg)[3] <= 'Z')) &&
		    (('0' <= (*msg)[4] && (*msg)[4] <= '9') ||
		     ('A' <= (*msg)[4] && (*msg)[4] <= 'Z')))
			sqlstate = "";
		throw(SQL, "assert", SQLSTATE(M0M29) "%s", *msg);
	}
	return MAL_SUCCEED;
}
#endif
