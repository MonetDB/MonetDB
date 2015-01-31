/*
 * The contents of this file are subject to the MonetDB Public License
 * Version 1.1 (the "License"); you may not use this file except in
 * compliance with the License. You may obtain a copy of the License at
 * http://www.monetdb.org/Legal/MonetDBLicense
 *
 * Software distributed under the License is distributed on an "AS IS"
 * basis, WITHOUT WARRANTY OF ANY KIND, either express or implied. See the
 * License for the specific language governing rights and limitations
 * under the License.
 *
 * The Original Code is the MonetDB Database System.
 *
 * The Initial Developer of the Original Code is CWI.
 * Portions created by CWI are Copyright (C) 1997-July 2008 CWI.
 * Copyright August 2008-2015 MonetDB B.V.
 * All Rights Reserved.
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
	(void) cntxt;
	(void) mb;
	if (*flg) {
		const char *sqlstate = "M0M29!";
		/* mdbDump(mb,stk,pci); */
		if (strlen(*msg) > 6 && (*msg)[5] == '!' && (('0' <= (*msg)[0] && (*msg)[0] <= '9') || ('A' <= (*msg)[0] && (*msg)[0] <= 'Z')) && (('0' <= (*msg)[1] && (*msg)[1] <= '9') || ('A' <= (*msg)[1] && (*msg)[1] <= 'Z')) &&
		    (('0' <= (*msg)[2] && (*msg)[2] <= '9') || ('A' <= (*msg)[2] && (*msg)[2] <= 'Z')) && (('0' <= (*msg)[3] && (*msg)[3] <= '9') || ('A' <= (*msg)[3] && (*msg)[3] <= 'Z')) && (('0' <= (*msg)[4] && (*msg)[4] <= '9') ||
																								 ('A' <= (*msg)[4] && (*msg)[4] <= 'Z')))
			sqlstate = "";
		throw(SQL, "assert", "%s%s", sqlstate, *msg);
	}
	return MAL_SUCCEED;
}

str
SQLassertInt(Client cntxt, MalBlkPtr mb, MalStkPtr stk, InstrPtr pci)
{
	int *flg = getArgReference_int(stk, pci, 1);
	str *msg = getArgReference_str(stk, pci, 2);
	(void) cntxt;
	(void) mb;
	if (*flg) {
		const char *sqlstate = "M0M29!";
		/* mdbDump(mb,stk,pci); */
		if (strlen(*msg) > 6 && (*msg)[5] == '!' && (('0' <= (*msg)[0] && (*msg)[0] <= '9') || ('A' <= (*msg)[0] && (*msg)[0] <= 'Z')) && (('0' <= (*msg)[1] && (*msg)[1] <= '9') || ('A' <= (*msg)[1] && (*msg)[1] <= 'Z')) &&
		    (('0' <= (*msg)[2] && (*msg)[2] <= '9') || ('A' <= (*msg)[2] && (*msg)[2] <= 'Z')) && (('0' <= (*msg)[3] && (*msg)[3] <= '9') || ('A' <= (*msg)[3] && (*msg)[3] <= 'Z')) && (('0' <= (*msg)[4] && (*msg)[4] <= '9') ||
																								 ('A' <= (*msg)[4] && (*msg)[4] <= 'Z')))
			sqlstate = "";
		throw(SQL, "assert", "%s%s", sqlstate, *msg);
	}
	return MAL_SUCCEED;
}

str
SQLassertWrd(Client cntxt, MalBlkPtr mb, MalStkPtr stk, InstrPtr pci)
{
	wrd *flg = getArgReference_wrd(stk, pci, 1);
	str *msg = getArgReference_str(stk, pci, 2);
	(void) cntxt;
	(void) mb;
	if (*flg) {
		const char *sqlstate = "M0M29!";
		/* mdbDump(mb,stk,pci); */
		if (strlen(*msg) > 6 && (*msg)[5] == '!' && (('0' <= (*msg)[0] && (*msg)[0] <= '9') || ('A' <= (*msg)[0] && (*msg)[0] <= 'Z')) && (('0' <= (*msg)[1] && (*msg)[1] <= '9') || ('A' <= (*msg)[1] && (*msg)[1] <= 'Z')) &&
		    (('0' <= (*msg)[2] && (*msg)[2] <= '9') || ('A' <= (*msg)[2] && (*msg)[2] <= 'Z')) && (('0' <= (*msg)[3] && (*msg)[3] <= '9') || ('A' <= (*msg)[3] && (*msg)[3] <= 'Z')) && (('0' <= (*msg)[4] && (*msg)[4] <= '9') ||
																								 ('A' <= (*msg)[4] && (*msg)[4] <= 'Z')))
			sqlstate = "";
		throw(SQL, "assert", "%s%s", sqlstate, *msg);
	}
	return MAL_SUCCEED;
}

str
SQLassertLng(Client cntxt, MalBlkPtr mb, MalStkPtr stk, InstrPtr pci)
{
	lng *flg = getArgReference_lng(stk, pci, 1);
	str *msg = getArgReference_str(stk, pci, 2);
	(void) cntxt;
	(void) mb;
	if (*flg) {
		const char *sqlstate = "M0M29!";
		/* mdbDump(mb,stk,pci); */
		if (strlen(*msg) > 6 && (*msg)[5] == '!' && (('0' <= (*msg)[0] && (*msg)[0] <= '9') || ('A' <= (*msg)[0] && (*msg)[0] <= 'Z')) && (('0' <= (*msg)[1] && (*msg)[1] <= '9') || ('A' <= (*msg)[1] && (*msg)[1] <= 'Z')) &&
		    (('0' <= (*msg)[2] && (*msg)[2] <= '9') || ('A' <= (*msg)[2] && (*msg)[2] <= 'Z')) && (('0' <= (*msg)[3] && (*msg)[3] <= '9') || ('A' <= (*msg)[3] && (*msg)[3] <= 'Z')) && (('0' <= (*msg)[4] && (*msg)[4] <= '9') ||
																								 ('A' <= (*msg)[4] && (*msg)[4] <= 'Z')))
			sqlstate = "";
		throw(SQL, "assert", "%s%s", sqlstate, *msg);
	}
	return MAL_SUCCEED;
}

#ifdef HAVE_HGE
str
SQLassertHge(Client cntxt, MalBlkPtr mb, MalStkPtr stk, InstrPtr pci){
	hge *flg = (hge*) getArgReference(stk,pci, 1);
	str *msg = (str*) getArgReference(stk,pci, 2);
	(void) cntxt;
	(void)mb;
	if (*flg){
		const char *sqlstate = "M0M29!";
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
		throw(SQL, "assert", "%s%s", sqlstate, *msg);
	}
	return MAL_SUCCEED;
}
#endif
