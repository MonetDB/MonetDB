/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0.  If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 *
 * Copyright 1997 - July 2008 CWI, August 2008 - 2021 MonetDB B.V.
 */

#include "monetdb_config.h"
#include "opt_reduce.h"
#include "mal_interpreter.h"

str
OPTreduceImplementation(Client cntxt, MalBlkPtr mb, MalStkPtr stk, InstrPtr p)
{
	int actions = 0;
	char buf[256];
	lng usec = GDKusec();
	str msg = MAL_SUCCEED;

	(void)cntxt;
	(void)stk;
	(void) p;

	actions = mb->vtop;
	trimMalVariables(mb,0);
	actions = actions - mb->vtop;

    /* Defense line against incorrect plans */
	/* plan is not changed */
	/* plan is not changed */
    //if( actions > 0){
        //msg = chkTypes(cntxt->usermodule, mb, FALSE);
        //if (!msg)
	//	msg = chkFlow(mb);
        //if (!msg)
        // 	msg = chkDeclarations(mb);
    //}
    /* keep all actions taken as a post block comment */
	usec = GDKusec()- usec;
    snprintf(buf,256,"%-20s actions=%2d time=" LLFMT " usec","reduce",actions, usec);
    newComment(mb,buf);
	if( actions > 0)
		addtoMalBlkHistory(mb);

	return msg;
}
