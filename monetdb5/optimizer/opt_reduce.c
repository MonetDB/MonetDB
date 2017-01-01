/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0.  If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 *
 * Copyright 1997 - July 2008 CWI, August 2008 - 2017 MonetDB B.V.
 */

#include "monetdb_config.h"
#include "opt_reduce.h"
#include "mal_interpreter.h"

int
OPTreduceImplementation(Client cntxt, MalBlkPtr mb, MalStkPtr stk, InstrPtr p)
{
	int actions = 0;
	char buf[256];
	lng usec = GDKusec();
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
        //chkTypes(cntxt->fdout, cntxt->nspace, mb, FALSE);
        //chkFlow(cntxt->fdout, mb);
        //chkDeclarations(cntxt->fdout, mb);
    //}
    /* keep all actions taken as a post block comment */
    snprintf(buf,256,"%-20s actions=%2d time=" LLFMT " usec","reduce",actions,GDKusec() - usec);
    newComment(mb,buf);

	return actions;
}
