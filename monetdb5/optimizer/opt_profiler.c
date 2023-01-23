/*
 * SPDX-License-Identifier: MPL-2.0
 *
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0.  If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 *
 * Copyright 1997 - July 2008 CWI, August 2008 - 2023 MonetDB B.V.
 */

/*
 * Collect properties for beautified variable rendering
 * All variables are tagged with the schema.table.column name if possible.
 */

#include "monetdb_config.h"
#include "mal_instruction.h"
#include "mal_profiler.h"
#include "opt_prelude.h"
#include "opt_profiler.h"

str
OPTprofilerImplementation(Client cntxt, MalBlkPtr mb, MalStkPtr stk, InstrPtr pci)
{
	int i, actions = 0;
	InstrPtr p;
	str msg = MAL_SUCCEED;

	(void) stk;
	(void) cntxt;
	/* we only need the beautified version if we plan to emit events */
	if(profilerStatus == 0 )
		goto wrapup;

	for( i=0; i< mb->stop; i++){
		p= getInstrPtr(mb,i);
		if( p == NULL)
			continue;
		if ( getModuleId(p) == NULL || getFunctionId(p) == NULL)
			continue;
		if( getModuleId(p)== sqlRef && (getFunctionId(p)== bindRef || getFunctionId(p) == bindidxRef)){
			getVarSTC(mb,getArg(p,0)) = i;
		} else
		if( getModuleId(p)== sqlRef && getFunctionId(p)== tidRef){
			getVarSTC(mb,getArg(p,0)) = i;
		} else
		if( getModuleId(p)== sqlRef && (getFunctionId(p)== deltaRef || getFunctionId(p) == subdeltaRef)){
			// inherit property of first argument
			getVarSTC(mb,getArg(p,0)) = getVarSTC(mb,getArg(p,1));
		} else
		if( getModuleId(p)== sqlRef && getFunctionId(p)== projectdeltaRef){
			getVarSTC(mb,getArg(p,0)) = getVarSTC(mb,getArg(p,1));
		} else
		if( getModuleId(p)== algebraRef && getFunctionId(p)== projectionRef){
			getVarSTC(mb,getArg(p,0)) = getVarSTC(mb,getArg(p,p->argc-1));
		} else
		if( getModuleId(p)== algebraRef &&
			(getFunctionId(p)== selectRef ||
			 getFunctionId(p) == thetaselectRef ||
			 getFunctionId(p) == selectNotNilRef) ){
			getVarSTC(mb,getArg(p,0)) = getVarSTC(mb,getArg(p,p->retc));
		} else
		if( getModuleId(p)== algebraRef && getFunctionId(p)== likeselectRef){
			getVarSTC(mb,getArg(p,0)) = getVarSTC(mb,getArg(p,p->retc));
		} else
		if( getModuleId(p)== algebraRef &&
			( getFunctionId(p)== joinRef ||
			  getFunctionId(p) == leftjoinRef ||
			  getFunctionId(p) == thetajoinRef ||
			  getFunctionId(p) == bandjoinRef ||
			  getFunctionId(p) == rangejoinRef )){
				getVarSTC(mb,getArg(p,0)) = getVarSTC(mb,getArg(p,p->retc));
				getVarSTC(mb,getArg(p,1)) = getVarSTC(mb,getArg(p,p->retc +1));
		} else
		if( getModuleId(p)== matRef && getFunctionId(p)== packIncrementRef){
			getVarSTC(mb,getArg(p,0)) = getVarSTC(mb,getArg(p,1));
		}
	}
	actions = 1;
	/* Defense line against incorrect plans */
	/* Plan remains unaffected */
	// msg = chkTypes(cntxt->usermodule, mb, FALSE);
	// if (!msg)
	//	msg = chkFlow(mb);
	// if (!msg)
	// 	msg = chkDeclarations(mb);
wrapup:
	/* keep actions taken as a fake argument*/
	(void) pushInt(mb, pci, actions);
	return msg;
}
