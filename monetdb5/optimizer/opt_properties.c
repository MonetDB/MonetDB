/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0.  If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 *
 * Copyright 1997 - July 2008 CWI, August 2008 - 2021 MonetDB B.V.
 */

/* author M.Kersten
 * This optimizer is used to scale-down columns when properties
 * of the underlying BATs permits it.
 * The effect should be a smaller footprint.
 */
#include "monetdb_config.h"
#include "mal_builder.h"
#include "mal_interpreter.h"
#include "opt_properties.h"

#define properties(TPE) \
	val =  BATmax_skipnil(bn, (void*) (&vmax.val.TPE), 1);\
	if( val != &vmax.val.TPE){\
		/* error */\
	}\
	val =  BATmin_skipnil(bn, (void*) (&vmin.val.TPE), 1);\
	if( val != &vmin.val.TPE){\
		/* error */\
	}


#define keeparound(TPE)\
	setArgType(mb, q, 0, TYPE_void);\
	q = pushArgument(mb, q, getArg(pci,1));\
	q = pushInt(mb, q, vmin.val.TPE);\
	q = pushInt(mb, q, vmax.val.TPE);\
	q = pushLng(mb, q, nils);


static str
PROPstatistics(Client cntxt, MalBlkPtr mb, MalStkPtr stk, InstrPtr pci)
{
	InstrPtr q;
	bat bid;
	BAT *b, *bn;
	int tpe = TYPE_int, basetype= TYPE_any;
	ValRecord vmin, vmax;
	lng nils=5;
	ptr val;

    (void) cntxt;
    (void) mb;
    (void) stk;

	bid = getVarConstant(mb, getArg(pci, 2)).val.ival;
	basetype = getBatType(getArgType(mb, pci,1));

	switch(basetype){
		case TYPE_bte: case TYPE_sht: case TYPE_int: case TYPE_lng: case TYPE_hge: break;
		default: return MAL_SUCCEED;
	}

    if ((b = BATdescriptor(bid)) == NULL){
        throw(MAL, "algebra.max", SQLSTATE(HY002) RUNTIME_OBJECT_MISSING);
	}
	bn = VIEWcreate(b->hseqbase, b);

    // Access a BAT and derive the properties 
	switch(basetype){
	case TYPE_bte: properties(btval); break;
	case TYPE_int: properties(ival); break;
	}
    // Consolidate the type, looking for the minimum type needed for the min/max value;

	// Leave the properties behind in the plan
	q = newStmt(mb, propertiesRef, getRef);
	switch(tpe){
	case TYPE_int: keeparound(ival); break;
	}
	BBPunfix(bn->batCacheid);
	BBPunfix(b->batCacheid);
    return MAL_SUCCEED;
}

str
OPTpropertiesImplementation(Client cntxt, MalBlkPtr mb, MalStkPtr stk, InstrPtr pci)
{
	int i, actions = 0, *originaltype= NULL;
	int limit = mb->stop;
	InstrPtr p, *old = mb->stmt;
	char buf[256];
	lng usec = GDKusec();
	str msg = MAL_SUCCEED;

	(void) stk;
	(void) cntxt;
	(void) pci;

	setVariableScope(mb);
	if ( newMalBlkStmt(mb, mb->ssize) < 0)
		throw(MAL,"optimizer.properties", SQLSTATE(HY013) MAL_MALLOC_FAIL);

	/* Collect properties */
	for (i = 0; i < limit; i++) {
		p = old[i];

		if (p->token == ENDsymbol){
			for(; i<limit; i++)
				if (old[i])
					pushInstruction(mb,old[i]);
			break;
		}
		pushInstruction(mb,p);

		// Phase I, add the properties of the BATs to the plan
		if( getModuleId(p) == propertiesRef && ( getFunctionId(p) == bindRef || getFunctionId(p) == bindidxRef)){
			(void) PROPstatistics(cntxt, mb, stk, p);
		}
	}

	GDKfree(old);
	GDKfree(originaltype);
    /* Defense line against incorrect plans */
	msg = chkTypes(cntxt->usermodule, mb, FALSE);
	if (!msg)
		msg = chkFlow(mb);
	if (!msg)
		msg = chkDeclarations(mb);
    /* keep all actions taken as a post block comment */
	usec = GDKusec()- usec;
    snprintf(buf,256,"%-20s actions=%2d time=" LLFMT " usec","properties",actions, usec);
    newComment(mb,buf);
	if( actions > 0)
		addtoMalBlkHistory(mb);
	return msg;
}
