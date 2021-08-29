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
 * The effect should be a smaller intermediate footprint.
 */
#include "monetdb_config.h"
#include "mal_builder.h"
#include "mal_interpreter.h"
#include "opt_properties.h"

#define properties(TPE) \
	val =  BATmax_skipnil(bn, (void*) (&vmax.val.TPE), 1);\
	vmax.vtype = basetype;\
	restype =scaledown(&vmax);\
	if( val != &vmax.val.TPE){\
		assert(0);\
		/* error */\
	}\
	val =  BATmin_skipnil(bn, (void*) (&vmin.val.TPE), 1);\
	vmin.vtype = basetype;\
	restype =scaledown(&vmin);\
	if( val != &vmin.val.TPE){\
		/* error */\
	}


#define keeparound(T, TPE)\
	q = newStmt(mb, propertiesRef, infoRef);\
	setArgType(mb, q, 0, TYPE_void);\
	q = pushArgument(mb, q, getArg(pci,1));\
	q = push##T(mb, q, vmin.val.TPE);\
	q = push##T(mb, q, vmax.val.TPE);\
	q = pushLng(mb, q, cnt);\
	q->token = REMsymbol


static int
scaledown(ValPtr v){
	switch(v->vtype){
#ifdef HAVE_HGE
	case TYPE_hge:
				if( v->val.lval > - INT_MAX && v->val.lval < INT_MAX){
					v->vtype = TYPE_int;
				} else break;
#endif
	case TYPE_lng:
				if( v->val.lval > - INT_MAX && v->val.lval < INT_MAX){
					v->vtype = TYPE_int;
				} else break;
	case TYPE_int:
				if( v->val.ival > -(1<<16) && v->val.ival < 1<<16){
					v->vtype = TYPE_sht;
				} else break;
	case TYPE_sht:
				if( v->val.shval > -128 && v->val.shval <128){
					v->vtype = TYPE_bte;
				} else break;
	case TYPE_bte:
		break;
	}
	return v->vtype;
}

static str
PROPstatistics(Client cntxt, MalBlkPtr mb, MalStkPtr stk, InstrPtr pci)
{
	InstrPtr q;
	bat bid;
	BAT *b, *bn;
	int restype = TYPE_any, basetype= TYPE_any;
	ValRecord vmin, vmax;
	lng cnt = 0;
	ptr val;

    (void) cntxt;
    (void) mb;
    (void) stk;

	vmin.val.lval = vmax.val.lval = 0;
	vmin.vtype = vmax.vtype = TYPE_void;
	bid = getVarConstant(mb, getArg(pci, 2)).val.ival;
	basetype = getBatType(getArgType(mb, pci,1));

	switch(basetype){
		case TYPE_bte: case TYPE_sht: case TYPE_int: case TYPE_lng: case TYPE_hge: case TYPE_flt: case TYPE_dbl: case TYPE_oid:break;
		default: return MAL_SUCCEED;
	}

    if ((b = BATdescriptor(bid)) == NULL){
        throw(MAL, "algebra.max", SQLSTATE(HY002) RUNTIME_OBJECT_MISSING);
	}
	bn = VIEWcreate(b->hseqbase, b);
	cnt = (lng) BATcount(bn);

    // Access a BAT and derive the properties 
	switch(basetype){
		case TYPE_bte: properties(btval); break;
		case TYPE_sht: properties(shval); break;
		case TYPE_int: properties(ival); break;
		case TYPE_lng: properties(lval); break;
#ifdef HAVE_HGE
		case TYPE_hge: properties(hval); break;
#endif
		case TYPE_flt: properties(fval); break;
		case TYPE_dbl: properties(dval); break;
		case TYPE_oid: properties(oval); break;
	}
    // Consolidate the type, looking for the minimum type needed for the min/max value;

	// Leave the properties behind in the plan
	switch(restype){
		case TYPE_bte: keeparound(Bte, btval); break;
		case TYPE_sht: keeparound(Sht, shval); break;
		case TYPE_int: keeparound(Int, ival); break;
		case TYPE_lng: keeparound(Lng, lval); break;
#ifdef HAVE_HGE
		case TYPE_hge: keeparound(Hge, hval); break;
#endif
		case TYPE_flt: keeparound(Flt, fval); break;
		case TYPE_dbl: keeparound(Dbl, dval); break;
		case TYPE_oid: keeparound(Oid, oval); break;
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
