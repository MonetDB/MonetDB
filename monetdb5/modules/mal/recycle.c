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
 * Copyright August 2008-2013 MonetDB B.V.
 * All Rights Reserved.
 */

/*
 * @a M. Ivanova, M. Kersten
 * @f recycle
 * The Recycler
 * Just the interface to the recycler.
 *
 * The Recycler is a variation of the interpreter
 * which inspects the variable table for alternative results.
 */
#include "monetdb_config.h"
#include "mal_interpreter.h"
#include "mal_function.h"
#include "mal_listing.h"
#include "mal_recycle.h"
#include "recycle.h"
#include "algebra.h"

/*
 * The recycler is started when the first function is called for its support using recycler.prologue().
 * Upon exit of the last function, the content of the recycle cache is destroyed using recycler.epilogue().
 */
str
RECYCLEdumpWrap(Client cntxt, MalBlkPtr mb, MalStkPtr stk, InstrPtr pci)
{
	stream *s = cntxt->fdout;
	str fname;
	int tp;

	(void) mb;

	if (pci->argc >1)
		tp = * (int*) getArgReference(stk, pci,1);
	else tp = 1;

	if (pci->argc >2){
		fname = * (str*) getArgReference(stk, pci,2);
		s = open_wastream(fname);
		if (s == NULL )
			throw(MAL,"recycle.dumpQ", RUNTIME_FILE_NOT_FOUND" %s", fname);
		if (mnstr_errnr(s)){
			mnstr_close(s);
			throw(MAL,"recycle.dumpQ", RUNTIME_FILE_NOT_FOUND" %s", fname);
		}
	}

	switch(tp){
		case 2:	RECYCLEdumpQPat(s);
				break;
		case 3: RECYCLEdumpDataTrans(s);
				break;
		case 1:
		default:RECYCLEdump(s);
	}

	if( s != cntxt->fdout)
		close_stream(s);
	return MAL_SUCCEED;
}


/*
 * Called to collect statistics at the end of each query.
 */

str
RECYCLEsetAdmission(Client cntxt, MalBlkPtr mb, MalStkPtr stk, InstrPtr p)
{
	int crd;
	(void) cntxt;
	(void) mb;

	admissionPolicy = * (int*) getArgReference(stk,p,1);
	if( p->argc > 2 && admissionPolicy >= ADM_INTEREST ){
		crd = * (int*) getArgReference(stk,p,2);
		if ( crd > 0 )
			recycleMaxInterest = crd + REC_MIN_INTEREST;
	}
	return MAL_SUCCEED;
}

str
RECYCLEsetReuse(Client cntxt, MalBlkPtr mb, MalStkPtr stk, InstrPtr p)
{
	(void) cntxt;
	(void) mb;
	reusePolicy = * (int*) getArgReference(stk, p,1);
	return MAL_SUCCEED;
}

str
RECYCLEsetCache(Client cntxt, MalBlkPtr mb, MalStkPtr stk, InstrPtr p)
{
	(void) cntxt;
	(void) mb;
	rcachePolicy = * (int*) getArgReference(stk, p, 1);
	if( rcachePolicy && p->argc > 2)
		recycleCacheLimit = * (int*) getArgReference(stk, p, 2);
	if( rcachePolicy && p->argc > 3)
		recycleMemory= * (int*) getArgReference(stk, p, 3);
	if( rcachePolicy && p->argc > 4)
		recycleAlpha = * (flt*) getArgReference(stk, p, 4);
	return MAL_SUCCEED;
}

str
RECYCLEgetAdmission(int *p)
{
	*p = admissionPolicy;
	return MAL_SUCCEED;
}

str
RECYCLEgetReuse(int *p)
{
	*p = reusePolicy;
	return MAL_SUCCEED;
}

str
RECYCLEgetCache(int *p)
{
	*p = rcachePolicy;
	return MAL_SUCCEED;
}

/*
 * At the end of the session we have to cleanup the recycle cache.
 */
str
RECYCLEshutdownWrap(Client cntxt, MalBlkPtr mb, MalStkPtr stk, InstrPtr p){

	(void) mb;
	(void) stk;
	(void) p;
	RECYCLEshutdown(cntxt);
	return MAL_SUCCEED;
}
str
RECYCLEmonitor(int *ret, int *p)
{
	(void) ret;
	monitorRecycler = *p;
	return MAL_SUCCEED;
}

str
RECYCLElog(int *ret, str *nm)
{
	stream *s;
	(void) ret;
	recycleLog = GDKstrdup(*nm);
	s = open_wastream(recycleLog);
    if (s){

		mnstr_printf(s,"# Q\t TimeQ(ms)\t");
		if ( monitorRecycler & 2) { /* Current query stat */
			mnstr_printf(s,"InstrQ\t PotRecQ NonBind ");
			mnstr_printf(s,"RecQ\t TotRec\t ");
			mnstr_printf(s,"|| RPadded  RPreset RPtotal ResetTime(ms) RPMem(KB)");
		}

		if ( monitorRecycler & 1) { /* RP stat */
			mnstr_printf(s,"| TotExec\tTotCL\tMem(KB)\tReused\t ");
#ifdef _DEBUG_CACHE_
			mnstr_printf(s,"RPRem\tRPMiss\t ");
#endif
		}

		if ( monitorRecycler & 4) { /* Data transfer stat */
			mnstr_printf(s,"| Trans#\t Trans(KB)\t RecTrans#\t RecTrans(KB)\t ");
		}

		if ( reusePolicy == REUSE_MULTI )
			mnstr_printf(s, "MSFind\t MSCompute\n");
		else mnstr_printf(s,"\n");

		close_stream(s);
	}

	return MAL_SUCCEED;
}

str
RECYCLEstartWrap(Client cntxt, MalBlkPtr mb, MalStkPtr stk, InstrPtr p)
{
	(void) stk;
	(void) p;
	minAggr = ALGminany;
	maxAggr = ALGmaxany;
	return RECYCLEstart(cntxt,mb);
}

str
RECYCLEstopWrap(Client cntxt, MalBlkPtr mb, MalStkPtr stk, InstrPtr p){
	(void) stk;
	(void) p;
	return RECYCLEstop(cntxt,mb);
}
