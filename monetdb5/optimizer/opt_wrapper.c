/*
 * SPDX-License-Identifier: MPL-2.0
 *
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0.  If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 *
 * Copyright 1997 - July 2008 CWI, August 2008 - 2023 MonetDB B.V.
 */

/*  author M.L. Kersten
 * The optimizer wrapper code is the interface to the MAL optimizer calls.
 *
 * Before an optimizer is finished, it should leave a clean state behind.
 * Moreover, some information of the optimization step is saved for
 * debugging and analysis.
*/

#include "monetdb_config.h"
#include "mal_listing.h"

/*
 * The optimizer used so far
*/
#include "opt_aliases.h"
#include "opt_bincopyfrom.h"
#include "opt_coercion.h"
#include "opt_commonTerms.h"
#include "opt_candidates.h"
#include "opt_constants.h"
#include "opt_costModel.h"
#include "opt_dataflow.h"
#include "opt_deadcode.h"
#include "opt_emptybind.h"
#include "opt_evaluate.h"
#include "opt_garbageCollector.h"
#include "opt_generator.h"
#include "opt_inline.h"
#include "opt_jit.h"
#include "opt_projectionpath.h"
#include "opt_matpack.h"
#include "opt_json.h"
#include "opt_postfix.h"
#include "opt_mask.h"
#include "opt_for.h"
#include "opt_dict.h"
#include "opt_mergetable.h"
#include "opt_mitosis.h"
#include "opt_multiplex.h"
#include "opt_profiler.h"
#include "opt_pushselect.h"
#include "opt_querylog.h"
#include "opt_reduce.h"
#include "opt_remap.h"
#include "opt_remoteQueries.h"
#include "opt_reorder.h"
#include "opt_volcano.h"
#include "opt_fastpath.h"
#include "opt_strimps.h"
#include "opt_wlc.h"
#include "optimizer_private.h"

// keep the optimizer list sorted
static struct {
	str nme;
	str (*fcn)();
	int calls;
	lng timing;
} codes[] = {
	{"aliases", &OPTaliasesImplementation,0,0},
	{"bincopyfrom", &OPTbincopyfromImplementation,0,0},
	{"candidates", &OPTcandidatesImplementation,0,0},
	{"coercions", &OPTcoercionImplementation,0,0},
	{"commonTerms", &OPTcommonTermsImplementation,0,0},
	{"constants", &OPTconstantsImplementation,0,0},
	{"costModel", &OPTcostModelImplementation,0,0},
	{"dataflow", &OPTdataflowImplementation,0,0},
	{"deadcode", &OPTdeadcodeImplementation,0,0},
	{"defaultfast", &OPTdefaultfastImplementation,0,0},
	{"dict", &OPTdictImplementation,0,0},
	{"emptybind", &OPTemptybindImplementation,0,0},
	{"evaluate", &OPTevaluateImplementation,0,0},
	{"for", &OPTforImplementation,0,0},
	{"garbageCollector", &OPTgarbageCollectorImplementation,0,0},
	{"generator", &OPTgeneratorImplementation,0,0},
	{"inline", &OPTinlineImplementation,0,0},
	{"jit", &OPTjitImplementation,0,0},
	{"json", &OPTjsonImplementation,0,0},
	{"mask", &OPTmaskImplementation,0,0},
	{"matpack", &OPTmatpackImplementation,0,0},
	{"mergetable", &OPTmergetableImplementation,0,0},
	{"minimalfast", &OPTminimalfastImplementation,0,0},
	{"mitosis", &OPTmitosisImplementation,0,0},
	{"multiplex", &OPTmultiplexImplementation,0,0},
	{"postfix", &OPTpostfixImplementation,0,0},
	{"profiler", &OPTprofilerImplementation,0,0},
	{"projectionpath", &OPTprojectionpathImplementation,0,0},
	{"pushselect", &OPTpushselectImplementation,0,0},
	{"querylog", &OPTquerylogImplementation,0,0},
	{"reduce", &OPTreduceImplementation,0,0},
	{"remap", &OPTremapImplementation,0,0},
	{"remoteQueries", &OPTremoteQueriesImplementation,0,0},
	{"reorder", &OPTreorderImplementation,0,0},
	{"strimps", &OPTstrimpsImplementation,0,0},
	{"volcano", &OPTvolcanoImplementation,0,0},
	{"wlc", &OPTwlcImplementation,0,0},
	{0,0,0,0}
};
static int codehash[256];
static MT_Lock codeslock = MT_LOCK_INITIALIZER(codeslock);

static
void fillcodehash(void)
{
	int i, idx;

	for( i=0;  i< 256; i++)
		codehash[i] = -1;
	for (i=0; codes[i].nme; i++){
		idx = (int) codes[i].nme[0];
		if( codehash[idx] == -1 ){
			codehash[idx] = i;
		}
	}
}

str OPTwrapper (Client cntxt, MalBlkPtr mb, MalStkPtr stk, InstrPtr p){
	str modnme = "(NONE)";
	const char *fcnnme = "(NONE)";
	Symbol s= NULL;
	int i;
	str msg = MAL_SUCCEED;
	lng clk;

	// no optimizer starts with a null byte, initialization sets a zero
	if( codehash[0] != -1 || codehash[0] == 0)
		fillcodehash();
	if (cntxt->mode == FINISHCLIENT)
		throw(MAL, "optimizer", SQLSTATE(42000) "prematurely stopped client");

	if( p == NULL)
		throw(MAL, "opt_wrapper", SQLSTATE(HY002) "missing optimizer statement");

	if( mb->errors)
		throw(MAL, "opt_wrapper", SQLSTATE(42000) "MAL block contains errors");
	fcnnme = getFunctionId(p);

	if( p && p->argc > 1 ){
		if( getArgType(mb,p,1) != TYPE_str ||
			getArgType(mb,p,2) != TYPE_str ||
			!isVarConstant(mb,getArg(p,1)) ||
			!isVarConstant(mb,getArg(p,2))
			)
			throw(MAL, getFunctionId(p), SQLSTATE(42000) ILLARG_CONSTANTS);

		if( stk != 0){
			modnme= *getArgReference_str(stk,p,1);
			fcnnme= *getArgReference_str(stk,p,2);
		} else {
			modnme= getArgDefault(mb,p,1);
			fcnnme= getArgDefault(mb,p,2);
		}
		//removeInstruction(mb, p);
		p->token = REMsymbol;
		s= findSymbol(cntxt->usermodule, putName(modnme),putName(fcnnme));

		if( s == NULL)
			throw(MAL, getFunctionId(p), SQLSTATE(HY002) RUNTIME_OBJECT_UNDEFINED "%s.%s", modnme, fcnnme);
		mb = s->def;
		stk= 0;
	} else if( p ){
		p->token = REMsymbol;
	}

	clk = GDKusec();
	const char *id = getFunctionId(p);
	for (i=codehash[*id]; codes[i].nme; i++){
		if (codes[i].nme[0] == *id && strcmp(codes[i].nme, getFunctionId(p)) == 0){
			msg = (str)(*(codes[i].fcn))(cntxt, mb, stk, p);
			clk = GDKusec() - clk;
			MT_lock_set(&codeslock);
			codes[i].timing += clk;
			codes[i].calls++;
			MT_lock_unset(&codeslock);
			p= pushLng(mb, p, clk);
			if (msg) {
				str newmsg = createException(MAL, getFunctionId(p), SQLSTATE(42000) "Error in optimizer %s: %s", getFunctionId(p), msg);
				freeException(msg);
				return newmsg;
			}
			addtoMalBlkHistory(mb);
			break;
		}
	}
	if (codes[i].nme == 0)
		throw(MAL, fcnnme,  SQLSTATE(HY002) "Optimizer implementation '%s' missing", fcnnme);

	if ( mb->errors)
		throw(MAL, fcnnme, SQLSTATE(42000) PROGRAM_GENERAL "%s.%s %s", modnme, fcnnme, mb->errors);
	return MAL_SUCCEED;
}

str
OPTstatistics(Client cntxt, MalBlkPtr mb, MalStkPtr stk, InstrPtr p)
{
	bat  *nme = (bat*) getArgReference_bat(stk, p, 0);
	bat  *cnt = (bat*) getArgReference_bat(stk, p, 1);
	bat  *time = (bat*) getArgReference_bat(stk, p, 2);
	BAT *n, *c, *t;
	int i;

	(void) cntxt;
	(void) mb;
	n = COLnew(0, TYPE_str, 256, TRANSIENT);
	c = COLnew(0, TYPE_int, 256, TRANSIENT);
	t = COLnew(0, TYPE_lng, 256, TRANSIENT);
	if( n == NULL || c == NULL || t == NULL){
		BBPreclaim(n);
		BBPreclaim(c);
		BBPreclaim(t);
		throw(MAL,"optimizer.statistics", SQLSTATE(HY013) MAL_MALLOC_FAIL);
	}
	MT_lock_set(&codeslock);
	for( i= 0; codes[i].nme; i++){
		if (BUNappend(n, codes[i].nme, false) != GDK_SUCCEED ||
			BUNappend(c, &codes[i].calls, false) != GDK_SUCCEED ||
			BUNappend(t, &codes[i].timing, false) != GDK_SUCCEED) {
			MT_lock_unset(&codeslock);
			BBPreclaim(n);
			BBPreclaim(c);
			BBPreclaim(t);
			throw(MAL,"optimizer.statistics", SQLSTATE(HY013) MAL_MALLOC_FAIL);
		}
	}
	MT_lock_unset(&codeslock);
	*nme = n->batCacheid;
	BBPkeepref(n);
	*cnt = c->batCacheid;
	BBPkeepref(c);
	*time = t->batCacheid;
	BBPkeepref(t);
	return MAL_SUCCEED;
}
