/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0.  If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 *
 * Copyright 1997 - July 2008 CWI, August 2008 - 2018 MonetDB B.V.
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
#include "opt_oltp.h"
#include "opt_postfix.h"
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
#include "opt_wlc.h"

struct{
	str nme;
	str (*fcn)();
	int calls;
	lng timing;
} codes[] = {
	{"aliases", &OPTaliasesImplementation,0,0},
	{"candidates", &OPTcandidatesImplementation,0,0},
	{"coercions", &OPTcoercionImplementation,0,0},
	{"commonTerms", &OPTcommonTermsImplementation,0,0},
	{"constants", &OPTconstantsImplementation,0,0},
	{"costModel", &OPTcostModelImplementation,0,0},
	{"dataflow", &OPTdataflowImplementation,0,0},
	{"deadcode", &OPTdeadcodeImplementation,0,0},
	{"emptybind", &OPTemptybindImplementation,0,0},
	{"evaluate", &OPTevaluateImplementation,0,0},
	{"garbageCollector", &OPTgarbageCollectorImplementation,0,0},
	{"generator", &OPTgeneratorImplementation,0,0},
	{"inline", &OPTinlineImplementation,0,0},
	{"jit", &OPTjitImplementation,0,0},
	{"json", &OPTjsonImplementation,0,0},
	{"matpack", &OPTmatpackImplementation,0,0},
	{"mergetable", &OPTmergetableImplementation,0,0},
	{"mitosis", &OPTmitosisImplementation,0,0},
	{"multiplex", &OPTmultiplexImplementation,0,0},
	{"oltp", &OPToltpImplementation,0,0},
	{"postfix", &OPTpostfixImplementation,0,0},
	{"profiler", &OPTprofilerImplementation,0,0},
	{"projectionpath", &OPTprojectionpathImplementation,0,0},
	{"pushselect", &OPTpushselectImplementation,0,0},
	{"querylog", &OPTquerylogImplementation,0,0},
	{"reduce", &OPTreduceImplementation,0,0},
	{"remap", &OPTremapImplementation,0,0},
	{"remoteQueries", &OPTremoteQueriesImplementation,0,0},
	{"reorder", &OPTreorderImplementation,0,0},
	{"volcano", &OPTvolcanoImplementation,0,0},
	{"wlc", &OPTwlcImplementation,0,0},
	{0,0,0,0}
};
mal_export str OPTwrapper(Client cntxt, MalBlkPtr mb, MalStkPtr stk, InstrPtr p);

#define OPTIMIZERDEBUG if (0) 

str OPTwrapper (Client cntxt, MalBlkPtr mb, MalStkPtr stk, InstrPtr p){
	str modnme = "(NONE)";
	str fcnnme = "(NONE)";
	Symbol s= NULL;
	int i, actions = 0;
	char optimizer[256];
	str msg = MAL_SUCCEED;
	lng clk;

	if (cntxt->mode == FINISHCLIENT)
		throw(MAL, "optimizer", SQLSTATE(42000) "prematurely stopped client");

	if( p == NULL)
		throw(MAL, "opt_wrapper", SQLSTATE(HY002) "missing optimizer statement");

	if( mb->errors)
		throw(MAL, "opt_wrapper", SQLSTATE(42000) "MAL block contains errors");
	snprintf(optimizer,256,"%s", fcnnme = getFunctionId(p));

	OPTIMIZERDEBUG 
		fprintf(stderr,"=APPLY OPTIMIZER %s\n",fcnnme);
	if( p && p->argc > 1 ){
		if( getArgType(mb,p,1) != TYPE_str ||
			getArgType(mb,p,2) != TYPE_str ||
			!isVarConstant(mb,getArg(p,1)) ||
			!isVarConstant(mb,getArg(p,2))
			)
			throw(MAL, optimizer, SQLSTATE(42000) ILLARG_CONSTANTS);

		if( stk != 0){
			modnme= *getArgReference_str(stk,p,1);
			fcnnme= *getArgReference_str(stk,p,2);
		} else {
			modnme= getArgDefault(mb,p,1);
			fcnnme= getArgDefault(mb,p,2);
		}
		removeInstruction(mb, p);
		s= findSymbol(cntxt->usermodule, putName(modnme),putName(fcnnme));

		if( s == NULL) 
			throw(MAL, optimizer, SQLSTATE(HY002) RUNTIME_OBJECT_UNDEFINED ":%s.%s", modnme, fcnnme);
		mb = s->def;
		stk= 0;
	} else if( p ) 
		removeInstruction(mb, p);

	for (i=0; codes[i].nme; i++)
		if (strcmp(codes[i].nme, optimizer) == 0){
			clk = GDKusec();
			msg = (str)(*(codes[i].fcn))(cntxt, mb, stk, 0);
			codes[i].timing += GDKusec() - clk;
			codes[i].calls++;
			if (msg) 
				throw(MAL, optimizer, SQLSTATE(42000) "Error in optimizer %s", optimizer);
			break;	
		}
	if (codes[i].nme == 0)
		throw(MAL, optimizer, SQLSTATE(HY002) "Optimizer implementation '%s' missing", fcnnme);

	OPTIMIZERDEBUG {
		fprintf(stderr,"=FINISHED %s  %d\n",optimizer, actions);
		fprintFunction(stderr,mb,0,LIST_MAL_DEBUG );
	}
	if ( mb->errors)
		throw(MAL, optimizer, SQLSTATE(42000) PROGRAM_GENERAL ":%s.%s", modnme, fcnnme);
	return MAL_SUCCEED;
}

mal_export str OPTstatistics(Client cntxt, MalBlkPtr mb, MalStkPtr stk, InstrPtr p);

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
		throw(MAL,"optimizer.statistics", SQLSTATE(HY001) MAL_MALLOC_FAIL);
	}
	for( i= 0; codes[i].nme; i++){
		if (BUNappend(n, codes[i].nme, false) != GDK_SUCCEED ||
			BUNappend(c, &codes[i].calls, false) != GDK_SUCCEED ||
			BUNappend(t, &codes[i].timing, false) != GDK_SUCCEED) {
			BBPreclaim(n);
			BBPreclaim(c);
			BBPreclaim(t);
			throw(MAL,"optimizer.statistics", SQLSTATE(HY001) MAL_MALLOC_FAIL);
		}
	}
	BBPkeepref( *nme = n->batCacheid);
	BBPkeepref( *cnt = c->batCacheid);
	BBPkeepref( *time = t->batCacheid);
	return MAL_SUCCEED;
}
