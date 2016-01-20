/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0.  If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 *
 * Copyright 1997 - July 2008 CWI, August 2008 - 2016 MonetDB B.V.
 */

/*  author M.L. Kersten
 * The optimizer wrapper code is the interface to the MAL optimizer calls.
 * It prepares the environment for the optimizers to do their work and removes
 * the call itself to avoid endless recursions.
 * 
 * Before an optimizer is finished, it should leave a clean state behind.
 * Moreover, the information of the optimization step is saved for
 * debugging and analysis.
 * 
 * The wrapper expects the optimizers to return the number of
 * actions taken, i.e. number of succesful changes to the code.

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
#include "opt_evaluate.h"
#include "opt_factorize.h"
#include "opt_garbageCollector.h"
#include "opt_generator.h"
#include "opt_inline.h"
#include "opt_projectionpath.h"
#include "opt_matpack.h"
#include "opt_json.h"
#include "opt_mergetable.h"
#include "opt_mitosis.h"
#include "opt_multiplex.h"
#include "opt_profiler.h"
#include "opt_pushselect.h"
#include "opt_qep.h"
#include "opt_querylog.h"
#include "opt_recycler.h"
#include "opt_reduce.h"
#include "opt_remap.h"
#include "opt_remoteQueries.h"
#include "opt_reorder.h"
#include "opt_statistics.h"

struct{
	str nme;
	int (*fcn)();
} codes[] = {
	{"aliases", &OPTaliasesImplementation},
	{"coercions", &OPTcoercionImplementation},
	{"commonTerms", &OPTcommonTermsImplementation},
	{"candidates", &OPTcandidatesImplementation},
	{"constants", &OPTconstantsImplementation},
	{"costModel", &OPTcostModelImplementation},
	{"dataflow", &OPTdataflowImplementation},
	{"deadcode", &OPTdeadcodeImplementation},
	{"dumpQEP", &OPTdumpQEPImplementation},
	{"evaluate", &OPTevaluateImplementation},
	{"factorize", &OPTfactorizeImplementation},
	{"garbageCollector", &OPTgarbageCollectorImplementation},
	{"generator", &OPTgeneratorImplementation},
	{"inline", &OPTinlineImplementation},
	{"projectionpath", &OPTprojectionpathImplementation},
	{"matpack", &OPTmatpackImplementation},
	{"json", &OPTjsonImplementation},
	{"mergetable", &OPTmergetableImplementation},
	{"mitosis", &OPTmitosisImplementation},
	{"multiplex", &OPTmultiplexImplementation},
	{"profiler", &OPTprofilerImplementation},
	{"pushselect", &OPTpushselectImplementation},
	{"querylog", &OPTquerylogImplementation},
	{"recycler", &OPTrecyclerImplementation},
	{"reduce", &OPTreduceImplementation},
	{"remap", &OPTremapImplementation},
	{"remoteQueries", &OPTremoteQueriesImplementation},
	{"reorder", &OPTreorderImplementation},
	{0,0}
};
opt_export str OPTwrapper(Client cntxt, MalBlkPtr mb, MalStkPtr stk, InstrPtr p);

#define OPTIMIZERDEBUG if (0) 

str OPTwrapper (Client cntxt, MalBlkPtr mb, MalStkPtr stk, InstrPtr p){
	str modnme = "(NONE)";
	str fcnnme = 0;
	str msg= MAL_SUCCEED;
	Symbol s= NULL;
	lng t,clk= GDKusec();
	int i, actions = 0;
	char optimizer[256];
	InstrPtr q;

	if( p == NULL)
		throw(MAL, "opt_wrapper", "missing optimizer statement");
	snprintf(optimizer,256,"%s", fcnnme = getFunctionId(p));
	q= copyInstruction(p);
	OPTIMIZERDEBUG 
		mnstr_printf(cntxt->fdout,"=APPLY OPTIMIZER %s\n",fcnnme);
	if( p && p->argc > 1 ){
		if( getArgType(mb,p,1) != TYPE_str ||
			getArgType(mb,p,2) != TYPE_str ||
			!isVarConstant(mb,getArg(p,1)) ||
			!isVarConstant(mb,getArg(p,2))
			) {
			freeInstruction(q);
			throw(MAL, optimizer, ILLARG_CONSTANTS);
		}

		if( stk != 0){
			modnme= *getArgReference_str(stk,p,1);
			fcnnme= *getArgReference_str(stk,p,2);
		} else {
			modnme= getArgDefault(mb,p,1);
			fcnnme= getArgDefault(mb,p,2);
		}
		removeInstruction(mb, p);
		s= findSymbol(cntxt->nspace, putName(modnme,strlen(modnme)),putName(fcnnme,strlen(fcnnme)));

		if( s == NULL) {
			freeInstruction(q);
			throw(MAL, optimizer, RUNTIME_OBJECT_UNDEFINED ":%s.%s", modnme, fcnnme);
		}
		mb = s->def;
		stk= 0;
	} else if( p ) 
		removeInstruction(mb, p);
	if( mb->errors ){
		/* when we have errors, we still want to see them */
		addtoMalBlkHistory(mb,getModuleId(q));
		freeInstruction(q);
		return MAL_SUCCEED;
	}


	for ( i=0; codes[i].nme; i++)
		if ( strcmp(codes[i].nme, optimizer)== 0 ){
			actions = (int)(*(codes[i].fcn))(cntxt, mb, stk,0);
			break;	
		}
	if ( codes[i].nme == 0){
		freeInstruction(q);
		throw(MAL, optimizer, RUNTIME_OBJECT_UNDEFINED ":%s.%s", modnme, fcnnme);
	}

	msg= optimizerCheck(cntxt, mb, optimizer, actions, t=(GDKusec() - clk));
	OPTIMIZERDEBUG {
		mnstr_printf(cntxt->fdout,"=FINISHED %s  %d\n",optimizer, actions);
		printFunction(cntxt->fdout,mb,0,LIST_MAL_DEBUG );
	}
	DEBUGoptimizers
		mnstr_printf(cntxt->fdout,"#optimizer %-11s %3d actions %5d MAL instructions ("SZFMT" K) " LLFMT" usec\n", optimizer, actions, mb->stop, 
		((sizeof( MalBlkRecord) +mb->ssize * offsetof(InstrRecord, argv)+ mb->vtop * sizeof(int) /* argv estimate */ +mb->vtop* sizeof(VarRecord) + mb->vsize*sizeof(VarPtr)+1023)/1024),
		t);
	QOTupdateStatistics(getModuleId(q),actions,t);
	addtoMalBlkHistory(mb,getModuleId(q));
	freeInstruction(q);
	return msg;
}

