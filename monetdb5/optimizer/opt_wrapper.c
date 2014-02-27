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
 * Copyright August 2008-2014 MonetDB B.V.
 * All Rights Reserved.
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
#include "opt_accumulators.h"
#include "opt_aliases.h"
#include "opt_centipede.h"
#include "opt_cluster.h"
#include "opt_coercion.h"
#include "opt_commonTerms.h"
#include "opt_constants.h"
#include "opt_costModel.h"
#include "opt_dataflow.h"
#include "opt_deadcode.h"
#include "opt_emptySet.h"
#include "opt_evaluate.h"
#include "opt_factorize.h"
#include "opt_garbageCollector.h"
#include "opt_groups.h"
#include "opt_inline.h"
#include "opt_joinpath.h"
#include "opt_mapreduce.h"
#include "opt_matpack.h"
#include "opt_json.h"
#include "opt_mergetable.h"
#include "opt_mitosis.h"
#include "opt_multiplex.h"
#include "opt_octopus.h"
#include "opt_prejoin.h"
#include "opt_pushranges.h"
#include "opt_pushselect.h"
#include "opt_qep.h"
#include "opt_querylog.h"
#include "opt_recycler.h"
#include "opt_reduce.h"
#include "opt_remap.h"
#include "opt_remoteQueries.h"
#include "opt_reorder.h"
#include "opt_statistics.h"
#include "opt_strengthReduction.h"

struct{
	str nme;
	int (*fcn)();
} codes[] = {
	{"accumulators", &OPTaccumulatorsImplementation},
	{"aliases", &OPTaliasesImplementation},
	{"centipede", &OPTcentipedeImplementation},
	{"cluster", &OPTclusterImplementation},
	{"coercions", &OPTcoercionImplementation},
	{"commonTerms", &OPTcommonTermsImplementation},
	{"constants", &OPTconstantsImplementation},
	{"costModel", &OPTcostModelImplementation},
	{"dataflow", &OPTdataflowImplementation},
	{"deadcode", &OPTdeadcodeImplementation},
	{"dumpQEP", &OPTdumpQEPImplementation},
	{"emptySet", &OPTemptySetImplementation},
	{"evaluate", &OPTevaluateImplementation},
	{"factorize", &OPTfactorizeImplementation},
	{"garbageCollector", &OPTgarbageCollectorImplementation},
	{"groups", &OPTgroupsImplementation},
	{"inline", &OPTinlineImplementation},
	{"joinPath", &OPTjoinPathImplementation},
	{"mapreduce", &OPTmapreduceImplementation},
	{"matpack", &OPTmatpackImplementation},
	{"json", &OPTjsonImplementation},
	{"mergetable", &OPTmergetableImplementation},
	{"mitosis", &OPTmitosisImplementation},
	{"multiplex", &OPTmultiplexImplementation},
	{"octopus", &OPToctopusImplementation},
	{"prejoin", &OPTprejoinImplementation},
	{"pushranges", &OPTpushrangesImplementation},
	{"pushselect", &OPTpushselectImplementation},
	{"querylog", &OPTquerylogImplementation},
	{"recycler", &OPTrecyclerImplementation},
	{"reduce", &OPTreduceImplementation},
	{"remap", &OPTremapImplementation},
	{"remoteQueries", &OPTremoteQueriesImplementation},
	{"reorder", &OPTreorderImplementation},
	{"strengthReduction", &OPTstrengthReductionImplementation},
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
	InstrPtr q= copyInstruction(p);

	optimizerInit();
	snprintf(optimizer,256,"%s", fcnnme = getFunctionId(p));
	OPTIMIZERDEBUG 
		mnstr_printf(cntxt->fdout,"=APPLY OPTIMIZER %s\n",fcnnme);
	if( p && p->argc > 1 ){
		if( getArgType(mb,p,1) != TYPE_str ||
			getArgType(mb,p,2) != TYPE_str ||
			!isVarConstant(mb,getArg(p,1)) ||
			!isVarConstant(mb,getArg(p,2))
		) 
			throw(MAL, optimizer, ILLARG_CONSTANTS);

		if( stk != 0){
			modnme= *(str*)getArgReference(stk,p,1);
			fcnnme= *(str*)getArgReference(stk,p,2);
		} else {
			modnme= getArgDefault(mb,p,1);
			fcnnme= getArgDefault(mb,p,2);
		}
		removeInstruction(mb, p);
		s= findSymbol(cntxt->nspace, putName(modnme,strlen(modnme)),putName(fcnnme,strlen(fcnnme)));

		if( s == NULL) {
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
		throw(MAL, optimizer, RUNTIME_OBJECT_UNDEFINED ":%s.%s", modnme, fcnnme);
	}

	msg= optimizerCheck(cntxt, mb, optimizer, actions, t=(GDKusec() - clk),OPT_CHECK_ALL);
	OPTIMIZERDEBUG {
		mnstr_printf(cntxt->fdout,"=FINISHED %s  %d\n",optimizer, actions);
		printFunction(cntxt->fdout,mb,0,LIST_MAL_STMT | LIST_MAPI);
	}
	DEBUGoptimizers
		mnstr_printf(cntxt->fdout,"#optimizer %-11s %3d actions %5d MAL instructions ("SZFMT" K) " LLFMT" usec\n", optimizer, actions, mb->stop, 
		((sizeof( MalBlkRecord) +mb->ssize * sizeof(InstrRecord)+ mb->vtop * sizeof(int) /* argv estimate */ +mb->vtop* sizeof(VarRecord) + mb->vsize*sizeof(VarPtr)+1023)/1024),
		t);
	QOTupdateStatistics(getModuleId(q),actions,t);
	addtoMalBlkHistory(mb,getModuleId(q));
	freeInstruction(q);
	return msg;
}

