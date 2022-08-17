/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0.  If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 *
 * Copyright 1997 - July 2008 CWI, August 2008 - 2021 MonetDB B.V.
 */

#include "monetdb_config.h"
#include "opt_fastpath.h"
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
#include "opt_oltp.h"
#include "opt_postfix.h"
#include "opt_mask.h"
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
#include "opt_wlc.h"
#include "optimizer_private.h"
#include "mal_interpreter.h"

str
OPTminimalfastImplementation(Client cntxt, MalBlkPtr mb, MalStkPtr stk, InstrPtr p)
{
	int actions = 0;
	char buf[256];
	lng usec = GDKusec();
	str msg = MAL_SUCCEED;

	/* perform a single scan through the plan to determine which optimizer steps to skip */
	for( int i=0; i<mb->stop; i++){
		InstrPtr q = getInstrPtr(mb,i);
		if (q->modname == sqlRef && q->fcnname == importTableRef)
			bincopy= 1;
		if( getModuleId(q) == generatorRef)
			generator = 1;
		if ( getFunctionId(q) == multiplexRef)
			multiplex = 1;
	}

	optcall(true, OPTinlineImplementation);
	optcall(true, OPTremapImplementation);
	optcall(bincopy, OPTbincopyfromImplementation);
	optcall(true, OPTemptybindImplementation);
	optcall(true, OPTdeadcodeImplementation);
	optcall(true, OPTforImplementation);
	optcall(true, OPTdictImplementation);
	optcall(multiplex, OPTmultiplexImplementation);
	optcall(generator, OPTgeneratorImplementation);
	optcall(profilerStatus, OPTprofilerImplementation);
	optcall(profilerStatus, OPTcandidatesImplementation);
	optcall(true, OPTgarbageCollectorImplementation);

	/* Defense line against incorrect plans  handled by optimizer steps */
	/* keep all actions taken as a post block comment */
	usec = GDKusec()- usec;
	snprintf(buf,256,"%-20s actions=1 time=" LLFMT " usec","minimalfast", usec);
	newComment(mb,buf);
	if( actions > 0)
		addtoMalBlkHistory(mb);

	return msg;
}

str
OPTdefaultfastImplementation(Client cntxt, MalBlkPtr mb, MalStkPtr stk, InstrPtr p)
{
	int actions = 0;
	char buf[256];
	lng usec = GDKusec();
	str msg = MAL_SUCCEED;

	/* perform a single scan through the plan to determine which optimizer steps to skip */
	for( int i=0; i<mb->stop; i++){
		InstrPtr q = getInstrPtr(mb,i);
		if (q->modname == sqlRef && q->fcnname == importTableRef)
			bincopy= 1;
		if( getModuleId(q) == generatorRef)
			generator = 1;
		if ( getFunctionId(q) == multiplexRef)
			multiplex = 1;
	}

	optcall(true, OPTinlineImplementation);
	optcall(true, OPTremapImplementation);
	optcall(true, OPTcostModelImplementation);
	optcall(true, OPTcoercionImplementation);
	optcall(true, OPTaliasesImplementation);
	optcall(true, OPTevaluateImplementation);
	optcall(true, OPTemptybindImplementation);
	optcall(true, OPTdeadcodeImplementation);
	optcall(true, OPTpushselectImplementation);
	optcall(true, OPTaliasesImplementation);
	optcall(true, OPTforImplementation);
	optcall(true, OPTdictImplementation);
	optcall(true, OPTmitosisImplementation);
	optcall(true, OPTmergetableImplementation);
	optcall(bincopy, OPTbincopyfromImplementation);
	optcall(true, OPTaliasesImplementation);
	optcall(true, OPTconstantsImplementation);
	optcall(true, OPTcommonTermsImplementation);
	optcall(true, OPTprojectionpathImplementation);
	optcall(true, OPTdeadcodeImplementation);
	optcall(true, OPTreorderImplementation);
	optcall(true, OPTmatpackImplementation);
	optcall(true, OPTdataflowImplementation);
	optcall(true, OPTquerylogImplementation);
	optcall(multiplex, OPTmultiplexImplementation);
	optcall(generator, OPTgeneratorImplementation);
	optcall(profilerStatus, OPTprofilerImplementation);
	optcall(profilerStatus, OPTcandidatesImplementation);
	optcall(true, OPTdeadcodeImplementation);
	optcall(true, OPTpostfixImplementation);
	// optcall(true, OPTjitImplementation);
	optcall(true, OPTwlcImplementation);
	optcall(true, OPTgarbageCollectorImplementation);

	/* Defense line against incorrect plans  handled by optimizer steps */
	/* keep all actions taken as a post block comment */
	usec = GDKusec()- usec;
	snprintf(buf,256,"%-20s actions=1 time=" LLFMT " usec","defaultfast", usec);
	newComment(mb,buf);
	if( actions > 0)
		addtoMalBlkHistory(mb);

	return msg;
}
