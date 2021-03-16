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

	(void)cntxt;
	(void)stk;
	(void) p;


	msg = OPTinlineImplementation(cntxt, mb, stk, p);
	if( msg == MAL_SUCCEED) msg = OPTremapImplementation(cntxt, mb, stk, p);
	if( msg == MAL_SUCCEED) msg = OPTbincopyfromImplementation(cntxt, mb, stk, p);
	if( msg == MAL_SUCCEED) msg = OPTdeadcodeImplementation(cntxt, mb, stk, p);
	if( msg == MAL_SUCCEED) msg = OPTmultiplexImplementation(cntxt, mb, stk, p);
	if( msg == MAL_SUCCEED) msg = OPTgeneratorImplementation(cntxt, mb, stk, p);
	if( msg == MAL_SUCCEED) msg = OPTprofilerImplementation(cntxt, mb, stk, p);
	if( msg == MAL_SUCCEED) msg = OPTcandidatesImplementation(cntxt, mb, stk, p);
	if( msg == MAL_SUCCEED) msg = OPTgarbageCollectorImplementation(cntxt, mb, stk, p);

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

	(void)cntxt;
	(void)stk;
	(void) p;


	msg = OPTinlineImplementation(cntxt, mb, stk, p);
	if( msg == MAL_SUCCEED) msg = OPTremapImplementation(cntxt, mb, stk, p);
	if( msg == MAL_SUCCEED) msg = OPTcostModelImplementation(cntxt, mb, stk, p);
	if( msg == MAL_SUCCEED) msg = OPTcoercionImplementation(cntxt, mb, stk, p);
	if( msg == MAL_SUCCEED) msg = OPTaliasesImplementation(cntxt, mb, stk, p);
	if( msg == MAL_SUCCEED) msg = OPTevaluateImplementation(cntxt, mb, stk, p);
	if( msg == MAL_SUCCEED) msg = OPTemptybindImplementation(cntxt, mb, stk, p);
	if( msg == MAL_SUCCEED) msg = OPTdeadcodeImplementation(cntxt, mb, stk, p);
	if( msg == MAL_SUCCEED) msg = OPTpushselectImplementation(cntxt, mb, stk, p);
	if( msg == MAL_SUCCEED) msg = OPTaliasesImplementation(cntxt, mb, stk, p);
	if( msg == MAL_SUCCEED) msg = OPTmitosisImplementation(cntxt, mb, stk, p);
	if( msg == MAL_SUCCEED) msg = OPTmergetableImplementation(cntxt, mb, stk, p);
	if( msg == MAL_SUCCEED) msg = OPTbincopyfromImplementation(cntxt, mb, stk, p);
	if( msg == MAL_SUCCEED) msg = OPTaliasesImplementation(cntxt, mb, stk, p);
	if( msg == MAL_SUCCEED) msg = OPTconstantsImplementation(cntxt, mb, stk, p);
	if( msg == MAL_SUCCEED) msg = OPTcommonTermsImplementation(cntxt, mb, stk, p);
	if( msg == MAL_SUCCEED) msg = OPTprojectionpathImplementation(cntxt, mb, stk, p);
	if( msg == MAL_SUCCEED) msg = OPTdeadcodeImplementation(cntxt, mb, stk, p);
	if( msg == MAL_SUCCEED) msg = OPTreorderImplementation(cntxt, mb, stk, p);
	if( msg == MAL_SUCCEED) msg = OPTmatpackImplementation(cntxt, mb, stk, p);
	if( msg == MAL_SUCCEED) msg = OPTdataflowImplementation(cntxt, mb, stk, p);
	if( msg == MAL_SUCCEED) msg = OPTquerylogImplementation(cntxt, mb, stk, p);
	if( msg == MAL_SUCCEED) msg = OPTmultiplexImplementation(cntxt, mb, stk, p);
	if( msg == MAL_SUCCEED) msg = OPTgeneratorImplementation(cntxt, mb, stk, p);
	if( msg == MAL_SUCCEED) msg = OPTprofilerImplementation(cntxt, mb, stk, p);
	if( msg == MAL_SUCCEED) msg = OPTcandidatesImplementation(cntxt, mb, stk, p);
	if( msg == MAL_SUCCEED) msg = OPTdeadcodeImplementation(cntxt, mb, stk, p);
	if( msg == MAL_SUCCEED) msg = OPTpostfixImplementation(cntxt, mb, stk, p);
	// if( msg == MAL_SUCCEED) msg = OPTjitImplementation(cntxt, mb, stk, p);
	if( msg == MAL_SUCCEED) msg = OPTwlcImplementation(cntxt, mb, stk, p);
	if( msg == MAL_SUCCEED) msg = OPTgarbageCollectorImplementation(cntxt, mb, stk, p);

	/* Defense line against incorrect plans  handled by optimizer steps */
	/* keep all actions taken as a post block comment */
	usec = GDKusec()- usec;
	snprintf(buf,256,"%-20s actions=1 time=" LLFMT " usec","defaultfast", usec);
	newComment(mb,buf);
	if( actions > 0)
		addtoMalBlkHistory(mb);

	return msg;
}
