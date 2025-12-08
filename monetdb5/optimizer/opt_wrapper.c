/*
 * SPDX-License-Identifier: MPL-2.0
 *
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0.  If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 *
 * Copyright 2024, 2025 MonetDB Foundation;
 * Copyright August 2008 - 2023 MonetDB B.V.;
 * Copyright 1997 - July 2008 CWI.
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
#include "opt_projectionpath.h"
#include "opt_matpack.h"
#include "opt_postfix.h"
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
#include "opt_fastpath.h"
#include "optimizer_private.h"

// keep the optimizer list sorted
static const struct {
	char *nme;
	str (*fcn)(Client, MalBlkPtr, MalStkPtr, InstrPtr);
} codes[] = {
	/* most used compound optimizers at the top */
	{"defaultpipe", &OPTdefaultpipeImplementation},
	{"minimalpipe", &OPTminimalpipeImplementation},
	{"recursivepipe", &OPTdefaultpipeImplementation},
	{"nomitosispipe", &OPTdefaultpipeImplementation},
	{"sequentialpipe", &OPTdefaultpipeImplementation},

	/* all regular MAL optimizers */
	{"aliases", &OPTaliasesImplementation},
	{"candidates", &OPTcandidatesImplementation},
	{"coercions", &OPTcoercionImplementation},
	{"commonTerms", &OPTcommonTermsImplementation},
	{"constants", &OPTconstantsImplementation},
	{"costModel", &OPTcostModelImplementation},
	{"dataflow", &OPTdataflowImplementation},
	{"deadcode", &OPTdeadcodeImplementation},
	{"dict", &OPTdictImplementation},
	{"emptybind", &OPTemptybindImplementation},
	{"evaluate", &OPTevaluateImplementation},
	{"for", &OPTforImplementation},
	{"garbageCollector", &OPTgarbageCollectorImplementation},
	{"generator", &OPTgeneratorImplementation},
	{"inline", &OPTinlineImplementation},
	{"matpack", &OPTmatpackImplementation},
	{"mergetable", &OPTmergetableImplementation},
	{"mitosis", &OPTmitosisImplementation},
	{"multiplex", &OPTmultiplexImplementation},
	{"postfix", &OPTpostfixImplementation},
	{"profiler", &OPTprofilerImplementation},
	{"projectionpath", &OPTprojectionpathImplementation},
	{"pushselect", &OPTpushselectImplementation},
	{"querylog", &OPTquerylogImplementation},
	{"reduce", &OPTreduceImplementation},
	{"remap", &OPTremapImplementation},
	{"remoteQueries", &OPTremoteQueriesImplementation},
	{"reorder", &OPTreorderImplementation},
	{0, 0}
};

str
OPTwrapper(Client cntxt, MalBlkPtr mb, MalStkPtr stk, InstrPtr p)
{
	str modnme = "optimizer";
	const char *fcnnme;
	Symbol s = NULL;
	int i;
	str msg = MAL_SUCCEED;

	// no optimizer starts with a null byte, initialization sets a zero
	if (cntxt->mode == FINISHCLIENT)
		throw(MAL, "optimizer", SQLSTATE(42000) "prematurely stopped client");

	if (p == NULL)
		throw(MAL, "opt_wrapper",
			  SQLSTATE(HY002) "missing optimizer statement");

	if (mb->errors) {
		msg = mb->errors;
		mb->errors = NULL;
		return msg;
	}
	fcnnme = getFunctionId(p);

	if (p && p->argc > 1) {
		if (getArgType(mb, p, 1) != TYPE_str
			|| getArgType(mb, p, 2) != TYPE_str
			|| !isVarConstant(mb, getArg(p, 1))
			|| !isVarConstant(mb, getArg(p, 2)))
			throw(MAL, getFunctionId(p), SQLSTATE(42000) ILLARG_CONSTANTS);

		if (stk != NULL) {
			modnme = *getArgReference_str(stk, p, 1);
			fcnnme = *getArgReference_str(stk, p, 2);
		} else {
			modnme = getArgDefault(mb, p, 1);
			fcnnme = getArgDefault(mb, p, 2);
		}
		p->token = REMsymbol;
		s = findSymbol(cntxt->usermodule, putName(modnme), putName(fcnnme));

		if (s == NULL)
			throw(MAL, getFunctionId(p),
				  SQLSTATE(HY002) RUNTIME_OBJECT_UNDEFINED "%s.%s", modnme,
				  fcnnme);
		mb = s->def;
		stk = NULL;
	} else if (p) {
		p->token = REMsymbol;
	}

	const char *id = getFunctionId(p);
	for (i = 0; codes[i].nme != NULL; i++) {
		if (strcmp(codes[i].nme, id) == 0) {
			msg = (*codes[i].fcn) (cntxt, mb, stk, p);
			if (msg || mb->errors) {
				/* we can only return one or the other */
				if (msg == NULL)
					msg = mb->errors;
				mb->errors = NULL;
				str newmsg = createException(MAL, getFunctionId(p),
											 SQLSTATE(42000)
											 "Error in optimizer %s: %s",
											 getFunctionId(p), msg);
				return newmsg;
			}
			break;
		}
	}
	if (codes[i].nme == 0)
		throw(MAL, fcnnme,
			  SQLSTATE(HY002) "Optimizer implementation '%s' missing", fcnnme);

	return MAL_SUCCEED;
}
