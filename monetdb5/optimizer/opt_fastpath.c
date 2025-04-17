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

#include "monetdb_config.h"
#include "opt_fastpath.h"
#include "opt_aliases.h"
#include "opt_coercion.h"
#include "opt_commonTerms.h"
#include "opt_candidates.h"
#include "opt_constants.h"
#include "opt_costModel.h"
#include "opt_dataflow.h"
#include "opt_deadcode.h"
#include "opt_dict.h"
#include "opt_for.h"
#include "opt_emptybind.h"
#include "opt_evaluate.h"
#include "opt_garbageCollector.h"
#include "opt_generator.h"
#include "opt_inline.h"
#include "opt_projectionpath.h"
#include "opt_matpack.h"
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
#include "opt_fastpath.h"
#include "optimizer_private.h"
#include "mal_interpreter.h"

#define optcall(OPT)													\
	do {																\
		if ((msg = OPT(cntxt, mb, stk, pci)) != MAL_SUCCEED)			\
			goto bailout;												\
		actions += *(int*)getVarValue(mb, getArg(pci, pci->argc - 1));	\
		delArgument(pci, pci->argc - 1); /* keep number of argc low, so 'pci' is not reallocated */ \
	} while (0)

str
OPTminimalfastImplementation(Client cntxt, MalBlkPtr mb, MalStkPtr stk,
							 InstrPtr pci)
{
	str msg = MAL_SUCCEED;
	bool generator = false, multiplex = true;
	int actions = 0;

	/* perform a single scan through the plan to determine which optimizer steps to skip */
	for (int i = 0; i < mb->stop; i++) {
		InstrPtr q = getInstrPtr(mb, i);
		if (getModuleId(q) == generatorRef) {
			generator = true;
			if (multiplex)
				break;
		}
		if (getFunctionId(q) == multiplexRef) {
			multiplex = true;
			if (generator)
				break;
		}
	}

	optcall(OPTinlineImplementation);
	optcall(OPTremapImplementation);
	optcall(OPTemptybindImplementation);
	optcall(OPTdeadcodeImplementation);
	optcall(OPTforImplementation);
	optcall(OPTdictImplementation);
	if (multiplex)
		optcall(OPTmultiplexImplementation);
	if (generator)
		optcall(OPTgeneratorImplementation);
	if (profilerStatus)
		optcall(OPTprofilerImplementation);
	optcall(OPTgarbageCollectorImplementation);

	/* Defense line against incorrect plans  handled by optimizer steps */
	/* keep actions taken as a fake argument */
  bailout:
	(void) pushInt(mb, pci, actions);
	return msg;
}

str
OPTdefaultfastImplementation(Client cntxt, MalBlkPtr mb, MalStkPtr stk,
							 InstrPtr pci)
{
	str msg = MAL_SUCCEED;
	bool generator = false, multiplex = false;
	int actions = 0;

	/* perform a single scan through the plan to determine which optimizer steps to skip */
	for (int i = 0; i < mb->stop; i++) {
		InstrPtr q = getInstrPtr(mb, i);
		if (getModuleId(q) == generatorRef) {
			generator = true;
			if (multiplex)
				break;
		}
		if (getFunctionId(q) == multiplexRef) {
			multiplex = true;
			if (generator)
				break;
		}
	}

	optcall(OPTinlineImplementation);
	optcall(OPTremapImplementation);
	optcall(OPTcostModelImplementation);
	optcall(OPTcoercionImplementation);
	optcall(OPTaliasesImplementation);
	optcall(OPTevaluateImplementation);
	optcall(OPTemptybindImplementation);
	optcall(OPTdeadcodeImplementation);
	optcall(OPTpushselectImplementation);
	optcall(OPTaliasesImplementation);
	optcall(OPTforImplementation);
	optcall(OPTdictImplementation);
	if (!cntxt->no_mitosis) {
		optcall(OPTmitosisImplementation);
		optcall(OPTmergetableImplementation); /* depends on mitosis */
	}
	optcall(OPTaliasesImplementation);
	optcall(OPTconstantsImplementation);
	optcall(OPTcommonTermsImplementation);
	optcall(OPTprojectionpathImplementation);
	optcall(OPTdeadcodeImplementation);
	if (!cntxt->no_mitosis) {
		optcall(OPTmatpackImplementation); /* depends on mergetable */
		optcall(OPTreorderImplementation); /* depends on mitosis */
	}
	optcall(OPTdataflowImplementation);
	optcall(OPTquerylogImplementation);
	if (multiplex)
		optcall(OPTmultiplexImplementation);
	if (generator)
		optcall(OPTgeneratorImplementation);
	if (profilerStatus)
		optcall(OPTcandidatesImplementation);
	optcall(OPTdeadcodeImplementation);
	optcall(OPTpostfixImplementation);
	if (profilerStatus)
		optcall(OPTprofilerImplementation);
	optcall(OPTgarbageCollectorImplementation);

	/* Defense line against incorrect plans  handled by optimizer steps */
	/* keep actions taken as a fake argument */
  bailout:
	(void) pushInt(mb, pci, actions);
	return msg;
}
