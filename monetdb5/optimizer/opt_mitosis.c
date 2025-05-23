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
#include "opt_mitosis.h"
#include "mal_interpreter.h"
#include "gdk_utils.h"

#define MIN_PART_SIZE 100000	/* minimal record count per partition */
#define MAX_PARTS2THREADS_RATIO 4	/* There should be at most this multiple more of partitions then threads */


str
OPTmitosisImplementation(Client cntxt, MalBlkPtr mb, MalStkPtr stk,
						 InstrPtr pci)
{
	int i, j, limit, slimit, estimate = 0, pieces = 1, mito_parts = 0,
		mito_size = 0, row_size = 0, mt = -1, nr_cols = 0, nr_aggrs = 0,
		nr_maps = 0;
	str schema = 0, table = 0;
	BUN r = 0, rowcnt = 0;		/* table should be sizeable to consider parallel execution */
	InstrPtr p, q, *old, target = 0;
	size_t argsize = 6 * sizeof(lng), m = 0;
	/*       estimate size per operator estimate:   4 args + 2 res */
	int threads = GDKnr_threads ? GDKnr_threads : 1, maxparts = MAXSLICES;
	str msg = MAL_SUCCEED;

	/* if the user has associated limitation on the number of threads, respect it in the
	 * generation of the number of partitions. Beware, they may lead to larger pieces, it only
	 * limits the CPU power */
	if (cntxt->workerlimit)
		threads = cntxt->workerlimit;
	(void) cntxt;
	(void) stk;

	old = mb->stmt;
	for (i = 1; i < mb->stop; i++) {
		InstrPtr p = old[i];

		if (getModuleId(p) == sqlRef && getFunctionId(p) == assertRef
			&& p->argc > 2 && getArgType(mb, p, 2) == TYPE_str
			&& isVarConstant(mb, getArg(p, 2))
			&& getVarConstant(mb, getArg(p, 2)).val.sval != NULL
			&&
			(strstr(getVarConstant(mb, getArg(p, 2)).val.sval,
					"PRIMARY KEY constraint")
			 || strstr(getVarConstant(mb, getArg(p, 2)).val.sval,
					   "UNIQUE constraint"))) {
			pieces = 0;
			goto bailout;
		}

		/* mitosis/mergetable bailout conditions */
		/* Crude protection against self join explosion */
		if (p->retc == 2 && isMatJoinOp(p))
			maxparts = threads;

		nr_aggrs += (p->argc > 2 && getModuleId(p) == aggrRef);
		nr_maps += (isMapOp(p));

		if ((getModuleId(p) == algebraRef &&
		    getFunctionId(p) == groupedfirstnRef) ||
		    (p->argc > 2 && getModuleId(p) == aggrRef
			&& getFunctionId(p) != subcountRef && getFunctionId(p) != subminRef
			&& getFunctionId(p) != submaxRef && getFunctionId(p) != subavgRef
			&& getFunctionId(p) != subsumRef && getFunctionId(p) != subprodRef
			&& getFunctionId(p) != countRef && getFunctionId(p) != minRef
			&& getFunctionId(p) != maxRef && getFunctionId(p) != avgRef
			&& getFunctionId(p) != sumRef && getFunctionId(p) != prodRef)) {
			pieces = 0;
			goto bailout;
		}

		/* rtree functions should not be optimized by mitosis
		 * (single-threaded execution) */
		if (getModuleId(p) == rtreeRef) {
			pieces = 0;
			goto bailout;
		}

		/* do not split up floating point bat that is being summed */
		if (p->retc == 1 &&
			(((p->argc == 5 || p->argc == 6)
			  && getModuleId(p) == aggrRef
			  && getFunctionId(p) == subsumRef)
			 || (p->argc == 4
				 && getModuleId(p) == aggrRef
				 && getFunctionId(p) == sumRef))
			&& isaBatType(getArgType(mb, p, p->retc))
			&& (getBatType(getArgType(mb, p, p->retc)) == TYPE_flt
				|| getBatType(getArgType(mb, p, p->retc)) == TYPE_dbl)) {
			pieces = 0;
			goto bailout;
		}

		if (p->argc > 2
			&& (getModuleId(p) == capiRef || getModuleId(p) == rapiRef
				|| getModuleId(p) == pyapi3Ref)
			&& getFunctionId(p) == subeval_aggrRef) {
			pieces = 0;
			goto bailout;
		}

		/* Mergetable cannot handle intersect/except's for now */
		if (getModuleId(p) == algebraRef && getFunctionId(p) == groupbyRef) {
			pieces = 0;
			goto bailout;
		}

		/* locate the largest non-partitioned table */
		if (getModuleId(p) != sqlRef
			|| (getFunctionId(p) != bindRef && getFunctionId(p) != bindidxRef
				&& getFunctionId(p) != tidRef))
			continue;
		/* don't split insert BATs */
		if (p->argc > 5 && getVarConstant(mb, getArg(p, 5)).val.ival == 1)
			continue;
		if (p->argc > 6)
			continue;			/* already partitioned */
		/*
		 * The SQL optimizer already collects the counts of the base
		 * table and passes them on as a row property.  All pieces for a
		 * single subplan should ideally fit together.
		 */
		r = getRowCnt(mb, getArg(p, 0));
		if (r == rowcnt)
			nr_cols++;
		if (r > rowcnt) {
			/* the rowsize depends on the column types, assume void-headed */
			row_size = ATOMsize(getBatType(getArgType(mb, p, 0)));
			rowcnt = r;
			nr_cols = 1;
			target = p;
			estimate++;
			r = 0;
		}
	}
	if (target == 0) {
		pieces = 0;
		goto bailout;
	}
	/*
	 * The number of pieces should be based on the footprint of the
	 * query plan, such that preferably it can be handled without
	 * swapping intermediates.  For the time being we just go for pieces
	 * that fit into memory in isolation.  A fictive rowcount is derived
	 * based on argument types, such that all pieces would fit into
	 * memory conveniently for processing. We attempt to use not more
	 * threads than strictly needed.
	 * Experience shows that the pieces should not be too small.
	 * If we should limit to |threads| is still an open issue.
	 *
	 * Take into account the number of client connections,
	 * because all user together are responsible for resource contentions
	 */

	/* improve memory usage estimation */
	if (nr_cols > 1 || nr_aggrs > 1 || nr_maps > 1)
		argsize = (nr_cols + nr_aggrs + nr_maps) * sizeof(lng);
	/* We haven't assigned the number of pieces.
	 * Determine the memory available for this client
	 */

	/* respect the memory limit size set for the user
	 * and determine the column part size
	 */
	m = GDK_mem_maxsize / MCactiveClients();	/* use temporarily */
	if (cntxt->memorylimit > 0 && (size_t) cntxt->memorylimit << 20 < m)
		m = ((size_t) cntxt->memorylimit << 20) / argsize;
	else if (cntxt->maxmem > 0 && cntxt->maxmem < (lng) m)
		m = (size_t) (cntxt->maxmem / argsize);
	else
		m = m / argsize;

	/* if data exceeds memory size,
	 * i.e., (rowcnt*argsize > GDK_mem_maxsize),
	 * i.e., (rowcnt > GDK_mem_maxsize/argsize = m) */
	if (rowcnt > m && m / threads > 0) {
		/* create |pieces| > |threads| partitions such that
		 * |threads| partitions at a time fit in memory,
		 * i.e., (threads*(rowcnt/pieces) <= m),
		 * i.e., (rowcnt/pieces <= m/threads),
		 * i.e., (pieces => rowcnt/(m/threads))
		 * (assuming that (m > threads*MIN_PART_SIZE)) */
		/* the number of pieces affects SF-100, going beyond 8x increases
		 * the optimizer costs beyond the execution time
		 */
		pieces = ((int) ceil((double) rowcnt / (m / threads)));
		if (pieces <= threads)
			pieces = threads;
	} else if (rowcnt > MIN_PART_SIZE) {
		/* exploit parallelism, but ensure minimal partition size to
		 * limit overhead */
		pieces = MIN((int) ceil((double) rowcnt / MIN_PART_SIZE),
					 MAX_PARTS2THREADS_RATIO * threads);
	}

	/* when testing, always aim for full parallelism, but avoid
	 * empty pieces */
	FORCEMITODEBUG if (pieces < threads)
		 pieces = (int) MIN((BUN) threads, rowcnt);
	/* prevent plan explosion */
	if (pieces > maxparts)
		pieces = maxparts;
	/* to enable experimentation we introduce the option to set
	 * the number of partitions required and/or the size of each chunk (in K)
	 */
	mito_parts = GDKgetenv_int("mito_parts", 0);
	if (mito_parts > 0)
		pieces = mito_parts;
	mito_size = GDKgetenv_int("mito_size", 0);
	if (mito_size > 0)
		pieces = (int) ((rowcnt * row_size) / (mito_size * 1024));

	if (pieces <= 1) {
		pieces = 0;
		goto bailout;
	}

	/* at this stage we have identified the #chunks to be used for the largest table */
	limit = mb->stop;
	slimit = mb->ssize;
	if (newMalBlkStmt(mb, mb->stop + 2 * estimate) < 0)
		throw(MAL, "optimizer.mitosis", SQLSTATE(HY013) MAL_MALLOC_FAIL);
	estimate = 0;

	schema = getVarConstant(mb, getArg(target, 2)).val.sval;
	table = getVarConstant(mb, getArg(target, 3)).val.sval;
	for (i = 0; mb->errors == NULL && i < limit; i++) {
		int upd = 0, qtpe, rtpe = 0, qv, rv;
		InstrPtr matq, matr = NULL;
		p = old[i];

		if (getModuleId(p) != sqlRef
			|| !(getFunctionId(p) == bindRef || getFunctionId(p) == bindidxRef
				 || getFunctionId(p) == tidRef)) {
			pushInstruction(mb, p);
			continue;
		}
		/* don't split insert BATs */
		if (p->argc == 6 && getVarConstant(mb, getArg(p, 5)).val.ival == 1) {
			pushInstruction(mb, p);
			continue;
		}
		r = getRowCnt(mb, getArg(p, 0));
		if (r < rowcnt) {
			pushInstruction(mb, p);
			continue;
		}
		/* Don't split the (index) bat if we already have identified a range */
		/* This will happen if we inline separately optimized routines */
		if (p->argc > 7) {
			pushInstruction(mb, p);
			continue;
		}
		if (p->retc == 2)
			upd = 1;
		if (mt < 0
			&& (strcmp(schema, getVarConstant(mb, getArg(p, 2 + upd)).val.sval)
				|| strcmp(table,
						  getVarConstant(mb, getArg(p, 3 + upd)).val.sval))) {
			pushInstruction(mb, p);
			continue;
		}
		/* we keep the original bind operation, because it allows for
		 * easy undo when the mergtable can not do something */
		// pushInstruction(mb, p);

		qtpe = getVarType(mb, getArg(p, 0));

		matq = newInstructionArgs(NULL, matRef, newRef, pieces + 1);
		if (matq == NULL) {
			msg = createException(MAL, "optimizer.mitosis",
								  SQLSTATE(HY013) MAL_MALLOC_FAIL);
			break;
		}
		getArg(matq, 0) = getArg(p, 0);

		if (upd) {
			matr = newInstructionArgs(NULL, matRef, newRef, pieces + 1);
			if (matr == NULL) {
				freeInstruction(matq);
				msg = createException(MAL, "optimizer.mitosis",
									  SQLSTATE(HY013) MAL_MALLOC_FAIL);
				break;
			}
			getArg(matr, 0) = getArg(p, 1);
			rtpe = getVarType(mb, getArg(p, 1));
		}

		for (j = 0; j < pieces; j++) {
			q = copyInstruction(p);
			if (q == NULL) {
				freeInstruction(matr);
				freeInstruction(matq);
				for (; i < limit; i++)
					if (old[i])
						pushInstruction(mb, old[i]);
				GDKfree(old);
				throw(MAL, "optimizer.mitosis",
					  SQLSTATE(HY013) MAL_MALLOC_FAIL);
			}
			q = pushInt(mb, q, j);
			q = pushInt(mb, q, pieces);

			qv = getArg(q, 0) = newTmpVariable(mb, qtpe);
			if (upd) {
				rv = getArg(q, 1) = newTmpVariable(mb, rtpe);
			}
			pushInstruction(mb, q);
			matq = pushArgument(mb, matq, qv);
			if (upd)
				matr = pushArgument(mb, matr, rv);
		}
		pushInstruction(mb, matq);
		if (upd)
			pushInstruction(mb, matr);
		freeInstruction(p);
	}
	for (; i < slimit; i++)
		if (old[i])
			pushInstruction(mb, old[i]);
	GDKfree(old);

	/* Defense line against incorrect plans */
	if (msg == MAL_SUCCEED) {
		msg = chkTypes(cntxt->usermodule, mb, FALSE);
		if (msg == MAL_SUCCEED) {
			msg = chkFlow(mb);
			if (msg == MAL_SUCCEED)
				msg = chkDeclarations(mb);
		}
	}
  bailout:
	/* keep actions taken as a fake argument */
	(void) pushInt(mb, pci, pieces);
	return msg;
}
