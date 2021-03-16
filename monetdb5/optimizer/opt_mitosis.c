/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0.  If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 *
 * Copyright 1997 - July 2008 CWI, August 2008 - 2021 MonetDB B.V.
 */

#include "monetdb_config.h"
#include "opt_mitosis.h"
#include "mal_interpreter.h"
#include "gdk_utils.h"


str
OPTmitosisImplementation(Client cntxt, MalBlkPtr mb, MalStkPtr stk, InstrPtr p)
{
	int i, j, limit, slimit, estimate = 0, pieces = 1, mito_parts = 0, mito_size = 0, row_size = 0, mt = -1;
	str schema = 0, table = 0;
	BUN r = 0, rowcnt = 0;    /* table should be sizeable to consider parallel execution*/
	InstrPtr q, *old, target = 0;
	size_t argsize = 6 * sizeof(lng), m = 0;
	/*     per op estimate:   4 args + 2 res*/
	int threads = GDKnr_threads ? GDKnr_threads : 1;
	int activeClients;
	char buf[256];
	lng usec = GDKusec();
	str msg = MAL_SUCCEED;

	//if ( optimizerIsApplied(mb,"mitosis") )
		//return 0;
	(void) cntxt;
	(void) stk;

	old = mb->stmt;
	for (i = 1; i < mb->stop; i++) {
		InstrPtr p = old[i];

		if (getModuleId(p) == sqlRef && getFunctionId(p) == assertRef &&
			p->argc > 2 && getArgType(mb, p, 2) == TYPE_str &&
			isVarConstant(mb, getArg(p, 2)) &&
			getVarConstant(mb, getArg(p, 2)).val.sval != NULL &&
			(strstr(getVarConstant(mb, getArg(p, 2)).val.sval, "PRIMARY KEY constraint") ||
			 strstr(getVarConstant(mb, getArg(p, 2)).val.sval, "UNIQUE constraint"))){
			pieces = 0;
			goto bailout;
		}

		/* mitosis/mergetable bailout conditions */

		if (p->argc > 2 && getModuleId(p) == aggrRef &&
		        getFunctionId(p) != subcountRef &&
		    	getFunctionId(p) != subminRef &&
		    	getFunctionId(p) != submaxRef &&
		    	getFunctionId(p) != subavgRef &&
		    	getFunctionId(p) != subsumRef &&
		    	getFunctionId(p) != subprodRef &&

		        getFunctionId(p) != countRef &&
		    	getFunctionId(p) != minRef &&
		    	getFunctionId(p) != maxRef &&
		    	getFunctionId(p) != avgRef &&
		    	getFunctionId(p) != sumRef &&
		    	getFunctionId(p) != prodRef){
				pieces = 0;
				goto bailout;
			}

		/* do not split up floating point bat that is being summed */
		if (p->retc == 1 &&
			(((p->argc == 6 || p->argc == 7) &&
			  getModuleId(p) == aggrRef &&
			  getFunctionId(p) == subsumRef) ||
			 (p->argc == 4 &&
			  getModuleId(p) == aggrRef &&
			  getFunctionId(p) == sumRef)) &&
			isaBatType(getArgType(mb, p, p->retc)) &&
			(getBatType(getArgType(mb, p, p->retc)) == TYPE_flt ||
			 getBatType(getArgType(mb, p, p->retc)) == TYPE_dbl)){
				pieces = 0;
				goto bailout;
			}

		if (p->argc > 2 && (getModuleId(p) == capiRef || getModuleId(p) == rapiRef || getModuleId(p) == pyapi3Ref) &&
		        getFunctionId(p) == subeval_aggrRef){
				pieces = 0;
				goto bailout;
			}

		/* Mergetable cannot handle intersect/except's for now */
		if (getModuleId(p) == algebraRef && getFunctionId(p) == groupbyRef){
			pieces = 0;
			goto bailout;
		}

		/* locate the largest non-partitioned table */
		if (getModuleId(p) != sqlRef || (getFunctionId(p) != bindRef && getFunctionId(p) != bindidxRef))
			continue;
		/* don't split insert BATs */
		if (getVarConstant(mb, getArg(p, 5)).val.ival == 1)
			continue;
		if (p->argc > 6)
			continue;  /* already partitioned */
		/*
		 * The SQL optimizer already collects the counts of the base
		 * table and passes them on as a row property.  All pieces for a
		 * single subplan should ideally fit together.
		 */
		r = getRowCnt(mb, getArg(p, 0));
		if (r > rowcnt) {
			/* the rowsize depends on the column types, assume void-headed */
			row_size = ATOMsize(getBatType(getArgType(mb,p,0)));
			rowcnt = r;
			target = p;
			estimate++;
			r = 0;
		}
	}
	if (target == 0){
		pieces = 0;
		goto bailout;
	}
	/*
	 * The number of pieces should be based on the footprint of the
	 * queryplan, such that preferrably it can be handled without
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
	cntxt->idle = 0; // this one is definitely not idle
	activeClients = mb->activeClients = MCactiveClients();

/* This code was used to experiment with block sizes, mis-using the memorylimit  variable
	if (cntxt->memorylimit){
		// the new mitosis scheme uses a maximum chunck size in MB from the client context
		m = (size_t) ((cntxt->memorylimit * 1024 *1024) / row_size);
		pieces = (int) (rowcnt / m + (rowcnt - m * pieces > 0));
	}
	if (cntxt->memorylimit == 0 || pieces <= 1){
*/
	if (pieces <= 1){
		/* the old allocation scheme */
		m = GDK_mem_maxsize / argsize;
		/* if data exceeds memory size,
		 * i.e., (rowcnt*argsize > GDK_mem_maxsize),
		 * i.e., (rowcnt > GDK_mem_maxsize/argsize = m) */
		assert(threads > 0);
		assert(activeClients > 0);
		if (rowcnt > m && m / threads / activeClients > 0) {
			/* create |pieces| > |threads| partitions such that
			 * |threads| partitions at a time fit in memory,
			 * i.e., (threads*(rowcnt/pieces) <= m),
			 * i.e., (rowcnt/pieces <= m/threads),
			 * i.e., (pieces => rowcnt/(m/threads))
			 * (assuming that (m > threads*MINPARTCNT)) */
			pieces = (int) (rowcnt / (m / threads / activeClients)) + 1;
		} else if (rowcnt > MINPARTCNT) {
		/* exploit parallelism, but ensure minimal partition size to
		 * limit overhead */
			pieces = (int) MIN(rowcnt / MINPARTCNT, (BUN) threads);
		}
	}

	/* when testing, always aim for full parallelism, but avoid
	 * empty pieces */
	FORCEMITODEBUG
	if (pieces < threads)
		pieces = (int) MIN((BUN) threads, rowcnt);
	/* prevent plan explosion */
	if (pieces > MAXSLICES)
		pieces = MAXSLICES;
	/* to enable experimentation we introduce the option to set
	 * the number of parts required and/or the size of each chunk (in K)
	 */
	mito_parts = GDKgetenv_int("mito_parts", 0);
	if (mito_parts > 0)
		pieces = mito_parts;
	mito_size = GDKgetenv_int("mito_size", 0);
	if (mito_size > 0)
		pieces = (int) ((rowcnt * row_size) / (mito_size * 1024));

	if (pieces <= 1){
		pieces = 0;
		goto bailout;
	}

	/* at this stage we have identified the #chunks to be used for the largest table */
	limit = mb->stop;
	slimit = mb->ssize;
	if (newMalBlkStmt(mb, mb->stop + 2 * estimate) < 0)
		throw(MAL,"optimizer.mitosis", SQLSTATE(HY013) MAL_MALLOC_FAIL);
	estimate = 0;

	schema = getVarConstant(mb, getArg(target, 2)).val.sval;
	table = getVarConstant(mb, getArg(target, 3)).val.sval;
	for (i = 0; i < limit; i++) {
		int upd = 0, qtpe, rtpe = 0, qv, rv;
		InstrPtr matq, matr = NULL;
		p = old[i];

		if (getModuleId(p) != sqlRef ||
			!(getFunctionId(p) == bindRef ||
			  getFunctionId(p) == bindidxRef ||
			  getFunctionId(p) == tidRef)) {
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
		if (mt < 0 && (strcmp(schema, getVarConstant(mb, getArg(p, 2 + upd)).val.sval) ||
			       strcmp(table, getVarConstant(mb, getArg(p, 3 + upd)).val.sval))) {
			pushInstruction(mb, p);
			continue;
		}
		/* we keep the original bind operation, because it allows for
		 * easy undo when the mergtable can not do something */
		// pushInstruction(mb, p);

		qtpe = getVarType(mb, getArg(p, 0));

		matq = newInstructionArgs(NULL, matRef, newRef, pieces + 1);
		getArg(matq, 0) = getArg(p, 0);

		if (upd) {
			matr = newInstructionArgs(NULL, matRef, newRef, pieces + 1);
			getArg(matr, 0) = getArg(p, 1);
			rtpe = getVarType(mb, getArg(p, 1));
		}

		for (j = 0; j < pieces; j++) {
			q = copyInstruction(p);
			if( q == NULL){
				for (; i<limit; i++)
					if (old[i])
						pushInstruction(mb,old[i]);
				GDKfree(old);
				throw(MAL,"optimizer.mitosis", SQLSTATE(HY013) MAL_MALLOC_FAIL);
			}
			q = pushInt(mb, q, j);
			q = pushInt(mb, q, pieces);

			qv = getArg(q, 0) = newTmpVariable(mb, qtpe);
			if (upd) {
				rv = getArg(q, 1) = newTmpVariable(mb, rtpe);
			}
			pushInstruction(mb, q);
			matq = addArgument(mb, matq, qv);
			if (upd)
				matr = addArgument(mb, matr, rv);
		}
		pushInstruction(mb, matq);
		if (upd)
			pushInstruction(mb, matr);
		freeInstruction(p);
	}
	for (; i<slimit; i++)
		if (old[i])
			freeInstruction(old[i]);
	GDKfree(old);

    /* Defense line against incorrect plans */
    	msg = chkTypes(cntxt->usermodule, mb, FALSE);
	if (!msg)
        	msg = chkFlow(mb);
	if (!msg)
        	msg = chkDeclarations(mb);
    /* keep all actions taken as a post block comment */
bailout:
	usec = GDKusec()- usec;
    snprintf(buf,256,"%-20s actions=%d time=" LLFMT " usec","mitosis", pieces, usec);
    newComment(mb,buf);
	if( pieces > 0)
		addtoMalBlkHistory(mb);
	return msg;
}
