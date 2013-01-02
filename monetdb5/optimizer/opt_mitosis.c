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
 * Copyright August 2008-2013 MonetDB B.V.
 * All Rights Reserved.
*/
#include "monetdb_config.h"
#include "opt_mitosis.h"
#include "opt_octopus.h"
#include "mal_interpreter.h"

static int eligible(MalBlkPtr mb )
{
	InstrPtr p;
	int i;
	for (i=1; i< mb->stop; i++){
		p = getInstrPtr(mb,i);
		if ( getModuleId(p) == sqlRef && getFunctionId(p) == assertRef &&
			p->argc > 2 && getArgType(mb,p,2) == TYPE_str &&
			isVarConstant(mb,getArg(p,2)) &&
			getVarConstant(mb, getArg(p,2)).val.sval != NULL &&
			strstr(getVarConstant(mb, getArg(p,2)).val.sval, "PRIMARY KEY constraint") )
		return 0;
	}
	return 1;
}

int
OPTmitosisImplementation(Client cntxt, MalBlkPtr mb, MalStkPtr stk, InstrPtr p)
{
	int i, j, k, limit, estimate=0, tpe, pieces=1;
	str schema=0, table=0;
	VarRecord low,hgh;
	BUN slice;
	wrd r = 0, rowcnt=0;	/* table should be sizeable to consider parallel execution*/
	InstrPtr q,*old, target= 0, matq;
	size_t argsize = 6 * sizeof(lng);
	/*     per op:   6 = (2+1)*2   <=  2 args + 1 res, each with head & tail */
	int threads = GDKnr_threads ? GDKnr_threads:1;
	ValRecord vr;
	VarPtr loc,rows;

	(void)cntxt;
	(void) stk;
	if ( !eligible(mb))
		return 0;
	
	/* locate the largest non-partitioned table */
	for (i=1; i< mb->stop; i++){
		q= getInstrPtr(mb,i);
		if (getModuleId(q)!= sqlRef || getFunctionId(q)!=bindRef )
			continue;
		/* don't split insert BATs */
		if (getVarConstant(mb, getArg(q,5)).val.ival == 1 )
			continue;
		if( q->argc > 6 )
			continue; /* already partitioned */
		/*
		 * @-
		 * The SQL optimizer already collects the counts of the
		 * base table and passes them on as a row property.
		 * All pieces for a single subplan should ideally fit together.
		 */
		r = getVarRows(mb, getArg(q, 0));
		if (r >= rowcnt){
			rowcnt = r;
			target = q;
			estimate++;
			r = 0;
		}
	}
	if (target == 0)
		return 0;
	/*
	 * The number of pieces should be based on the footprint of the queryplan,
	 * such that preferrably it can be handled without swapping intermediates.
	 * For the time being we just go for pieces that fit into memory in isolation.
	 * A fictive rowcount is derived based on argument types, such that all pieces
	 * would fit into memory conveniently for processing. We attempt to use
	 * not more threads than strictly needed.
	 * Experience shows that the pieces should not be too small.
	 * If we should limit to |threads| is still an open issue.
	 */
	if ( (i = OPTlegAdviceInternal(mb,stk,p)) > 0 )
		pieces = i;
	else {
		r = (wrd) (monet_memory / argsize);
		/* if data exceeds memory size,
		 * i.e., (rowcnt*argsize > monet_memory),
		 * i.e., (rowcnt > monet_memory/argsize = r) */
		if (rowcnt > r && r/threads > 0)
			/* create |pieces| > |threads| partitions such that
			 * |threads| partitions at a time fit in memory,
			 * i.e., (threads*(rowcnt/pieces) <= r),
			 * i.e., (rowcnt/pieces <= r/threads),
			 * i.e., (pieces => rowcnt/(r/threads))
			 * (assuming that (r > threads*MINPARTCNT)) */
			pieces = (int) (rowcnt / (r/threads)) + 1;
		else
		/* exploit parallelism, but ensure minimal partition size to limit overhead */
		if (rowcnt > MINPARTCNT)
			pieces = (int) MIN ( (rowcnt / MINPARTCNT) , (wrd) threads);
		/* when testing, always aim for full parallelism, but avoid empty pieces */
		FORCEMITODEBUG
			if (pieces < threads)
				pieces = (int) MIN ( (wrd) threads , rowcnt );
		/* prevent plan explosion */
		if (pieces > MAXSLICES)
			pieces = MAXSLICES;

	}
	if (pieces <= 1)
		return 0;
	OPTDEBUGmitosis
		mnstr_printf(cntxt->fdout,"#opt_mitosis: target is %s.%s "
			" with " SSZFMT " rows into " SSZFMT " rows/piece %d threads %d pieces\n",
				getVarConstant(mb, getArg(target,2)).val.sval,
				getVarConstant(mb, getArg(target,3)).val.sval,
				rowcnt, r, threads, pieces);

	old = mb->stmt;
	limit= mb->stop;
	if ( newMalBlkStmt(mb,mb->ssize + 2* estimate) < 0 )
		return 0;
	estimate=0;

	schema = getVarConstant(mb, getArg(target,2)).val.sval;
	table = getVarConstant(mb, getArg(target,3)).val.sval;
	for(i=0;i<limit;i++){
		p= old[i];

		if (getModuleId(p)!= sqlRef || 
			!(getFunctionId(p)==bindRef ||
			  getFunctionId(p)==bindidxRef )){
			pushInstruction(mb,p);
			continue;
		}
		/* don't split insert BATs */
		if (getVarConstant(mb, getArg(p,5)).val.ival == 1 ){
			pushInstruction(mb,p);
			continue;
		}
		/* Don't split the (index) bat if we already have identified a range */
		/* This will happen if we inline separately optimized routines */
		if ( p->argc != 6 ){
			pushInstruction(mb,p);
			continue;
		}
		if (strcmp(schema, getVarConstant(mb, getArg(p,2)).val.sval) ||
		    strcmp(table, getVarConstant(mb, getArg(p,3)).val.sval)){
			pushInstruction(mb,p);
			continue;
		}
		/* we keep the original bind operation, because it allows
			for easy undo when the mergtable can not do something
		*/
		pushInstruction(mb,p);
		
		slice= (BUN) (rowcnt/pieces);
		hgh.value.vtype= low.value.vtype= TYPE_oid;
		low.value.val.oval= 0;
		tpe = getVarType(mb,getArg(p,0));

		matq= newInstruction(NULL,ASSIGNsymbol);
		setModuleId(matq,matRef);
		setFunctionId(matq,newRef);
		getArg(matq,0)= getArg(p,0);

		for(j=0; j < pieces; j++){
			q= copyInstruction(p);
			q= pushInt(mb,q,j);
			q= pushInt(mb,q,pieces);
			/*q= pushOid(mb,q,low.value.val.oval);*/
			if (j+1 < pieces) {
				hgh.value.val.oval= low.value.val.oval+slice;
			} else {
				assert(rowcnt <= (wrd) BUN_MAX);
				hgh.value.val.oval= (BUN) rowcnt;
			}
			/*q = pushOid(mb,q,hgh.value.val.oval);*/

			k= getArg(q,0)= newTmpVariable(mb,tpe);
			setVarUDFtype(mb,k);
			setVarUsed(mb,k);
			/*
			 * @-
			 * The target variable should inherit file location and row count
			 */
			loc = varGetProp(mb, getArg(p,0), fileProp);
			if ( loc ){
				memset((char*) &vr, 0, sizeof(vr));
				varSetProp(mb, k, fileProp, op_eq, VALset(&vr, TYPE_str, GDKstrdup(loc->value.val.sval)));
			}
			rows = varGetProp(mb, getArg(p,0), rowsProp);
			if ( rows){
				wrd prows = rows->value.val.wval / pieces + 1;
				memset((char*) &vr, 0, sizeof(vr));
				varSetProp(mb, k, rowsProp, op_eq, VALset(&vr, TYPE_wrd, &prows));
			}

			if (getFunctionId(p) == binddbatRef) {
				varSetProp(mb, k, PropertyIndex("tlb"), op_gte, (ptr) &low.value);
				varSetProp(mb, k, PropertyIndex("tub"), op_lt, (ptr) &hgh.value);
			} else {
				varSetProp(mb, k, PropertyIndex("hlb"), op_gte, (ptr) &low.value);
				varSetProp(mb, k, PropertyIndex("hub"), op_lt, (ptr) &hgh.value);
			}
			low.value.val.oval += slice;
			pushInstruction(mb,q);
			matq= pushArgument(mb,matq,k);
		}
		pushInstruction(mb,matq);
	}
	return 1;
}
