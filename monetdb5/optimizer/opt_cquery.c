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
 * Copyright August 2008-2015 MonetDB B.V.
 * All Rights Reserved.
 */

/*
 * (author) M. Kersten
 * Assume simple queries . 
 */
/*
 * We keep a flow dependency table to detect.
 */
#include "monetdb_config.h"
#include "opt_cquery.h"
#include "opt_deadcode.h"
#include "mal_interpreter.h"    /* for showErrors() */
#include "mal_builder.h"
#include "opt_dataflow.h"

#define MAXBSKT 64
#define getStreamTableInfo(S,T) \
	for(fnd=0, k= 0; k< btop; k++) \
	if( strcmp(schemas[k], S)== 0 && strcmp(tables[k], T)== 0 ){ \
		fnd= 1; break;\
	}

//#define DEBUG_OPT_CQUERY

str
OPTcqueryImplementation(Client cntxt, MalBlkPtr mb, MalStkPtr stk, InstrPtr pci)
{
	int i, j, k, fnd, limit, slimit;
	InstrPtr r, p, *old;
	int *alias;
	str  schemas[MAXBSKT];
	str  tables[MAXBSKT];
	int input[MAXBSKT]= {0};
	int output[MAXBSKT]= {0};
	int btop=0, lastmvc=0;
	int noerror=0;
	int mvcseen = 0;
	int cq= strncmp(getFunctionId(getInstrPtr(mb,0)),"cq",2) == 0;
	char buf[256];
	lng usec = GDKusec();

	(void) pci;
	// Check applicability of optimizer by looking for streams
	for(i =0; i< mb->stop; i++){
		p= getInstrPtr(mb,i);
		if( getModuleId(p) == basketRef||
			getModuleId(p) == cqueryRef)
			break;
	}
	if( i == mb->stop)
		return MAL_SUCCEED;

#ifdef DEBUG_OPT_CQUERY
	mnstr_printf(cntxt->fdout, "#cquery optimizer start\n");
	printFunction(cntxt->fdout, mb, stk, LIST_MAL_DEBUG);
#endif
	old = mb->stmt;
	limit = mb->stop;
	slimit = mb->ssize;

	/* first analyse the query for streaming tables */
	for (i = 1; i < limit && btop <MAXBSKT; i++){
		p = old[i];
		if( getModuleId(p)== basketRef && (getFunctionId(p)== registerRef || getFunctionId(p)== bindRef )  ){
			schemas[btop]= getVarConstant(mb, getArg(p,2)).val.sval;
			tables[btop]= getVarConstant(mb, getArg(p,3)).val.sval;
#ifdef DEBUG_OPT_CQUERY
			mnstr_printf(cntxt->fdout, "#cquery bind stream table %s.%s\n", schemas[btop], tables[btop]);
#endif
			for( j =0; j< btop ; j++)
			if( strcmp(schemas[j], schemas[btop])==0  && strcmp(tables[j],tables[btop]) ==0)
				break;
			input[j]= 1;
			if( j == btop)
				btop++;
		}
		if( getModuleId(p)== basketRef && getFunctionId(p) == appendRef ){
			schemas[btop]= getVarConstant(mb, getArg(p,2)).val.sval;
			tables[btop]= getVarConstant(mb, getArg(p,3)).val.sval;
#ifdef DEBUG_OPT_CQUERY
			mnstr_printf(cntxt->fdout, "#cquery append stream table %s.%s\n", schemas[btop], tables[btop]);
#endif
			for( j =0; j< btop ; j++)
			if( strcmp(schemas[j], schemas[btop])==0  && strcmp(tables[j],tables[btop]) ==0)
				break;

			output[j]= 1;
			if( j == btop)
				btop++;
		}
		if( getModuleId(p)== basketRef && getFunctionId(p) == updateRef ){
			schemas[btop]= getVarConstant(mb, getArg(p,2)).val.sval;
			tables[btop]= getVarConstant(mb, getArg(p,3)).val.sval;
#ifdef DEBUG_OPT_CQUERY
			mnstr_printf(cntxt->fdout, "#cquery update stream table %s.%s\n", schemas[btop], tables[btop]);
#endif
			for( j =0; j< btop ; j++)
			if( strcmp(schemas[j], schemas[btop])==0  && strcmp(tables[j],tables[btop]) ==0)
				break;

			output[j]= 1;
			if( j == btop)
				btop++;
		}
		if( getModuleId(p)== cqueryRef && getFunctionId(p) == basketRef){
			schemas[btop]= getVarConstant(mb, getArg(p,1)).val.sval;
			tables[btop]= getVarConstant(mb, getArg(p,2)).val.sval;
#ifdef DEBUG_OPT_CQUERY
			mnstr_printf(cntxt->fdout, "#cquery basket ref %s.%s\n", schemas[btop],tables[btop]);
#endif
			for( j =0; j< btop ; j++)
			if( strcmp(schemas[j], schemas[btop])==0  && strcmp(tables[j],tables[btop]) ==0)
				break;
			if( j == btop)
				btop++;
		}
		if( getModuleId(p)== sqlRef && getFunctionId(p) == appendRef )
			lastmvc = getArg(p,0);
		if (!cq && getModuleId(p) == sqlRef && getFunctionId(p) == affectedRowsRef )
			lastmvc = getArg(p,0);
		if( getModuleId(p)== cqueryRef && getFunctionId(p) == tumbleRef){
			lastmvc = getArg(p,1);
		}
	}
#ifdef DEBUG_OPT_CQUERY
	mnstr_printf(cntxt->fdout, "#cquery optimizer started with %d streams, mvc %d\n", btop,lastmvc);
#endif
	if( btop == MAXBSKT || btop == 0)
		return MAL_SUCCEED;

	(void) stk;

	alias = (int *) GDKzalloc(mb->vtop * 2 * sizeof(int));
	if (alias == 0)
		return MAL_SUCCEED;

	if (newMalBlkStmt(mb, slimit) < 0)
		return MAL_SUCCEED;

	pushInstruction(mb, old[0]);
	for (i = 1; i < limit; i++)
		if (old[i]) {
			p = old[i];

			if(getModuleId(p) == sqlRef && getFunctionId(p)== transactionRef){
				freeInstruction(p);
				continue;
			}
			if(getModuleId(p) == sqlRef && getFunctionId(p)== mvcRef){
				pushInstruction(mb,p);
				lastmvc = getArg(p,0);
				// watch out for second transaction in same block
				if( mvcseen){
					// unlock the tables
					for( j=btop-1; j>= 0; j--){
						r= newStmt(mb,basketRef,unlockRef);
						r= pushArgument(mb,r,lastmvc);
						r= pushStr(mb,r, schemas[j]);
						r= pushStr(mb,r, tables[j]);
						lastmvc= getArg(r,0);
					}
				}
				// register and lock all baskets used
				for( j=0; j<btop; j++){
					p= newStmt(mb,basketRef,registerRef);
					p= pushArgument(mb,p,lastmvc);
					p= pushStr(mb,p, schemas[j]);
					p= pushStr(mb,p, tables[j]);
					p= pushInt(mb,p, output[j]);
					alias[lastmvc] = getArg(p,0);
					lastmvc = getArg(p,0);
	
					p= newStmt(mb,basketRef,lockRef);
					p= pushArgument(mb,p,lastmvc);
					p= pushStr(mb,p, schemas[j]);
					p= pushStr(mb,p, tables[j]);
					lastmvc = getArg(p,0);
				}
				mvcseen=1;
				continue;
			}
			// register all baskets used after the mvc had been determined
			if (getModuleId(p) == sqlRef && getFunctionId(p) == tidRef ){
				getStreamTableInfo(getVarConstant(mb,getArg(p,2)).val.sval, getVarConstant(mb,getArg(p,3)).val.sval );
#ifdef DEBUG_OPT_CQUERY
				mnstr_printf(cntxt->fdout, "#cquery optimizer found stream %d\n",fnd);
#endif
				if( fnd){
					getModuleId(p) = basketRef;
					pushInstruction(mb,p);
					alias[getArg(p,0)] = -1;
					//freeInstruction(p);
					continue;
				}
			}

			if (getModuleId(p) == algebraRef && getFunctionId(p) == projectionRef && alias[getArg(p,1)] < 0){
				alias[getArg(p,0)] = getArg(p,2);
				freeInstruction(p);
				continue;
			}

			if (getModuleId(p) == sqlRef && getFunctionId(p) == affectedRowsRef ){
				if(cq)
					freeInstruction(p);
				else 
					pushInstruction(mb,p);
				continue;
			}

			if( getModuleId(p)== cqueryRef && getFunctionId(p)==errorRef)
				noerror++;
			if (p->token == ENDsymbol && btop > 0 && noerror==0) {
				// empty all baskets used only when we are optimizing a cq
				for(j = 0; j < btop; j++)
				if( input[j] && !output[j] ){
					r =  newStmt(mb, basketRef, tumbleRef);
					r =  pushArgument(mb,r, lastmvc);
					r =  pushStr(mb,r, schemas[j]);
					r =  pushStr(mb,r, tables[j]);
					lastmvc = getArg(r,0);
				}

				/* catch any exception left behind */
				r = newAssignment(mb);
				j = getArg(r, 0) = newVariable(mb, "SQLexception", 12, TYPE_str);
				setVarUDFtype(mb, j);
				r->barrier = CATCHsymbol;

				r = newStmt(mb,cqueryRef, errorRef);
				r = pushStr(mb, r, getModuleId(old[0]));
				r = pushStr(mb, r, getFunctionId(old[0]));
				r = pushArgument(mb, r, j);

				r = newAssignment(mb);
				getArg(r, 0) = j;
				r->barrier = EXITsymbol;
				r = newAssignment(mb);
				j = getArg(r, 0) = newVariable(mb, "MALexception",12, TYPE_str);
				setVarUDFtype(mb, j);
				r->barrier = CATCHsymbol;

				r = newStmt(mb,cqueryRef, errorRef);
				r = pushStr(mb, r, getModuleId(old[0]));
				r = pushStr(mb, r, getFunctionId(old[0]));
				r = pushArgument(mb, r, j);

				r = newAssignment(mb);
				getArg(r, 0) = j;
				r->barrier = EXITsymbol;

				/* non-contiguous queries call for releasing the lock on the basket */
				for( j=btop-1; j>= 0; j--){
					r= newStmt(mb,basketRef,unlockRef);
					r= pushArgument(mb,r,lastmvc);
					r= pushStr(mb,r, schemas[j]);
					r= pushStr(mb,r, tables[j]);
					lastmvc= getArg(r,0);
				}
					//p= newStmt(mb,basketRef,commitRef);
					//p= pushArgument(mb,p, lastmvc);
					//p= pushStr(mb,p, schemas[j]);
					//p= pushStr(mb,p, tables[j]);
					//lastmvc = getArg(p,0);
				break;
			}

			for (j = 0; j < p->argc; j++)
				if (alias[getArg(p, j)] > 0)
					getArg(p, j) = alias[getArg(p, j)];

			if ( (getModuleId(p) == sqlRef ||getModuleId(p)== basketRef) && getFunctionId(p) == appendRef ){
				getArg(p,1) = lastmvc;
				lastmvc = getArg(p,0);
			}
			if ( (getModuleId(p) == sqlRef ||getModuleId(p)== basketRef) && getFunctionId(p) == updateRef ){
				getArg(p,1) = lastmvc;
				lastmvc = getArg(p,0);
			}
			pushInstruction(mb, p);
		}

    /* take the remainder as is */
    for (; i<limit; i++)
        if (old[i])
            pushInstruction(mb,old[i]);

    /* Defense line against incorrect plans */
	chkTypes(cntxt->fdout, cntxt->nspace, mb, FALSE);
	chkFlow(cntxt->fdout, mb);
	chkDeclarations(cntxt->fdout, mb);
    /* keep all actions taken as a post block comment */
    snprintf(buf,256,"%-20s actions=%2d time=" LLFMT " usec","cquery", btop, GDKusec() - usec);
    newComment(mb,buf);

#ifdef DEBUG_OPT_CQUERY
	mnstr_printf(cntxt->fdout, "#cquery optimizer final\n");
	printFunction(cntxt->fdout, mb, stk, LIST_MAL_DEBUG);
#endif
	GDKfree(alias);
	GDKfree(old);

    if( btop > 0)
        return MAL_SUCCEED;
	throw (MAL,"optimizer.cquery", "The cquery optimizer failed to start! (btop = %d)", btop);
}
