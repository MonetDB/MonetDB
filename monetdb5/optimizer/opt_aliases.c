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

#include "monetdb_config.h"
#include "opt_aliases.h"

int
OPTisAlias(InstrPtr p){
	if( p->token == ASSIGNsymbol &&
		p->barrier == 0 && 
		p->argc == 2)
		return TRUE;
	return FALSE;
}

void
OPTaliasRemap(InstrPtr p, int *alias){
	int i;
	for(i=0; i<p->argc; i++)
		getArg(p,i) = alias[getArg(p,i)];
}

int
OPTaliasesImplementation(Client cntxt, MalBlkPtr mb, MalStkPtr stk, InstrPtr p)
{
	int i,k=1, limit, actions=0;
	int *alias;
	Lifespan span;

	(void) stk;
	(void) cntxt;
	span= setLifespan(mb);
	if( span == NULL)
		return 0;

	alias= (int*) GDKmalloc(sizeof(int)* mb->vtop);
	if (alias == NULL)
		return 0;
	for(i=0; i<mb->vtop; i++) alias[i]=i;

	limit = mb->stop;
	for (i = 1; i < limit; i++){
		p= getInstrPtr(mb,i);
		mb->stmt[k++] = p;
		if (OPTisAlias(p)){
			if( getLastUpdate(span,getArg(p,0)) == i  &&
				getBeginLifespan(span,getArg(p,0)) == i  &&
				getLastUpdate(span,getArg(p,1)) <= i ){
				alias[getArg(p,0)]= alias[getArg(p,1)];
				freeInstruction(p);
				actions++;
				k--;
			} else 
				OPTaliasRemap(p,alias);
		} else 
			OPTaliasRemap(p,alias);
	}
	for(i=k; i<limit; i++)
		mb->stmt[i]= NULL;
	mb->stop= k;
	/*
	 * The second phase is constant alias replacement should be implemented.
	 */
	GDKfree(span);
	GDKfree(alias);
	return actions;
}
