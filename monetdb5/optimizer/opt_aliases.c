/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0.  If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 *
 * Copyright 1997 - July 2008 CWI, August 2008 - 2016 MonetDB B.V.
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
	if (alias == NULL){
		GDKfree(span);
		return 0;
	}
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
