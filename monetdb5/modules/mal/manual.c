/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0.  If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 *
 * Copyright 1997 - July 2008 CWI, August 2008 - 2016 MonetDB B.V.
 */

/*
 * (c) Martin Kersten
 * This module provides a wrapping of the help function in the .../mal/mal_modules.c
 * and the list of all MAL functions for analysis using SQL.
 */
#include "monetdb_config.h"
#include "manual.h"

str
MANUALcreateOverview(Client cntxt, MalBlkPtr mb, MalStkPtr stk, InstrPtr pci)
{
	BAT *mod, *fcn, *sig, *adr, *com;
	bat *mx = getArgReference_bat(stk,pci,0);
	bat *fx = getArgReference_bat(stk,pci,1);
	bat *sx = getArgReference_bat(stk,pci,2);
	bat *ax = getArgReference_bat(stk,pci,3);
	bat *cx = getArgReference_bat(stk,pci,4);
	Module s;
	Module* moduleList;
	int length;
	int j, k, ftop, top = 0;
	Symbol t;
	Module list[256]; 
	MalBlkPtr blks[25000];
	str mtab[25000];
	str ftab[25000];
	str hlp[25000];
	char buf[BUFSIZ], *tt;

	mod = COLnew(0, TYPE_str, 0, TRANSIENT);
	fcn = COLnew(0, TYPE_str, 0, TRANSIENT);
	sig = COLnew(0, TYPE_str, 0, TRANSIENT);
	adr = COLnew(0, TYPE_str, 0, TRANSIENT);
	com = COLnew(0, TYPE_str, 0, TRANSIENT);
	if( sig == NULL || adr == NULL || com == NULL){
		if(mod) BBPunfix(mod->batCacheid);
		if(fcn) BBPunfix(fcn->batCacheid);
		if(sig) BBPunfix(sig->batCacheid);
		if(adr) BBPunfix(adr->batCacheid);
		if(com) BBPunfix(com->batCacheid);
	}

	list[top++] = cntxt->nspace;
	getModuleList(&moduleList, &length);
	while (top < 256 && top <= length) {
		list[top] = moduleList[top - 1];
		top++;
	}
	freeModuleList(moduleList);

	for(k = 0; k < top; k++){
		s = list[k];
		ftop = 0;
		if( s->space)
		for(j=0;j<MAXSCOPE;j++)
		if(s->space[j]){
			for(t= s->space[j];t!=NULL;t=t->peer) {
				mtab[ftop]= t->def->stmt[0]->modname;
				ftab[ftop]= t->def->stmt[0]->fcnname;
				hlp[ftop]= t->def->help;
				blks[ftop]= t->def;
				ftop++;
				if( ftop == 25000)
					throw(MAL,"manual.functions","Out of space");
			}
		}

		for(j=0; j < ftop; j++){
			BUNappend(mod,mtab[j],TRUE);
			BUNappend(fcn,ftab[j],TRUE);
			buf[0]=0;
			BUNappend(com, hlp[j] ? hlp[j]:buf, TRUE);
			fcnDefinition(blks[j], getInstrPtr(blks[j],0), buf, TRUE, buf, BUFSIZ);
			tt = strstr(buf,"address ");
			if( tt){
				*tt = 0;
				tt += 8;
			}
			BUNappend(sig,buf,TRUE);
			buf[0]=0;
			BUNappend(adr,tt?tt:buf,TRUE);
		}
	}

	BBPkeepref( *mx = mod->batCacheid);
	BBPkeepref( *fx = fcn->batCacheid);
	BBPkeepref( *sx = sig->batCacheid);
	BBPkeepref( *ax = adr->batCacheid);
	BBPkeepref( *cx = com->batCacheid);
	(void)mb;
	return MAL_SUCCEED;
}
