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
	BAT *sig, *adr, *com;
	bat *sx = getArgReference_bat(stk,pci,0);
	bat *ax = getArgReference_bat(stk,pci,1);
	bat *cx = getArgReference_bat(stk,pci,2);
	Module s= cntxt->nspace;
	int j, k, ftop, top=0;
	Symbol t;
	Module list[256]; 
	MalBlkPtr blks[25000];
	str hlp[25000];
	char buf[BUFSIZ], *tt;

	sig = COLnew(0, TYPE_str, 0, TRANSIENT);
	adr = COLnew(0, TYPE_str, 0, TRANSIENT);
	com = COLnew(0, TYPE_str, 0, TRANSIENT);
	if( sig == NULL || adr == NULL || com == NULL){
		if(sig) BBPunfix(sig->batCacheid);
		if(adr) BBPunfix(adr->batCacheid);
		if(com) BBPunfix(com->batCacheid);
	}

	if(s==NULL){
		return MAL_SUCCEED;
	}
	list[top++]=s;
	while(s->outer && top < 256 ){ list[top++]= s->outer;s=s->outer;}

	for(k=0;k<top;k++){
		s= list[k];
		ftop = 0;
		if( s->subscope)
		for(j=0;j<MAXSCOPE;j++)
		if(s->subscope[j]){
			for(t= s->subscope[j];t!=NULL;t=t->peer) {
				hlp[ftop]= t->def->help;
				blks[ftop]= t->def;
				ftop++;
				if( ftop == 25000)
					throw(MAL,"manual.functions","Ouf of space");
			}
		}

		for(j=0; j<ftop; j++){
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

	BBPkeepref( *sx = sig->batCacheid);
	BBPkeepref( *ax = adr->batCacheid);
	BBPkeepref( *cx = com->batCacheid);
	(void)mb;
	return MAL_SUCCEED;
}

str
MANUALhelp(Client cntxt, MalBlkPtr mb, MalStkPtr stk, InstrPtr pci)
{
	char **msg;
	int i;
	str *text= getArgReference_str(stk,pci,1);
	(void) mb;		/* fool compiler */

	msg= getHelp(cntxt->nspace,*text,1);
	if( msg && msg[0] ){
		for(i=0; msg[i];i++){
			mal_unquote(msg[i]);
			mnstr_printf(cntxt->fdout,"%s\n",msg[i]);
			GDKfree(msg[i]);
		}
	}
	if( msg)
		GDKfree(msg);
	return MAL_SUCCEED;
}
