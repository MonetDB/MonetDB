/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0.  If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 *
 * Copyright 2008-2015 MonetDB B.V.
 */

/*
 * (c) Martin Kersten
 * This module introduces a series of commands that provide access
 * to the help information stored in the runtime environment.
 *
 * The manual bulk operations ease offline inspection of all function definitions.
 */
#include "monetdb_config.h"
#include "manual.h"

/*
 * The manual help overview merely lists the mod.function names
 * together with the help oneliner in texi format for inclusion in the documentation.
 */

str
MANUALcreateOverview(Client cntxt, MalBlkPtr mb, MalStkPtr stk, InstrPtr pci)
{
	BAT *sig, *adr, *com;
	bat *sx = getArgReference_bat(stk,pci,0);
	bat *ax = getArgReference_bat(stk,pci,1);
	bat *cx = getArgReference_bat(stk,pci,2);
	stream *f= cntxt->fdout;
	Module s= cntxt->nspace;
	int j,z;
	Symbol t;
	Module list[256]; 
	int k, ftop, top=0;
	MalBlkPtr blks[25000];
	str hlp[25000];
	char buf[BUFSIZ], *tt;

	sig = BATnew(TYPE_void, TYPE_str, 0, TRANSIENT);
	adr = BATnew(TYPE_void, TYPE_str, 0, TRANSIENT);
	com = BATnew(TYPE_void, TYPE_str, 0, TRANSIENT);
	if( sig == NULL || adr == NULL || com == NULL){
		if(sig) BBPunfix(sig->batCacheid);
		if(adr) BBPunfix(adr->batCacheid);
		if(com) BBPunfix(com->batCacheid);
	}
    BATseqbase(sig, 0);
    BATseqbase(adr, 0);
    BATseqbase(com, 0);

    BATkey(sig, TRUE);
    BATkey(adr, TRUE);
    BATkey(com, TRUE);

	if(s==NULL || f==NULL){
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

		for(z=0; z<ftop; z++){
			buf[0]=0;
			BUNappend(com, hlp[z] ? hlp[z]:buf, TRUE);
			fcnDefinition(blks[z], getInstrPtr(blks[z],0), buf, TRUE, buf, BUFSIZ);
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
