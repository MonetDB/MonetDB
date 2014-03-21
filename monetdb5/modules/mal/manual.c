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

/*
 * (c) Martin Kersten
 * This module introduces a series of commands that provide access
 * to the help information stored in the runtime environment.
 *
 * The manual bulk operations ease offline inspection of all function definitions.
 */
#include "monetdb_config.h"
#include "manual.h"


static int cmpModName(Module *f, Module *l){
    return strcmp((*f)->name, (*l)->name);
}

str
MANUALcreateIndex(Client cntxt, MalBlkPtr mb, MalStkPtr stk, InstrPtr pci)
{
	stream *f = cntxt->fdout;
	Module s = cntxt->nspace;
	int j,z,rows,cols;
	Symbol t;
	InstrPtr sig;
	Module list[256]; int k, top=0, ftop, fnd;
	InstrPtr fcn[5000];
	int r, c, *x = NULL, x_sze = 0;

	(void) mb;
	(void) stk;
	(void) pci;

	if(s==NULL || f==NULL){
		return MAL_SUCCEED;
	}
	list[top++]=s;
	while(s->outer){ list[top++]= s->outer;s=s->outer;}

	if(top>1) qsort(list, top, sizeof(Module),
		(int(*)(const void *, const void *))cmpModName);

	cols = 4;
	mnstr_printf(f,"@multitable @columnfractions .24 .24 .24 .24\n");
	for(k=0;k<top;k++){
		s= list[k];
		ftop = 0;
		if( s->subscope)
		for(j=0;j<MAXSCOPE;j++)
		if(s->subscope[j]){
			for(t= s->subscope[j];t!=NULL;t=t->peer) {
				sig= getSignature(t);
				fnd = 0;
				fnd= *getFunctionId(sig) == '#';
				for(z=0; z<ftop; z++)
				if( strcmp(getFunctionId(fcn[z]),getFunctionId(sig))==0){
					fnd++;
					break;
				}
				if( fnd == 0 && ftop<5000)
					fcn[ftop++] = sig;
			}
		}
		for(j=0; j<ftop; j++)
		for(z=j+1; z<ftop; z++)
		if( strcmp(getFunctionId(fcn[j]),getFunctionId(fcn[z]))  >0) {
			 sig= fcn[j]; fcn[j]=fcn[z]; fcn[z]= sig;
		}
		mnstr_printf(f,"@" "item\n");
		rows = (ftop + cols - 1) / cols;
		if (x == NULL) {
			/* 2x* to allow for empty/skipped fields/columns */
			x_sze = 2 * cols * rows;
			x = (int*) GDKmalloc(x_sze * sizeof(int));
		} else if (2 * cols * rows > x_sze) {
			x_sze = 2 * cols * rows;
			x = (int*) GDKrealloc(x, x_sze * sizeof(int));
		}
		if( x == NULL){
			GDKerror("dumpManualOverview"MAL_MALLOC_FAIL);
			return MAL_SUCCEED;
		}
		for (z = 0; z < rows; z++) {
			x[cols * z] = z;
		}
		for (c = 1; c < cols; c++) {
			for (r = 0; r < rows; r++) {
				int i = (cols * r) + c - 1;
				if (z < ftop &&
				    (x[i] < 0 || strlen(getModuleId(fcn[x[i]])) + strlen(getFunctionId(fcn[x[i]])) < (size_t)(80 / cols))) {
					x[i+1] = z++;
				} else {
					/* HACK to avoid long names running into next column in printed version */
					x[i+1] = -1;
				}
			}
		}
		z = 0;
		for (r = 0; r < rows; r++) {
			for (c = 0; c < cols; c++) {
				str it[] = {"item", "tab"};
				mnstr_printf(f,"@" "%s\n", it[(c > 0)]);
				if (x[z] != -1) {
					mnstr_printf(f,"%s.%s\n",
						getModuleId(fcn[x[z]]), getFunctionId(fcn[x[z]]));
				}
				z++;
			}
		}
	}
	mnstr_printf(f,"@end multitable\n");
	if (x != NULL)
		GDKfree(x);
	return MAL_SUCCEED;
}

/*
 * The manual help overview merely lists the mod.function names
 * together with the help oneliner in texi format for inclusion in the documentation.
 */

str
MANUALcreateSummary(Client cntxt, MalBlkPtr mb, MalStkPtr stk, InstrPtr pci)
{
	stream *f= cntxt->fdout;
	Module s= cntxt->nspace;
	int j,z;
	Symbol t;
	InstrPtr sig;
	Module list[256]; int k, ftop, fnd,top=0;
	InstrPtr fcn[5000];
	str hlp[5000],msg;
	str hlp_texi = NULL;
	size_t hlp_texi_len = 0;

	(void) mb;
	(void) stk;
	(void) pci;

	if(s==NULL || f==NULL){
		return MAL_SUCCEED;
	}
	list[top++]=s;
	while(s->outer ){ list[top++]= s->outer;s=s->outer;}

	if(top>1) qsort(list, top, sizeof(Module),
		(int(*)(const void *, const void *))cmpModName);

	for(k=0;k<top;k++){
		s= list[k];
		ftop = 0;
		if( s->subscope)
		for(j=0;j<MAXSCOPE;j++)
		if(s->subscope[j]){
			for(t= s->subscope[j];t!=NULL;t=t->peer) {
				sig= getSignature(t);
				fnd = 0;
				fnd= *getFunctionId(sig) == '#';
				for(z=0; z<ftop; z++)
				if( strcmp(getFunctionId(fcn[z]),getFunctionId(sig))==0){
					if( hlp[z] == 0)
						hlp[z]= t->def->help;
					fnd++;
					break;
				}
				if( fnd == 0 && ftop<5000){
					hlp[ftop]= t->def->help;
					fcn[ftop++] = sig;
				}
			}
		}

		for(j=0; j<ftop; j++)
		for(z=j+1; z<ftop; z++)
		if( strcmp(getFunctionId(fcn[j]),getFunctionId(fcn[z]))  >0) {
			msg= hlp[j]; hlp[j]=hlp[z]; hlp[z]= msg;
			 sig= fcn[j]; fcn[j]=fcn[z]; fcn[z]= sig;
		}

		for(z=0; z<ftop; z++){
			mnstr_printf(f,"%s.%s", getModuleId(fcn[z]), getFunctionId(fcn[z]));
			if( hlp[z] ) {
				str hlp_ = hlp[z];
				size_t hlp_len = 2*strlen(hlp[z]) + 1;
				if (hlp_texi == NULL) {
					hlp_texi = (str) GDKmalloc(hlp_len);
					hlp_texi_len = hlp_len;
				} else if (hlp_len > hlp_texi_len) {
					hlp_texi = (str) GDKrealloc(hlp_texi, hlp_len);
					hlp_texi_len = hlp_len;
				}
				if (hlp_texi != NULL) {
					str st, o = hlp_texi;
					for ( st= hlp[z]; *st; st++){
						if (*st == '\n' || *st == '\t')
							*o++ = ' ';
						else
						if( *st == '\\') {
							*o++ = ' ';
							st++;
						} else
							*o++ = *st;
					}
					*o++ = '\0';
					hlp_ = hlp_texi;
				}
				if (strlen(getModuleId(fcn[z])) + strlen(getFunctionId(fcn[z])) >= 20) {
					/* HACK to avoid long names running into help text in printed version */
				}
				mnstr_printf(f," | %s", hlp_);
			}
			mnstr_printf(f,"\n");
		}
	}
	if (hlp_texi != NULL)
		GDKfree(hlp_texi);
	return MAL_SUCCEED;
}

str
MANUALcompletion(Client cntxt, MalBlkPtr mb, MalStkPtr stk, InstrPtr pci)
{
	str *text= (str*) getArgReference(stk,pci,1);
	(void) mb;		/* fool compiler */
	dumpHelpTable(cntxt->fdout, cntxt->nspace, *text,1);
	return MAL_SUCCEED;
}

str
MANUALhelp(Client cntxt, MalBlkPtr mb, MalStkPtr stk, InstrPtr pci)
{
	char **msg;
	int i;
	str *text= (str*) getArgReference(stk,pci,1);
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

str
MANUALsearch(Client cntxt, MalBlkPtr mb, MalStkPtr stk, InstrPtr pci)
{
	char **msg;
	int i;
	str *pat= (str*) getArgReference(stk,pci,1);
	(void) mb;		/* fool compiler */

	msg= getHelpMatch(*pat);
	if( msg && msg[0] ){
		for(i=0; msg[i];i++){
			mal_unquote(msg[i]);
			mnstr_printf(cntxt->fdout,"%s\n",msg[i]+1);
			GDKfree(msg[i]);
		}
	}
	if( msg)
		GDKfree(msg);
	return MAL_SUCCEED;
}

