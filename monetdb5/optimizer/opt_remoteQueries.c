/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0.  If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 *
 * Copyright 1997 - July 2008 CWI, August 2008 - 2022 MonetDB B.V.
 */

#include "monetdb_config.h"
#include "opt_remoteQueries.h"
#include "mal_interpreter.h"	/* for showErrors() */
#include "mal_builder.h"

/*
 * The instruction sent is produced with a variation of call2str
 * from the debugger.
 */
static str
RQcall2str(MalBlkPtr mb, InstrPtr p)
{
	int k;
	size_t len=1;
	str msg;
	str s,cv= NULL;

	msg = (str) GDKmalloc(BUFSIZ);
	if (msg == NULL)
		return NULL;
	msg[0]='#';
	msg[1]=0;
	if( p->barrier)
		strcat(msg, operatorName(p->barrier));

	if( p->retc > 1) strcat(msg,"(");
	len = strlen(msg);
	for (k = 0; k < p->retc; k++) {
		sprintf(msg+len, "%s", getVarName(mb,getArg(p,k)));
		if (k < p->retc - 1)
			strcat(msg,",");
		len = strlen(msg);
	}
	if( p->retc > 1) strcat(msg,")");
	sprintf(msg+len,":= %s.%s(",getModuleId(p),getFunctionId(p));
	s = strchr(msg, '(');
	if (s) {
		s++;
		*s = 0;
		len = strlen(msg);
		for (k = p->retc; k < p->argc; k++) {
			VarPtr v = getVar(mb, getArg(p, k));
			if( isVarConstant(mb, getArg(p,k)) ){
				if( v->type == TYPE_void) {
					sprintf(msg+len, "nil");
				} else {
					if ((cv = VALformat(&v->value)) == NULL) {
						GDKfree(msg);
						return NULL;
					}
					sprintf(msg+len,"%s:%s",cv, ATOMname(v->type));
					GDKfree(cv);
				}

			} else
				sprintf(msg+len, "%s", getVarName(mb, getArg(p,k)));
			if (k < p->argc - 1)
				strcat(msg,",");
			len = strlen(msg);
		}
		strcat(msg,");");
	}
/* printf("#RQcall:%s\n",msg);*/
	return msg;
}
/*
 * The algorithm follows the common scheme used so far.
 * Instructions are taken out one-by-one and copied
 * to the new block.
 *
 * A local cache of connections is established, because
 * the statements related to a single remote database
 * should be executed in the same stack context.
 * A pitfall is to create multiple connections with
 * their isolated runtime environment.
 */
#define lookupServer(X)\
	/* lookup the server connection */\
	if( location[getArg(p,0)] == 0){\
		db = 0;\
		if( isVarConstant(mb,getArg(p,X)) )\
			db= getVarConstant(mb, getArg(p,X)).val.sval;\
		for(k=0; k<dbtop; k++)\
			if( strcmp(db, dbalias[k].dbname)== 0)\
				break;\
		\
		if( k== dbtop){\
			r= newInstruction(mb,mapiRef,lookupRef);\
			j= getArg(r,0)= newTmpVariable(mb, TYPE_int);\
			r= addArgument(mb,r, getArg(p,X));\
			pushInstruction(mb,r);\
			dbalias[dbtop].dbhdl= j;\
			dbalias[dbtop++].dbname= db;\
			if( dbtop== 127) dbtop--;\
		} else j= dbalias[k].dbhdl;\
		location[getArg(p,0)]= j;\
	} else j= location[getArg(p,0)];

#define prepareRemote(X)\
	r= newInstruction(mb,mapiRef,rpcRef);\
	getArg(r,0)= newTmpVariable(mb, X);\
	r= addArgument(mb,r,j);

#define putRemoteVariables()\
	for(j=p->retc; j<p->argc; j++)\
	if( location[getArg(p,j)] == 0 && !isVarConstant(mb,getArg(p,j)) ){\
		q= newInstruction(0, mapiRef, putRef);\
		getArg(q,0)= newTmpVariable(mb, TYPE_void);\
		q= addArgument(mb,q,location[getArg(p,j)]);\
		q= pushStr(mb,q, getVarName(mb,getArg(p,j)));\
		q= addArgument(mb,q,getArg(p,j));\
		pushInstruction(mb,q);\
	}

#define remoteAction()\
	s= RQcall2str(mb,p);\
	r= pushStr(mb,r,s+1);\
	GDKfree(s);\
	pushInstruction(mb,r);\
	freeInstruction(p);\
	actions++;

typedef struct{
	str dbname;
	int dbhdl;
} DBalias;

str
OPTremoteQueriesImplementation(Client cntxt, MalBlkPtr mb, MalStkPtr stk, InstrPtr pci)
{
	InstrPtr p, q, r, *old;
	int i, j, k, cnt, limit, slimit, actions=0;
	int remoteSite,collectFirst;
	int *location;
	DBalias *dbalias;
	int dbtop;
	char buf[BUFSIZ],*s, *db;
	ValRecord cst;
	str msg = MAL_SUCCEED;

	cst.vtype= TYPE_int;
	cst.val.ival= 0;
	cst.len = 0;

	(void) cntxt;
	(void) stk;

	limit = mb->stop;
	slimit = mb->ssize;
	old = mb->stmt;

	location= (int*) GDKzalloc(mb->vsize * sizeof(int));
	if ( location == NULL)
		throw(MAL, "optimizer.remote", SQLSTATE(HY013) MAL_MALLOC_FAIL);
	dbalias= (DBalias*) GDKzalloc(128 * sizeof(DBalias));
	if (dbalias == NULL){
		GDKfree(location);
		throw(MAL, "optimizer.remote", SQLSTATE(HY013) MAL_MALLOC_FAIL);
	}
	dbtop= 0;

	if ( newMalBlkStmt(mb, mb->ssize) < 0){
		GDKfree(dbalias);
		GDKfree(location);
		throw(MAL, "optimizer.remote", SQLSTATE(HY013) MAL_MALLOC_FAIL);
	}

	for (i = 0; i < limit; i++) {
		p = old[i];

		/* detect remote instructions */
		cnt=0;
		for(j=0; j<p->argc; j++)
			if (location[getArg(p,j)])
				cnt++;

		/* detect remote variable binding */

		if( (getModuleId(p)== mapiRef && getFunctionId(p)==bindRef)){
			if( p->argc == 3 && getArgType(mb,p,1) == TYPE_int ) {
				int tpe;
				j = getArg(p,1); /* lookupServer with key */
				tpe = getArgType(mb,p,0);
				/* result is remote */
				location[getArg(p,0)]= j;

				/* turn the instruction into a local one */
				/* one argument less */
				p->argc--;
				/* only use the second argument (string) */
				getArg(p,1)= getArg(p,2);

				getModuleId(p) = bbpRef;

				prepareRemote(tpe)
				putRemoteVariables()
				remoteAction()
			} else
				pushInstruction(mb,p);
		} else if( (getModuleId(p)== sqlRef && getFunctionId(p)==evalRef) ){
			if( p->argc == 3){
				/* a remote sql eval is needed */
				lookupServer(1)

				/* turn the instruction into a local one */
				/* one argument less */
				p->argc--;
				/* only use the second argument (string) */
				getArg(p,1)= getArg(p,2);

				prepareRemote(TYPE_void)

				s= RQcall2str(mb,p);
				r= pushStr(mb,r,s+1);
				GDKfree(s);
				pushInstruction(mb,r);
				freeInstruction(p);
				actions++;
			}
		} else if( (getModuleId(p)== sqlRef && getFunctionId(p)==bindRef) ){

			if( p->argc == 6 && getArgType(mb,p,4) == TYPE_str ) {
				int tpe;
				j = getArg(p,1); /* lookupServer with key */
				tpe = getArgType(mb,p,0);

				lookupServer(4)

				/* turn the instruction into a local one */
				k = defConstant(mb, TYPE_int, &cst);
				if( k>=0){
					getArg(p,4)= k;
					prepareRemote(tpe)
					putRemoteVariables()
					remoteAction()
				}
			} else
				pushInstruction(mb,p);
		} else
		if(getModuleId(p)== sqlRef && getFunctionId(p)== binddbatRef) {

			if( p->argc == 5 && getArgType(mb,p,3) == TYPE_str ) {
				lookupServer(3)

				/* turn the instruction into a local one */
				k= defConstant(mb, TYPE_int, &cst);
				if( k >= 0){
					getArg(p,3)= defConstant(mb, TYPE_int, &cst);
					prepareRemote(TYPE_void)
					putRemoteVariables()
					remoteAction()
				}
			} else {
				pushInstruction(mb,p);
			}
		} else
		if( getModuleId(p) == optimizerRef || cnt == 0 || p->barrier) /* local only or flow control statement */
			pushInstruction(mb,p);
		else {
			/*
			 * The hard part is to decide what to do with instructions that
			 * contain a reference to a remote variable.
			 * In the first implementation we use the following policy.
			 * If there are multiple sites involved, all arguments are
			 * moved local for processing. Moreover, all local arguments
			 * to be shipped should be simple.
			 */
			remoteSite=0;
			collectFirst= FALSE;
			for(j=0; j<p->argc; j++)
			if( location[getArg(p,j)]){
				if (remoteSite == 0)
					remoteSite= location[getArg(p,j)];
				else if( remoteSite != location[getArg(p,j)])
					collectFirst= TRUE;
			}
			if( getModuleId(p)== ioRef || (getModuleId(p)== sqlRef
					&& (getFunctionId(p)== resultSetRef ||
				getFunctionId(p)== rsColumnRef)))
				 collectFirst= TRUE;

			/* local BATs are not shipped */
			if( remoteSite && collectFirst== FALSE)
				for(j=p->retc; j<p->argc; j++)
				if( location[getArg(p,j)] == 0 &&
					isaBatType(getVarType(mb,getArg(p,j))))
						collectFirst= TRUE;

			if (collectFirst){
				/* perform locally */
				for(j=p->retc; j<p->argc; j++)
				if( location[getArg(p,j)]){
					q= newInstruction(0,mapiRef,rpcRef);
					getArg(q,0)= getArg(p,j);
					q= addArgument(mb,q,location[getArg(p,j)]);
					snprintf(buf,BUFSIZ,"io.print(%s);",
						getVarName(mb,getArg(p,j)) );
					q=  pushStr(mb,q,buf);
					pushInstruction(mb,q);
				}
				pushInstruction(mb,p);
				/* as of now all the targets are also local */
				for(j=0; j<p->retc; j++)
					location[getArg(p,j)]= 0;
				actions++;
			} else if (remoteSite){
				/* single remote site involved */
				r= newInstruction(mb,mapiRef,rpcRef);
				getArg(r,0)= newTmpVariable(mb, TYPE_void);
				r= addArgument(mb, r, remoteSite);

				for(j=p->retc; j<p->argc; j++)
				if( location[getArg(p,j)] == 0 && !isVarConstant(mb,getArg(p,j)) ){
					q= newInstruction(0,mapiRef,putRef);
					getArg(q,0)= newTmpVariable(mb, TYPE_void);
					q= addArgument(mb, q, remoteSite);
					q= pushStr(mb,q, getVarName(mb,getArg(p,j)));
					q= addArgument(mb, q, getArg(p,j));
					pushInstruction(mb,q);
				}
				s= RQcall2str(mb, p);
				pushInstruction(mb,r);
				(void) pushStr(mb,r,s+1);
				GDKfree(s);
				for(j=0; j<p->retc; j++)
					location[getArg(p,j)]= remoteSite;
				freeInstruction(p);
				actions++;
			} else
				pushInstruction(mb,p);
		}
	}
	for(; i<slimit; i++)
	if( old[i])
		pushInstruction(mb, old[i]);
	GDKfree(old);
	GDKfree(location);
	GDKfree(dbalias);

	/* Defense line against incorrect plans */
	if( actions){
		msg = chkTypes(cntxt->usermodule, mb, FALSE);
		if (!msg)
			msg = chkFlow(mb);
		if (!msg)
			msg = chkDeclarations(mb);
	}
	/* keep actions taken as a fake argument*/
	(void) pushInt(mb, pci, actions);
	return msg;
}
