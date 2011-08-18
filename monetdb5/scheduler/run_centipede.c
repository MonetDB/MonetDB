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
 * Copyright August 2008-2011 MonetDB B.V.
 * All Rights Reserved.
 */

/*
 * @f centipede
 * @a M. Kersten
 * @- Centipede scheduling.
 * It is purposely a variation of the octopus scheduler
 */
#include "monetdb_config.h"
#include "centipede.h"
#include "mal_builder.h"
#include <mapi.h>
#include "remote.h"
#include "mal_sabaoth.h"
#include "mal_recycle.h"
#include "opt_partition.h"
#include "mal_interpreter.h"

typedef struct REGMAL{
    str fcn;
    struct REGMAL *nxt;
} *Registry;

typedef struct {
    str uri;
    str usr;
    str pwd;
    Registry nxt; /* list of registered mal functions */
    bte active;
    str conn;
    int inuse;
} Peer;

static Peer peers[MAXSITES];    /* registry of peer servers */
static int nrpeers;				/* peers active in sliced processing */
static bte slicingLocal;		/* only use local node without remote calls*/

typedef	struct{
	InstrPtr target;
	str schema, table, column;
	int type, slice;
	int lgauge, hgauge;
	ValRecord bounds[MAXSITES];
} Gauge;

static int
CENTIPEDEfindPeer(str uri)
{
    int i;
    for (i = 0; i < nrpeers; i++)
        if ( strcmp(uri, peers[i].uri) == 0 )
            return i;
    return -1;
}
/* Look for and add a peer with uri in the registry.  Return index in registry */
int
CENTIPEDEgetPeer(str uri)
{
	int i;

	i = CENTIPEDEfindPeer(uri);
	if ( i >=0 ) {
		peers[i].active = 1;
		return i;
	}
	if ( nrpeers == MAXSITES)
		return -1;
	i = nrpeers;
	peers[i].usr = GDKstrdup("monetdb");
	peers[i].uri = GDKstrdup(uri);
	peers[i].pwd = GDKstrdup("monetdb");
	peers[i].active = 1;
	peers[i].nxt = NULL;
	peers[i].inuse = 0;
	nrpeers++;
	return i;
}

/* Clean function registry of non-active peers */

void CENTIPEDEcleanFunReg(int i)
{
	Registry r, q;
	mal_set_lock(mal_contextLock,"slicing.cleanFunReg");
	r = peers[i].nxt;
	peers[i].nxt = NULL;
	mal_unset_lock(mal_contextLock,"slicing.cleanFunReg");
	while ( r ) {
			q = r->nxt;
			GDKfree(r->fcn);
			GDKfree(r);
			r = q;
	}
}

str
CENTIPEDEdiscover(Client cntxt)
{
	bat bid = 0;
	BAT *b;
	BUN p,q;
	str msg = MAL_SUCCEED;
	BATiter bi;
	char buf[BUFSIZ]= "*/slicing", *s= buf;
	int i, nrworkers = 0;

	slicingLocal = 0;

	/* we have a new list of candidate peers */
	for (i=0; i<nrpeers; i++)
		peers[i].active = 0;

	msg = RMTresolve(&bid,&s);
	if ( msg == MAL_SUCCEED) {
		b = BATdescriptor(bid);
		if ( b != NULL && BATcount(b) > 0 ) {
			bi = bat_iterator(b);
			BATloop(b,p,q){
				str t= (str) BUNtail(bi,p);
				nrworkers += CENTIPEDEgetPeer(t) >= 0;
			}
		}
		BBPreleaseref(bid);
	} else
		GDKfree(msg);

	if ( !nrworkers  ) {
	 	/* there is a last resort, local execution */
		SABAOTHgetLocalConnection(&s);

		nrworkers += CENTIPEDEgetPeer(s) >= 0;
		slicingLocal = 1;
	}

#ifdef DEBUG_RUN_CENTIPEDE
	mnstr_printf(cntxt->fdout,"Active peers discovered %d\n",nrworkers);
	for (i=0; i<nrpeers; i++)
	if ( peers[i].uri )
		mnstr_printf(cntxt->fdout,"%s\n", peers[i].uri);
#else
		(void) cntxt;
#endif

	for (i=0; i<nrpeers; i++)
		if ( !peers[i].active )
			CENTIPEDEcleanFunReg(i);

	return MAL_SUCCEED;
}

/*
 * We register the code at all worker sites and keep
 * a list of those already sent.
 */
static int
CENTIPEDEfind(int i, str qry){
	Registry r;
	for ( r= peers[i].nxt; r; r= r->nxt)
	if ( strcmp(qry, r->fcn)==0)
		return 1;
	return 0;
}
/*
 * The work division looks at the system opportunities and
 * replaces all null valued target site references in all instructions.
 * The first policy is to simply perform round robin.
 * The more advanced way is to negotiat with the remote sites.
 */


/*
 * The scheduler runs all tentacles asynchronously.
 * We should be careful in accessing a site that runs out
 * of clients or any failure. It may cause the system to
 * wait forever.
 *
 * The version argument indicates the tentacles
 * if it is time to refresh their caches.
 * It should be obtained from the recycler where we
 * know when updates have been taken place.
 *
 * The time-out parameter is not used yet.
 */

static int admitSerialConn(void *cntxt, void *mb, void *stk, void *pci)
{
	str dburi;
	int i, adm = 0;
	MalStkPtr s = (MalStkPtr) stk;
	InstrPtr p = (InstrPtr) pci;

	(void) cntxt;
	(void) mb;

	if ( p->token == NOOPsymbol )
		return 1;
/*	if ( strncmp (getFunctionId(p), "exec", 4) == 0 )
		dburi = *(str*)getArgReference(s, p,2);
	else dburi = *(str*)getArgReference(s, p,1);
*/
	/* peer uri is the first argument */
	dburi = *(str*)getArgReference(s, p,p->retc);

	MT_lock_set(&s->stklock,"serialConn");
	i = CENTIPEDEfindPeer(dburi);
	if ( i >= 0 ) {
		if ( !peers[i].inuse ) {
			adm = peers[i].inuse = 1;
#ifdef DEBUG_RUN_CENTIPEDE
			mnstr_printf( ((Client)cntxt)->fdout,"USING conn. to peer %d (%s)\n", i, dburi);
			printInstruction(((Client)cntxt)->fdout,(MalBlkPtr)mb,0, p, LIST_MAL_ALL);
#endif
		}
		else {
#ifdef DEBUG_RUN_CENTIPEDE
		 	mnstr_printf( ((Client)cntxt)->fdout,"Conn. to peer %d is BUSY\n", i);
			printInstruction(((Client)cntxt)->fdout,(MalBlkPtr)mb,0, p, LIST_MAL_ALL);
#endif
		}
	}
	else {
#ifdef DEBUG_RUN_CENTIPEDE
		mnstr_printf( ((Client)cntxt)->fdout,"No peer %s\n", dburi);
		printInstruction(((Client)cntxt)->fdout,(MalBlkPtr)mb,0, p, LIST_MAL_ALL);
#endif
	}

	MT_lock_unset(&s->stklock,"serialConn");

	return adm;
}

static int wrapupSerialConn(void *cntxt, void *mb, void *stk, void *pci)
{
	str dburi;
	int i;
	MalStkPtr s = (MalStkPtr) stk;
	InstrPtr p = (InstrPtr) pci;

	(void) cntxt;
	(void) mb;

	if ( p->token == NOOPsymbol )
		return 0;

/*	if ( strncmp (getFunctionId(p), "exec", 4) == 0 )
		dburi = *(str*)getArgReference(s, p,2);
	else dburi = *(str*)getArgReference(s, p,1); */
	dburi = *(str*)getArgReference(s, p,p->retc);
	i = CENTIPEDEfindPeer(dburi);

	MT_lock_set(&s->stklock,"serialConn");
	if ( i >= 0 ) {
		peers[i].inuse = 0;
#ifdef DEBUG_RUN_CENTIPEDE
		mnstr_printf( ((Client)cntxt)->fdout,"RELEASE conn to peer %d (%s)\n", i, dburi);
		printInstruction(((Client)cntxt)->fdout,(MalBlkPtr)mb,0, p, LIST_MAL_ALL);
#endif
	}
	MT_lock_unset(&s->stklock,"serialConn");

	return 0;
}


/*
 * @-
 * Discover available workers and register tentacles on them
 * scheduler.register():bit;
 */
str
CENTIPEDEdiscoverRegister(Client cntxt, MalBlkPtr mb, MalStkPtr stk, InstrPtr pci)
{
	bit *res = (bit*) getArgReference(stk,pci,0);
	int j, start, stop, k, found, v, pr;
	InstrPtr p;
	str msg = MAL_SUCCEED;
	ValPtr wname;

	*res = 1;	/* execute the block */

	/* Find available peers */
	msg= CENTIPEDEdiscover(cntxt);
	if ( msg )
		return msg;

	/* Logical names "worker_x" in the mal block are replaced with the
	uri-s of the associated peers in the execution stack. Logical
	workers that do not have a physical match are replaced with the
	constant string "NOTworker" to mark the instructions on this
	worker to be skipped. */

	start= getPC(mb,pci);
	for (j = start + 1; j<mb->stop ; j++){
		p= getInstrPtr(mb,j);
		if ( p->barrier == EXITsymbol )
			break;
		v = getArg(p,1);
		wname = &getVarConstant(mb, v);

		found = 0;
		for ( k = 0; k < nrworkers; k++ )
			if ( strcmp(wname->val.sval, workers[k].name) == 0 ){
				found = 1;
				break;
			}
		if ( found ) {
			garbageElement(cntxt, &stk->stk[v]);
			pr = workers[k].pnum;
			stk->stk[v].val.sval = GDKstrdup(peers[pr].uri);
			stk->stk[v].len = (int) strlen(stk->stk[v].val.sval);
		} else {	/* disable instruction */
			garbageElement(cntxt, &stk->stk[v]);
			stk->stk[v].val.sval = GDKstrdup("NOTworker");
			stk->stk[v].len = (int) strlen(stk->stk[v].val.sval);
			p->token = NOOPsymbol;
		}
	}

	/* Register tentacle functions at peers */
	stop = j;
	if ( !octopusLocal ){	/*skip registration for local execution*/
		stk->admit = &admitSerialConn;
		stk->wrapup = &wrapupSerialConn;
		msg = runMALdataflow(cntxt,mb,start,stop,stk,0,pci);
		stk->admit = NULL;
		stk->wrapup = NULL;
	}
	*res = 0;
	return msg;
}

str CENTIPEDEregister(Client cntxt, MalBlkPtr mb, MalStkPtr stk, InstrPtr pci)
{
	int i, k;
	str conn, fname, dburi, msg = MAL_SUCCEED;

	(void) cntxt;
	(void) mb;
	dburi = *(str*)getArgReference(stk,pci,1);

#ifdef DEBUG_RUN_CENTIPEDE
		mnstr_printf(GDKout,"connect to  uri %s\n", dburi);
#endif

	msg = CENTIPEDEconnect(&conn, &dburi);
	if ( msg )
		return msg;

	i =	CENTIPEDEgetPeer(dburi);

	for (k = 2;	k < pci->argc; k++) {
		fname = *(str*)getArgReference(stk,pci,k);

		if( !CENTIPEDEfind(i, fname) ){
			msg = RMTregisterInternal(cntxt, conn, octopusRef, fname);

#ifdef DEBUG_RUN_CENTIPEDE
			mnstr_printf(GDKout,"centipede.%s registered at site %s\n",
				fname,dburi);
			mnstr_printf(GDKout,"reply: %s\n",msg?msg:"ok");
#endif
			if ( msg == MAL_SUCCEED){
				Registry r= (Registry) GDKzalloc(sizeof(struct REGMAL));
				r->fcn = GDKstrdup(fname);
				r->nxt = peers[i].nxt;
				peers[i].nxt = r;
			}
			else
				return msg;
		}
	}

	return msg;
}
