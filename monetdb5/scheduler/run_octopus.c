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
 * @f run_octopus
 * @a M. Kersten
 * @+ Octopus scheduling
 * The octopus modules enable compute cloud based processing of SQL queries.
 * The optimizer splits a plan managed by the mitosis and mergetable into
 * independent functions by backtracking the instruction flow starting
 * at the aggregation points.
 * This leads to a series of MAL functions with possibly quite some
 * instruction overlap when run at a single node,
 * which will not be a problem when the recycler is active.
 *
 * The octopus scheduler takes over control of a MAL execution by
 * re-directing requests to multiple sites. If there are no sites known,
 * then the code is executed linearly as is.
 *
 * The scheduler runs all tentacles asynchronously if possible.
 * To make our live easier, we assume that all tentacles are
 * grouped together in a guarded block as follows:
 *
 * @verbatim
 * barrier (parallel,version):= scheduler.octopus();
 * a:= octopus.exec_qry(sitename,fcnname,version,arg...);
 * ...
 * b:= octopus.exec_qry(sitename,fcnname,version,arg...);
 * z:= mat.pack(a,...,b);
 * exit (parallel,version);
 * @end verbatim
 *
 * This way the scope of the instructions can be easily deduced.
 * Moreover, the underlying data flow execution facility can
 * be used to schedule and initiate interaction with remote sites.
 * The octopus scheduler merely ensures that there are enough
 * work threads available.
 *
 * To make the octopus work, the scheduler needs a list of databases
 * to play with. This list it gets from Merovingian by resolving
 * all with the property 'shared=octopus' (set by monetdb).
 * The default is to use the local database as a target.
 */
/*
 * @+ Octopus scheduling implementation
 * The discovery phase consists of establishing connections with
 * the (remote) database servers.
 */
#include "monetdb_config.h"
#include "mal_interpreter.h"
#include "mat.h"
#include "run_octopus.h"
#include "optimizer.h"
#include <mapi.h>
#include "remote.h"
#include "mal_sabaoth.h"

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

typedef struct {
	int pnum;
	str name;
/*	str uri;
	str usr;
	str pwd;
	lng *bids;
	Registry nxt;*/	/* list of registered queries */
} Worker;

#define MAXSITES 2048	/* should become dynamic at some point */
static Peer peers[MAXSITES];	/* registry of peer servers */
static Worker workers[MAXSITES];  /* registry of workers for the current query */
static int nrworkers = 0;
static int nrpeers=0;
static bte octopusLocal=0;
bte optTarget = 1;
/*sht bidStrategy = 1;
#define BID_TRANS	1
#define BID_COVER	2
*/

static int
OCTOPUSfindPeer(str uri)
{
	int i;
	for (i=0; i<nrpeers; i++){
		if ( strcmp(uri, peers[i].uri) == 0 ){
			return i;
		}
	}
	return -1;
}

/* Look for and add a peer with uri in the registry.
Return index in registry */
static int
OCTOPUSgetPeer(str uri)
{
	int i;

	i = OCTOPUSfindPeer(uri);
	if ( i >=0 ) {
		peers[i].active = 1;
		return i;
	}
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

static void OCTOPUScleanFunReg(int i)
{
	Registry r, q;
	r = peers[i].nxt;
	while ( r ) {
			q = r->nxt;
			GDKfree(r->fcn);
			GDKfree(r);
			r = q;
	}
	peers[i].nxt = NULL;
}

str
OCTOPUSconnect(str *c, str *dburi)
{
	int i;
	str msg = MAL_SUCCEED;
	str conn = NULL, scen = "msql";


	i = OCTOPUSfindPeer(*dburi);
	if ( i < 0 ){
		msg = createException(MAL, "octopus.connect", "Server %s is not registered", *dburi);
		*c = NULL;
		return msg;
	}

	if ( peers[i].conn == NULL ) {
		msg = RMTconnectScen(&conn, &peers[i].uri, &peers[i].usr, &peers[i].pwd, &scen);
		if ( msg == MAL_SUCCEED )
			peers[i].conn = GDKstrdup(conn);
		else {
			*c = NULL;
			return msg;
		}
	}

	*c = GDKstrdup(peers[i].conn);
	return msg;
}

str
OCTOPUSgetVersion(int *res)
{
	*res = 1;
	return MAL_SUCCEED;
}

static str
OCTOPUSdiscover(Client cntxt){
	bat bid = 0;
	BAT *b;
	BUN p,q;
	str msg = MAL_SUCCEED;
	BATiter bi;
	int i;
	char buf[BUFSIZ]= "*/octopus", *s= buf;

	nrworkers = 0;
	octopusLocal = 0;
	for (i=0; i<nrpeers; i++)
		peers[i].active = 0;

	msg = RMTresolve(&bid,&s);
	if ( msg == MAL_SUCCEED) {
		b = BATdescriptor(bid);
		if ( b != NULL && BATcount(b) > 0 ) {
			bi = bat_iterator(b);
			BATloop(b,p,q){
				str t= (str) BUNtail(bi,p);

				workers[nrworkers].pnum = OCTOPUSgetPeer(t); /*ref to peers registry*/
				snprintf(buf,BUFSIZ,"worker_%d",nrworkers);
				workers[nrworkers].name = GDKstrdup(buf);

#ifdef DEBUG_RUN_OCTOPUS
				mnstr_printf(cntxt->fdout,"Worker site %d %s\n", nrworkers, t);
#endif
				nrworkers++;
			}
		}
		BBPreleaseref(bid);
	}

	if ( !nrworkers  ) {
	 	/* there is a last resort, local execution */
		SABAOTHgetLocalConnection(&s);

		workers[nrworkers].pnum = OCTOPUSgetPeer(s); /*ref to peers registry*/
		snprintf(buf,BUFSIZ,"worker_%d",nrworkers);
		workers[nrworkers].name = GDKstrdup(buf);

#ifdef DEBUG_RUN_OCTOPUS
		mnstr_printf(cntxt->fdout,"Worker site %d %s\n", nrworkers, s);
#endif
		nrworkers++;
		octopusLocal = 1;
	}

#ifdef DEBUG_RUN_OCTOPUS
	mnstr_printf(cntxt->fdout,"Octopus workers %d\n",nrworkers);
#else
		(void) cntxt;
#endif

	for (i=0; i<nrpeers; i++)
		if ( !peers[i].active )
			OCTOPUScleanFunReg(i);

	return MAL_SUCCEED;
}

/*
 * @-
 * We first register the tentacle code at all worker sites and keep
 * a list of those already sent.
 */
static int
OCTOPUSfind(int i, str qry){
	Registry r;
	for ( r= peers[i].nxt; r; r= r->nxt)
	if ( strcmp(qry, r->fcn)==0)
		return 1;
	return 0;
}


/*
 * @-
 * The work division looks at the system opportunities and
 * replaces all null valued target site references in all instructions.
 * The first policy is to simply perform round robin.
 * The more advanced way is to negotiat with the remote sites.
 */


/*
 * @-
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
	i = OCTOPUSfindPeer(dburi);
	if ( i >= 0 ) {
		if ( !peers[i].inuse ) {
			adm = peers[i].inuse = 1;
#ifdef DEBUG_RUN_OCTOPUS
			mnstr_printf( ((Client)cntxt)->fdout,"USING conn. to peer %d (%s)\n", i, dburi);
			printInstruction(((Client)cntxt)->fdout,(MalBlkPtr)mb,0, p, LIST_MAL_ALL);
#endif
		}
		else {
#ifdef DEBUG_RUN_OCTOPUS
		 	mnstr_printf( ((Client)cntxt)->fdout,"Conn. to peer %d is BUSY\n", i);
			printInstruction(((Client)cntxt)->fdout,(MalBlkPtr)mb,0, p, LIST_MAL_ALL);
#endif
		}
	}
	else {
#ifdef DEBUG_RUN_OCTOPUS
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
	i = OCTOPUSfindPeer(dburi);

	MT_lock_set(&s->stklock,"serialConn");
	if ( i >= 0 ) {
		peers[i].inuse = 0;
#ifdef DEBUG_RUN_OCTOPUS
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
OCTOPUSdiscoverRegister(Client cntxt, MalBlkPtr mb, MalStkPtr stk, InstrPtr pci)
{
	bit *res = (bit*) getArgReference(stk,pci,0);
	int j, start, stop, k, found, v, pr;
	InstrPtr p;
	str msg = MAL_SUCCEED;
	ValPtr wname;

	*res = 1;	/* execute the block */

	/* Find available peers */
	msg= OCTOPUSdiscover(cntxt);
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

str OCTOPUSregister(Client cntxt, MalBlkPtr mb, MalStkPtr stk, InstrPtr pci)
{
	int i, k;
	str conn, fname, dburi, msg = MAL_SUCCEED;

	(void) cntxt;
	(void) mb;
	dburi = *(str*)getArgReference(stk,pci,1);

#ifdef DEBUG_RUN_OCTOPUS
		mnstr_printf(GDKout,"connect to  uri %s\n", dburi);
#endif

	msg = OCTOPUSconnect(&conn, &dburi);
	if ( msg )
		return msg;

	i =	OCTOPUSgetPeer(dburi);

	for (k = 2;	k < pci->argc; k++) {
		fname = *(str*)getArgReference(stk,pci,k);

		if( !OCTOPUSfind(i, fname) ){
			msg = RMTregisterInternal(cntxt, conn, octopusRef, fname);

#ifdef DEBUG_RUN_OCTOPUS
			mnstr_printf(GDKout,"octopus.%s registered at site %s\n",
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

str
OCTOPUSbidding(Client cntxt, MalBlkPtr mb, MalStkPtr stk, InstrPtr pci)
{
	bit *res = (bit*) getArgReference(stk,pci,0);
	int j, start, k, i;
	InstrPtr p;
	lng bid;
	str *wname;
	str msg = MAL_SUCCEED;

    *res = 1;       /* execute the block */

    start = getPC(mb,pci);
    for (j = start + 1; j< mb->stop ; j++){
    	p = getInstrPtr(mb,j);
    	if ( p->barrier == EXITsymbol )
        	break;
	}

	if ( octopusLocal ) { /* skip bidding for local execution */
		for (k = start + 1; k < j ; k++){
	    	p = getInstrPtr(mb,k);
			wname = (str*) getArgReference(stk,p,p->retc);
			if ( strcmp(*wname, "NOTworker") == 0 )
        		bid = -1;
			else bid = 0;
			for ( i = 0; i < p->retc; i++)
				*(lng*)getArgReference(stk,p,i) = bid;
		}
	}
    else { 	/* distributed execution */
		stk->admit = &admitSerialConn;
		stk->wrapup = &wrapupSerialConn;
		msg = runMALdataflow(cntxt,mb,start,j,stk,0,pci);
		stk->admit = NULL;
		stk->wrapup = NULL;
	}

	*res = 0;
    return msg;

}

static lng
putJob(int t, int tcnt, lng **bid, bte *busy, bte **rsch)
{
	int k,r;
	bte *cur=NULL;
	char fl = 0;
	lng c=0, rc, maxc=0;

	cur = (bte*) GDKzalloc(sizeof(bte) * tcnt);

	for (k = 0; k < tcnt; k++){
		if (busy[k]) continue;
		if ( bid[t][k] < 0 ) continue; /* bid<0 => I don't want to participate */
		busy[k] = 1;
		if (t < tcnt - 1){
			rc = putJob(t+1,tcnt,bid,busy,&cur);
			switch (optTarget){
			case 1:	/* maximize the total */
				c = rc + bid[t][k];
				break;
			case 2:	/* makespan */
				c = (rc > bid[t][k])? rc: bid[t][k];
				break;
			}
		}
		else {
			c = bid[t][k];
		}

		if (fl) {
			if (c > maxc){
				(*rsch)[t] = k;
				maxc = c;
				for (r = t+1; r < tcnt; r++)
					(*rsch)[r] = cur[r];
			}
		}
		else {
			(*rsch)[t] = k;
			maxc = c;
			for (r = t+1; r < tcnt; r++)
				(*rsch)[r] = cur[r];
			fl = 1;
		}
		busy[k] = 0;
	}

	GDKfree(cur);
	return maxc;
}


str
OCTOPUSmakeSchedule(Client cntxt, MalBlkPtr mb, MalStkPtr stk, InstrPtr pci)
{
	int tcnt=1, offset, i, j, k, pr, v;
	str msg = MAL_SUCCEED;
	lng **bid=NULL;
	bte *sch=NULL, *busy;

	(void) mb;
	(void) cntxt;

	tcnt = *(int*) getArgReference(stk,pci,pci->retc);
	if ( pci->argc != tcnt*tcnt + tcnt + 1 )
		return "Wrong argument number of makeSchedule";
	offset = tcnt + 1;
	bid = (lng**) GDKzalloc(sizeof(lng*) * tcnt);
	sch = (bte*) GDKzalloc(sizeof(bte) * tcnt);
	busy = (bte*) GDKzalloc(sizeof(bte) * tcnt);

	for ( j = 0; j < tcnt; j++){
		bid[j] = (lng*) GDKzalloc(sizeof(lng) * tcnt);
	}
	for ( i = offset; i < pci->argc; i++) {
		j = (int) (i-offset) / tcnt;
		k = (int) (i-offset) % tcnt;
		bid[j][k] = *(lng*) getArgReference(stk,pci,i);
	}

	/* compute schedule */
/*	Round Robin
	for ( j = 0; j < tcnt; j++)
		sch[j] = j;
*/
	/* Optimal */
	putJob(0,tcnt,bid,busy,&sch);

	/* set returned schedule variables */
	for ( j = 0; j < tcnt; j++){
		v = getArg(pci,j);
		pr = workers[sch[j]].pnum;
		stk->stk[v].val.sval = GDKstrdup(peers[pr].uri);
		stk->stk[v].len = (int) strlen(stk->stk[v].val.sval);
	}

	for ( j= 0; j < tcnt; j++)
		GDKfree(bid[j]);
	GDKfree(bid);
	GDKfree(sch);
	GDKfree(busy);
	return msg;
}

static str
OCTOPUSdisconnect(Client cntxt)
{
	int i;
	str msg = MAL_SUCCEED;

	for ( i=0; i< nrpeers; i++)
		if ( peers[i].active && peers[i].conn != NULL ) {
		msg = RMTdisconnect(cntxt,&peers[i].conn);
		GDKfree(peers[i].conn);
		peers[i].conn = NULL;
	}
	return msg;
}

str
OCTOPUSrun(Client cntxt, MalBlkPtr mb, MalStkPtr stk, InstrPtr pci)
{
	bit *res = (bit*) getArgReference(stk,pci,0);
	int j, start;
	InstrPtr p;
	str msg = MAL_SUCCEED;

	*res = 1;	/* execute the block */
	start = getPC(mb,pci);
	for (j = start + 1; j< mb->stop ; j++){
		p = getInstrPtr(mb,j);
		if ( p->barrier == EXITsymbol )
			break;
	}

	stk->admit = &admitSerialConn;
	stk->wrapup = &wrapupSerialConn;
	msg = runMALdataflow(cntxt,mb,start,j,stk,0,pci);
	stk->admit = NULL;
	stk->wrapup = NULL;

	*res = 0; 	/* skip to the exit */
	OCTOPUSdisconnect(cntxt);
	return msg;

}
