/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0.  If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 *
 * Copyright 1997 - July 2008 CWI, August 2008 - 2017 MonetDB B.V.
 */

/*
 * @a M. Kersten
 * The experimental octopus/centipede modules enabled compute cloud based processing of SQL queries.
 * Their scheduler takes over control of a MAL execution by
 * re-directing requests to multiple sites. If there are no sites known,
 * then the code is executed locally as is.
 * The scheduler runs all subqueries asynchronously if possible.
 *
 * To make our live easier, we assume that all subqueries are
 * grouped together in a guarded block as follows:
 *
 * @verbatim
 * barrier parallel:= scheduler.srvpool();
 * (s1,...,sn) :=  srvpool.query("queryplan");
 * a:= user.S0_1_stub(s1,arg...);
 * ...
 * b:= srvpool.S0_1_stub(sn,arg...);
 * z:= mat.pack(a,...,b);
 * exit parallel;
 * @end verbatim
 *
 * We assume that the database versions are synchronised.
 *
 * To make the scheduler work, it needs a list of database instances.
 * This list it gets from Merovingian by resolving
 * all with the property 'shared=octopus' or 'shared=centipede' (set by monetdb).
 * The default is to use the local database as a target.
 *
 * Ideally, we would not like to ship query plans with each query, but
 * this requires knowledge that the remote server has not been restarted
 * since the last call. For the time being we assume the connection remains in tact.
 *
 */
#ifdef HAVE_MAPI

#include "monetdb_config.h"
#include "mal_interpreter.h"
#include "mal_dataflow.h"
#include "mat.h"
#include "srvpool.h"
#include "optimizer.h"
#include <mapi.h>
#include "remote.h"
#include "mal_sabaoth.h"

/*
 * All remotely known query blocks are listed here
*/
typedef struct REGMAL{
	str fcn;
	struct REGMAL *nxt;
} *Registry;

/*
 * All open connections are grouped here.
*/
typedef struct {
	str uri;	/* uri associated with remote server */
	str usr;	/* user credentials for using the server */
	str pwd;
	Registry nxt; /* list of registered mal functions */
	str conn;	/* remote handle */
} Server;

#define DEBUG_RUN_SRVPOOL
#define MAXSITES 2048	/* should become dynamic at some point */
static Server servers[MAXSITES];	/* registry of servers */
static int srvtop = 0;
static int srvbaseline= 0;
static str srvpattern = NULL;
static int localExecution= FALSE;

/* 
 * The partition optimizer requires the number of peers.
 * This information ideally comes from the centipede scheduler,
 * which maintains the collection of remote servers involved
 * in query processing.
 * A direct call is also provided for testing.
 * A local catalog of partitioned BATs is kept around to further
 * improve remote processing.
*/
str SRVsetServers(Client cntxt, MalBlkPtr mb, MalStkPtr stk, InstrPtr pci)
{
	(void) cntxt;
	if( getArgType(mb,pci,1) == TYPE_int) {
		srvbaseline = *getArgReference_int(stk,pci,1);
		if ( srvbaseline <= 0) {
			srvbaseline = 0 ;
			throw(MAL,"scheduler.setServers","Illegal number of servers");
		}
	} else
	if( getArgType(mb,pci,1) == TYPE_str) {
		if ( srvpattern)
			GDKfree(srvpattern);
		srvpattern = GDKstrdup(*getArgReference_str(stk,pci,1));
	}
	return MAL_SUCCEED;
}

/* Look for and add a server with uri in the registry, give it a local unique name.  Return index in registry */

static int
SRVPOOLnewServer(str uri)
{
	servers[srvtop].uri = GDKstrdup(uri);
	servers[srvtop].usr = GDKstrdup("monetdb");
	servers[srvtop].pwd = GDKstrdup("monetdb");
	servers[srvtop].nxt = NULL;
	srvtop++;
	return srvtop-1;
}


static int
SRVPOOLgetServer(str uri)
{
	int i;

	for (i=0; i<srvtop; i++)
	if ( strcmp(uri, servers[i].uri) == 0 )
		return i;

	return SRVPOOLnewServer(uri);
}

/* Clean function registry of non-active servers */

static void
SRVPOOLcleanup(int i)
{
	Registry r, q;
	r = servers[i].nxt;
	while ( r ) {
			q = r->nxt;
			GDKfree(r->fcn);
			GDKfree(r);
			r = q;
	}
	servers[i].nxt = NULL;
}


/* remove a server from the pool */
static void
SRVPOOLdropServer(int idx)
{
	int j;
	SRVPOOLcleanup(idx);
	if ( servers[idx].uri ) GDKfree(servers[idx].uri);
	servers[idx].uri = NULL;
	if ( servers[idx].usr ) GDKfree(servers[idx].usr);
	servers[idx].pwd = NULL;
	if ( servers[idx].pwd ) GDKfree(servers[idx].pwd);
	servers[idx].pwd = NULL;
	if ( servers[idx].conn ) GDKfree(servers[idx].conn);
	servers[idx].conn = NULL;
	for ( j = idx; j <srvtop -1; j++)
		servers[j]= servers[idx + 1];
	servers[j].uri = servers[j].usr = servers[j].pwd = servers[j].conn =  NULL;
	servers[j].nxt = NULL;
	srvtop --;
}

/* logically disconnect from all servers */
static str
SRVPOOLdisconnect(Client cntxt)
{
	int i;
	str msg = MAL_SUCCEED;

	for ( i=0; i< srvtop; i++)
	if ( servers[i].conn != NULL ) {
		if( msg) 
			GDKfree(msg);
		msg = RMTdisconnect(cntxt,&servers[i].conn);
		GDKfree(servers[i].conn);
		servers[i].conn = NULL;
	}
	return msg;
}
/* restart server pool */
str SRVPOOLreset(Client cntxt, MalBlkPtr mb, MalStkPtr stk, InstrPtr pci)
{
	int i;
	str msg;

	(void) mb;
	(void) stk;
	(void) pci;
	msg = SRVPOOLdisconnect(cntxt);
	for ( i=0; i< srvtop; i++){
		SRVPOOLcleanup(i);
		GDKfree(servers[i].uri);
	}
	memset((char*)servers, 0, sizeof(Server) * srvtop);
	srvtop = 0;
	return msg;
}

str
SRVPOOLconnect(str *c, str *uri)
{
	int i;
	str msg = MAL_SUCCEED;
	str conn = NULL, scen = "msql";


	*c = NULL;
	for ( i =0; i < srvtop; i++)
	if ( strcmp(*uri, servers[i].uri) == 0)
		break;
	if ( i == srvtop) 
		return createException(MAL, "srvpool.connect", "Server %s is not registered", *uri);

	if ( servers[i].conn == NULL ) {
		msg = RMTconnectScen(&conn, &servers[i].uri, &servers[i].usr, &servers[i].pwd, &scen);
		if ( msg == MAL_SUCCEED )
			servers[i].conn = GDKstrdup(conn);
		else 
			return msg;
	}

	*c = GDKstrdup(servers[i].conn);
	return msg;
}

/* switch local/remote execution */
str
SRVPOOLlocal(void *ret, bit *flag)
{
	(void) ret;
	localExecution= *flag != 0;
	return MAL_SUCCEED;
}

/* Look up the servers available for processing */
static str
SRVPOOLdiscover(Client cntxt)
{
	bat bid = 0;
	BAT *b;
	BUN p,q;
	str msg = MAL_SUCCEED, conn, scen = "msql";
	BATiter bi;
	int j = -1;
	char buf[BUFSIZ], *s= buf, *dbname;


	if ( srvbaseline == 0)
		srvbaseline = 2; /* for debugging */

	dbname= GDKgetenv("gdk_dbname");
	snprintf(buf,BUFSIZ,"%s/srvpool",dbname);
	if ( srvpattern == 0)
		/* set default pattern */
		srvpattern = GDKstrdup(buf);

	/* first check the validity of the connections */

	/* discover any new servers */
	msg = RMTresolve(&bid,&s);
	if ( msg == MAL_SUCCEED) {
		b = BATdescriptor(bid);
		if ( b != NULL && BATcount(b) > 0 ) {
			bi = bat_iterator(b);
			BATloop(b,p,q){
				str t= (str) BUNtail(bi,p);

				for( j = 0; j < srvtop; j++)
					if ( strcmp(servers[j].uri, t) == 0) 
						break;
				if ( servers[j].conn == NULL) {
					j = SRVPOOLnewServer(t); 
					msg = RMTconnectScen(&conn, &servers[j].uri, &servers[j].usr, &servers[j].pwd, &scen);
					if ( msg == MAL_SUCCEED )
						servers[j].conn = GDKstrdup(conn);
					else {
#ifdef DEBUG_RUN_SRVPOOL
						fprintf(stderr,"#Worker site %d reports %s \n", j, msg);
#endif
						/* Upon receiving an initialization error, the entry should be ignored */
						SRVPOOLdropServer(j);
						GDKfree(msg);
						msg = MAL_SUCCEED;
					}
				}

#ifdef DEBUG_RUN_SRVPOOL
				else 
					fprintf(stderr,"#Worker site %d alias %s %s\n", j, (servers[j].conn?servers[j].conn:""), t);
#endif
				assert(srvtop <MAXSITES);
			}
		}
		BBPunfix(bid);
	} 
	if( msg) {
		/* ignore merovingian complaints */
#ifdef DEBUG_RUN_SRVPOOL
		fprintf(stderr,"#%s\n", msg);
#endif
		GDKfree(msg);
		msg = MAL_SUCCEED;
	}

	while (srvtop < srvbaseline) {
	 	/* there is a last resort, use local execution */
		/* make sure you have enough connections */
		msg = SABAOTHgetLocalConnection(&s);
		if( msg == MAL_SUCCEED){
			j = SRVPOOLnewServer(s); /*ref to servers registry*/
			msg = RMTconnectScen(&conn, &servers[j].uri, &servers[j].usr, &servers[j].pwd, &scen);
		}
		if ( j >= 0 && msg == MAL_SUCCEED ) {
			servers[j].conn = GDKstrdup(conn);
#ifdef DEBUG_RUN_SRVPOOL
			fprintf(stderr,"#Worker site %d connection %s %s\n", j, servers[j].conn, s);
#endif
		} else
			GDKfree(msg);
	}

#ifdef DEBUG_RUN_SRVPOOL
	fprintf(stderr,"#Servers available %d\n", srvtop);
#else
	(void) cntxt;
#endif
	return msg;
}

/*
 * We first need to register the code at a worker sites and keep
 * a list of those already sent.
 */
static int
SRVPOOLfind(Client cntxt, int srv, str qry){
	Registry r, garbage;
	Symbol sym;

	for ( r= servers[srv].nxt; r; r= r->nxt)
	if ( strcmp(qry, r->fcn)==0) {
		/* double check on local user module as well */
		sym = findSymbol(cntxt->nspace, userRef, putName(qry));
		if (sym == NULL) {
			/* remove garbage */
			if ( r->nxt) {
				garbage = r->nxt;
				*r = *r->nxt;
				GDKfree(garbage);
			}
			continue;
		}
		return 1;
	}
	return 0;
}


static str
SRVPOOLregisterInternal(Client cntxt, str uri, str fname)
{
	int srv;
	str msg = MAL_SUCCEED, con;
	Registry r;

	srv =	SRVPOOLgetServer(uri);
	if ( srv < 0)
		throw(MAL,"srvpool.register","Server not registered");
#ifdef DEBUG_RUN_SRVPOOL
		fprintf(stderr,"#register %s at site %s\n", fname, servers[srv].conn);
#else
	(void) cntxt;
#endif
	msg = SRVPOOLconnect(&con, &uri);
	if ( msg)
		return msg;
	if ( con) GDKfree(con);
		

	if( !SRVPOOLfind(cntxt, srv, fname) ){
		if ( servers[srv].conn ) {
			if ( srvtop > 1) /* register remotely */
				msg = RMTregisterInternal(cntxt, servers[srv].conn, userRef, fname);
#ifdef DEBUG_RUN_SRVPOOL
				if ( msg) {
					Symbol sf = findSymbol(cntxt->nspace, userRef,putName(fname));
					if (sf){
						fprintf(stderr,"#Failed to register\n");
						fprintFunction(stderr, sf->def, 0, LIST_MAL_DEBUG);
					} else
						mnstr_printf(cntxt->fdout,"#undefined registration function\n");
				}
#endif
		} else
			throw(MAL,"srvpool.register","Site not reachable, connection missing");

		if ( msg == MAL_SUCCEED) {
			r= (Registry) GDKzalloc(sizeof(struct REGMAL));
			r->fcn = GDKstrdup(fname);
			r->nxt = servers[srv].nxt;
			servers[srv].nxt = r;
		}
	}
	return msg;
}
/* Make sure the MAL function identified in the srvpool.exec() is remotely known */
str SRVPOOLregister(Client cntxt, MalBlkPtr mb, MalStkPtr stk, InstrPtr pci)
{
	str fname, uri;

	(void) mb;
	uri = *getArgReference_str(stk,pci,1);
	fname = *getArgReference_str(stk,pci,2);

	return SRVPOOLregisterInternal(cntxt, uri, fname);
}


/*
 * The server pool actions are encapsulated in a barrier block
 * which is executed in dataflow manner
*/
str
SRVPOOLscheduler(Client cntxt, MalBlkPtr mb, MalStkPtr stk, InstrPtr pci)
{
	int *res = getArgReference_int(stk,pci,0), pc=0;
	str msg = MAL_SUCCEED;

	(void) mb;
	(void) stk;
	(void) pci;

	if ( localExecution) {
		*res = -1; /* skip this one */
	} else {
		/* only discover workers once */
		if ( srvtop == 0)
			SRVPOOLdiscover(cntxt);
		/* execute block in parallel */
		pc = getPC(mb, pci);
		if (pc < 0 ||  pc > pci->jump)
			throw(MAL,"scheduler.srvpool","Illegal statement range");
		if ( pc >= 0)
			msg = runMALdataflow(cntxt, mb, pc, pci->jump, stk);
		*res = int_nil;  /* continue at end of block */
	}
	return msg;
}

str
SRVPOOLquery(Client cntxt, MalBlkPtr mb, MalStkPtr stk, InstrPtr pci)
{
	int i,j, fnd =0;
	str plan = *getArgReference_str(stk,pci,pci->retc);
	str msg = MAL_SUCCEED;

	(void) cntxt;
	(void) mb;

	if ( pci->retc > srvtop )
		throw(MAL,"srvpool.server","Not enough servers");
	for ( j= i=0; i < pci->retc && j < srvtop; j++)  {
		if ( servers[j].conn ) {
			if ( !SRVPOOLfind(cntxt, j,plan) ){
				msg = SRVPOOLregisterInternal(cntxt, servers[i].uri, plan);
				if ( msg) {
					/* registration should succeed on all hots */
#ifdef DEBUG_RUN_SRVPOOL
					fprintf(stderr,"#failed to register %s at %d\n",plan,i);
					fprintf(stderr,"#%s\n",msg);
#endif
					break;
				}
			}
			if ( SRVPOOLfind(cntxt, j,plan) ){
				*getArgReference_str(stk,pci,i++) = GDKstrdup(servers[j].conn);
#ifdef DEBUG_RUN_SRVPOOL
				fprintf(stderr,"#found %s on server %d\n",plan,j);
#endif
				fnd ++;
			}
		}
#ifdef DEBUG_RUN_SRVPOOL
		fprintf(stderr,"#server %d uri %s conn %s\n", j,
			(servers[j].uri?servers[j].uri:"null"),
			(servers[j].conn?servers[j].conn:"null"));
#endif
	}
	if (msg == MAL_SUCCEED && fnd != pci->retc)
		throw(MAL,"srvpool.server","Not enough alife connections");
	return msg;
}

/*
 * run the designated remote plan 
*/
str SRVPOOLexec(Client cntxt, MalBlkPtr mb, MalStkPtr stk, InstrPtr p)
{
	(void) cntxt;
	(void) mb;
	(void) stk;
	(void) p;
	throw(MAL,"srvpool.exec","Unexpected call");
}
#else
// this avoids a compiler warning w.r.t. empty compilation units.
int SRVPOOLdummy = 42;
#endif /* HAVE_MAPI */

