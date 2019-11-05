/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0.  If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 *
 * Copyright 1997 - July 2008 CWI, August 2008 - 2019 MonetDB B.V.
 */

/*
 * author Martin Kersten, Fabian Groffen
 * Client Management
 * Each online client is represented with an entry in the clients table.
 * The client may inspect his record at run-time and partially change its properties.
 * The administrator sees all client records and has the right to adjust global properties.
 */


#include "monetdb_config.h"
#include "clients.h"
#include "mcrypt.h"
#include "mal_scenario.h"
#include "mal_instruction.h"
#include "mal_client.h"
#include "mal_authorize.h"
#include "mal_private.h"
#include "mtime.h"

static int
pseudo(bat *ret, BAT *b, str X1,str X2) {
	char buf[BUFSIZ];
	snprintf(buf,BUFSIZ,"%s_%s", X1,X2);
	if (BBPindex(buf) <= 0 && BBPrename(b->batCacheid, buf) != 0)
		return -1;
	if (BATroles(b,X2) != GDK_SUCCEED)
		return -1;
	*ret = b->batCacheid;
	BBPkeepref(*ret);
	return 0;
}

str
CLTsetListing(Client cntxt, MalBlkPtr mb, MalStkPtr stk, InstrPtr pci)
{
	(void) mb;
	*getArgReference_int(stk,pci,0) = cntxt->listing;
	cntxt->listing = *getArgReference_int(stk,pci,1);
	return MAL_SUCCEED;
}

str
CLTgetClientId(Client cntxt, MalBlkPtr mb, MalStkPtr stk, InstrPtr pci)
{
	(void) mb;
	if(cntxt - mal_clients < 0 || cntxt - mal_clients >= MAL_MAXCLIENTS)
		throw(MAL, "clients.getClientId", "Illegal client index");
	*getArgReference_int(stk,pci,0) = (int) (cntxt - mal_clients);
	return MAL_SUCCEED;
}

str
CLTgetScenario(Client cntxt, MalBlkPtr mb, MalStkPtr stk, InstrPtr pci)
{
	(void) mb;
	if (cntxt->scenario)
		*getArgReference_str(stk,pci,0) = GDKstrdup(cntxt->scenario);
	else
		*getArgReference_str(stk,pci,0) = GDKstrdup("nil");
	if(*getArgReference_str(stk,pci,0) == NULL)
		throw(MAL, "clients.getScenario", SQLSTATE(HY001) MAL_MALLOC_FAIL);
	return MAL_SUCCEED;
}

str
CLTsetScenario(Client cntxt, MalBlkPtr mb, MalStkPtr stk, InstrPtr pci)
{
	str msg = MAL_SUCCEED;

	(void) mb;
	msg = setScenario(cntxt, *getArgReference_str(stk,pci,1));
	*getArgReference_str(stk,pci,0) = 0;
	if (msg == NULL) {
		*getArgReference_str(stk,pci,0) = GDKstrdup(cntxt->scenario);
		if(*getArgReference_str(stk,pci,0) == NULL)
			throw(MAL, "clients.setScenario", SQLSTATE(HY001) MAL_MALLOC_FAIL);
	}
	return msg;
}

static void
CLTtimeConvert(time_t l, char *s)
{
	struct tm localt;

#ifdef HAVE_LOCALTIME_R
	(void) localtime_r(&l, &localt);
#else
	/* race condition: return value could be
	 * overwritten in parallel thread before
	 * assignment complete */
	localt = *localtime(&l);
#endif

#ifdef HAVE_ASCTIME_R3
	asctime_r(&localt, s, 26);
#else
#ifdef HAVE_ASCTIME_R
	asctime_r(&localt, s);
#else
	/* race condition: return value could be
	 * overwritten in parallel thread before copy
	 * complete, however on Windows, asctime is
	 * thread-safe */
	strcpy(s, asctime(&localt)); /* asctime produces string of length 25 */
#endif
#endif
	s[24] = 0;		/* remove newline */
}

str
CLTInfo(Client cntxt, MalBlkPtr mb, MalStkPtr stk, InstrPtr pci)
{
	bat *ret=  getArgReference_bat(stk,pci,0);
	bat *ret2=  getArgReference_bat(stk,pci,0);
	BAT *b = COLnew(0, TYPE_str, 12, TRANSIENT);
	BAT *bn = COLnew(0, TYPE_str, 12, TRANSIENT);
	char buf[32]; /* 32 bytes are enough */

	(void) mb;
	if (b == 0 || bn == 0){
		if ( b != 0) BBPunfix(b->batCacheid);
		if ( bn != 0) BBPunfix(bn->batCacheid);
		throw(MAL, "clients.info", SQLSTATE(HY001) MAL_MALLOC_FAIL);
	}

	(void) sprintf(buf, ""LLFMT"", (lng) cntxt->user);
	if (BUNappend(b, "user", false) != GDK_SUCCEED ||
		BUNappend(bn, buf, false) != GDK_SUCCEED)
		goto bailout;

	if (BUNappend(b, "scenario", false) != GDK_SUCCEED ||
		BUNappend(bn, cntxt->scenario, false) != GDK_SUCCEED)
		goto bailout;

	(void) sprintf(buf, "%d", cntxt->listing);
	if (BUNappend(b, "listing", false) != GDK_SUCCEED ||
		BUNappend(bn, buf, false) != GDK_SUCCEED)
		goto bailout;

	(void) sprintf(buf, "%d", cntxt->debug);
	if (BUNappend(b, "debug", false) != GDK_SUCCEED ||
		BUNappend(bn, buf, false) != GDK_SUCCEED)
		goto bailout;

	CLTtimeConvert(cntxt->login, buf);
	if (BUNappend(b, "login", false) != GDK_SUCCEED ||
		BUNappend(bn, buf, false) != GDK_SUCCEED)
		goto bailout;
	if (pseudo(ret,b,"client","info"))
		goto bailout;
	BBPkeepref(*ret2= bn->batCacheid);
	return MAL_SUCCEED;

bailout:
	BBPunfix(b->batCacheid);
	BBPunfix(bn->batCacheid);
	throw(MAL, "clients.info", SQLSTATE(HY001) MAL_MALLOC_FAIL);
}

str
CLTLogin(bat *nme, bat *ret)
{
	BAT *b = COLnew(0, TYPE_str, 12, TRANSIENT);
	BAT *u = COLnew(0, TYPE_oid, 12, TRANSIENT);
	int i;
	char s[32];

	if (b == 0 || u == 0)
		goto bailout;

	for (i = 0; i < MAL_MAXCLIENTS; i++) {
		Client c = mal_clients+i;
		if (c->mode >= RUNCLIENT && !is_oid_nil(c->user)) {
			CLTtimeConvert(c->login, s);
			if (BUNappend(b, s, false) != GDK_SUCCEED ||
				BUNappend(u, &c->user, false) != GDK_SUCCEED)
				goto bailout;
		}
	}
	if (pseudo(ret,b,"client","login") ||
		pseudo(nme,u,"client","name"))
		goto bailout;
	return MAL_SUCCEED;

bailout:
	BBPreclaim(b);
	BBPreclaim(u);
	throw(MAL, "clients.getLogins", SQLSTATE(HY001) MAL_MALLOC_FAIL);
}

str
CLTquit(Client cntxt, MalBlkPtr mb, MalStkPtr stk, InstrPtr pci)
{
	int id;
	(void) mb;		/* fool compiler */
	
	if ( pci->argc==2)
		id = *getArgReference_int(stk,pci,1);
	else id =cntxt->idx;

	if ( !(cntxt->user == MAL_ADMIN ||  mal_clients[id].user == cntxt->user) )
		throw(MAL, "client.quit", INVCRED_ACCESS_DENIED);

	/* A user can only quite a session under the same id */
	if ( cntxt->idx == mal_clients[id].idx)
		mal_clients[id].mode = FINISHCLIENT;
	else 
		throw(MAL, "client.quit", INVCRED_ACCESS_DENIED);
	return MAL_SUCCEED;
}

/* Stopping a client in a softmanner by setting the time out marker */
str
CLTstop(Client cntxt, MalBlkPtr mb, MalStkPtr stk, InstrPtr pci)
{
	int id = *getArgReference_int(stk,pci,1);

	(void) mb;
	if (cntxt->user == mal_clients[id].user || cntxt->user == MAL_ADMIN)
		mal_clients[id].qtimeout = 1; /* stop client in one microsecond */
	/* this forces the designated client to stop at the next instruction */
	return MAL_SUCCEED;
}

/* Queries can be temporarily suspended */
str
CLTsuspend(Client cntxt, MalBlkPtr mb, MalStkPtr stk, InstrPtr pci)
{
	int *id=  getArgReference_int(stk,pci,1);
	(void) cntxt;
	(void) mb;
	if ( !(cntxt->user == MAL_ADMIN ||  mal_clients[*id].user == cntxt->user) )
		throw(MAL, "client.suspend", INVCRED_ACCESS_DENIED);
    return MCsuspendClient(*id);
}

str
CLTwakeup(void *ret, int *id)
{
    (void) ret;     /* fool compiler */
    return MCawakeClient(*id);
}

/* set session time out based in seconds */
str
CLTsetSessionTimeout(Client cntxt, MalBlkPtr mb, MalStkPtr stk, InstrPtr pci)
{
	lng sto;
	(void) mb;
	sto=  *getArgReference_lng(stk,pci,1);
	if( sto < 0)
		throw(MAL,"timeout","Session time out should be >= 0");
	cntxt->stimeout = sto * 1000 * 1000;
    return MAL_SUCCEED;
}

/* Set the current query timeout in seconds */
str
CLTsetTimeout(Client cntxt, MalBlkPtr mb, MalStkPtr stk, InstrPtr pci)
{
	lng qto,sto;
	(void) mb;
	qto=  *getArgReference_lng(stk,pci,1);
	if( qto < 0)
		throw(MAL,"timeout","Query time out should be >= 0");
	if ( pci->argc == 3){
		sto=  *getArgReference_lng(stk,pci,2);
		if( sto < 0)
			throw(MAL,"timeout","Session time out should be >= 0");
		cntxt->stimeout = sto * 1000 * 1000;
	}
	cntxt->qtimeout = qto * 1000 * 1000;
    return MAL_SUCCEED;
}

/* Retrieve the session time out */
str
CLTgetTimeout(Client cntxt, MalBlkPtr mb, MalStkPtr stk, InstrPtr pci)
{
	lng *qto=  getArgReference_lng(stk,pci,0);
	lng *sto=  getArgReference_lng(stk,pci,1);
	(void) mb;
	*qto = cntxt->qtimeout;
	*sto = cntxt->stimeout;
    return MAL_SUCCEED;
}

/* Long running queries are traced in the logger 
 * with a message from the interpreter.
 * This value should be set to minutes to avoid a lengthly log */
str
CLTsetPrintTimeout(void *ret, int *secs)
{
	(void) ret;
	if (is_int_nil(*secs))
		setqptimeout(0);
	else
		setqptimeout((lng) *secs * 60 * 1000000);
	return MAL_SUCCEED;
}

str CLTmd5sum(str *ret, str *pw) {
#ifdef HAVE_MD5_UPDATE
	char *mret = mcrypt_MD5Sum(*pw, strlen(*pw));

	if (!mret)
		throw(MAL, "clients.md5sum", SQLSTATE(HY001) MAL_MALLOC_FAIL);
	*ret = GDKstrdup(mret);
	free(mret);
	if(*ret == NULL)
		throw(MAL, "clients.md5sum", SQLSTATE(HY001) MAL_MALLOC_FAIL);
	return MAL_SUCCEED;
#else
	(void) ret;
	(void) pw;
	throw(MAL, "clients.md5sum", SQLSTATE(0A000) PROGRAM_NYI);
#endif
}

str CLTsha1sum(str *ret, str *pw) {
#ifdef HAVE_SHA1_UPDATE
	char *mret = mcrypt_SHA1Sum(*pw, strlen(*pw));

	if (!mret)
		throw(MAL, "clients.sha1sum", SQLSTATE(HY001) MAL_MALLOC_FAIL);
	*ret = GDKstrdup(mret);
	free(mret);
	if(*ret == NULL)
		throw(MAL, "clients.sha1sum", SQLSTATE(HY001) MAL_MALLOC_FAIL);
	return MAL_SUCCEED;
#else
	(void) ret;
	(void) pw;
	throw(MAL, "clients.sha1sum", SQLSTATE(0A000) PROGRAM_NYI);
#endif
}

str CLTripemd160sum(str *ret, str *pw) {
#ifdef HAVE_RIPEMD160_UPDATE
	char *mret = mcrypt_RIPEMD160Sum(*pw, strlen(*pw));

	if (!mret)
		throw(MAL, "clients.ripemd160sum", SQLSTATE(HY001) MAL_MALLOC_FAIL);
	*ret = GDKstrdup(mret);
	free(mret);
	if(*ret == NULL)
		throw(MAL, "clients.ripemd160sum", SQLSTATE(HY001) MAL_MALLOC_FAIL);
	return MAL_SUCCEED;
#else
	(void) ret;
	(void) pw;
	throw(MAL, "clients.ripemd160sum", SQLSTATE(0A000) PROGRAM_NYI);
#endif
}

str CLTsha2sum(str *ret, str *pw, int *bits) {
	char *mret;
	switch (*bits) {
#ifdef HAVE_SHA512_UPDATE
		case 512:
			mret = mcrypt_SHA512Sum(*pw, strlen(*pw));
			break;
#endif
#ifdef HAVE_SHA384_UPDATE
		case 384:
			mret = mcrypt_SHA384Sum(*pw, strlen(*pw));
			break;
#endif
#ifdef HAVE_SHA256_UPDATE
		case 256:
			mret = mcrypt_SHA256Sum(*pw, strlen(*pw));
			break;
#endif
#ifdef HAVE_SHA224_UPDATE
		case 224:
			mret = mcrypt_SHA224Sum(*pw, strlen(*pw));
			break;
#endif
		default:
			throw(ILLARG, "clients.sha2sum", "wrong number of bits "
					"for SHA2 sum: %d", *bits);
	}

	if (!mret)
		throw(MAL, "clients.sha2sum", SQLSTATE(HY001) MAL_MALLOC_FAIL);
	*ret = GDKstrdup(mret);
	free(mret);
	if(*ret == NULL)
		throw(MAL, "clients.sha2sum", SQLSTATE(HY001) MAL_MALLOC_FAIL);
	return MAL_SUCCEED;
}

str CLTbackendsum(str *ret, str *pw) {
	char *mret = mcrypt_BackendSum(*pw, strlen(*pw));
	if (mret == NULL)
		throw(MAL, "clients.backendsum", SQLSTATE(HY001) MAL_MALLOC_FAIL);
	*ret = GDKstrdup(mret);
	free(mret);
	if(*ret == NULL)
		throw(MAL, "clients.backendsum", SQLSTATE(HY001) MAL_MALLOC_FAIL);
	return MAL_SUCCEED;
}

str CLTaddUser(Client cntxt, MalBlkPtr mb, MalStkPtr stk, InstrPtr pci) {
	oid *ret = getArgReference_oid(stk, pci, 0);
	str *usr = getArgReference_str(stk, pci, 1);
	str *pw = getArgReference_str(stk, pci, 2);

	(void)mb;
	
	return AUTHaddUser(ret, cntxt, *usr, *pw);
}

str CLTremoveUser(Client cntxt, MalBlkPtr mb, MalStkPtr stk, InstrPtr pci) {
	str *usr;
	(void)mb;

	usr = getArgReference_str(stk, pci, 1);

	return AUTHremoveUser(cntxt, *usr);
}

str CLTgetUsername(Client cntxt, MalBlkPtr mb, MalStkPtr stk, InstrPtr pci) {
	str *ret = getArgReference_str(stk, pci, 0);
	(void)mb;

	return AUTHgetUsername(ret, cntxt);
}

str CLTgetPasswordHash(Client cntxt, MalBlkPtr mb, MalStkPtr stk, InstrPtr pci) {
	str *ret = getArgReference_str(stk, pci, 0);
	str *user = getArgReference_str(stk, pci, 1);

	(void)mb;

	return AUTHgetPasswordHash(ret, cntxt, *user);
}

str CLTchangeUsername(Client cntxt, MalBlkPtr mb, MalStkPtr stk, InstrPtr pci) {
	str *old = getArgReference_str(stk, pci, 1);
	str *new = getArgReference_str(stk, pci, 2);

	(void)mb;

	return AUTHchangeUsername(cntxt, *old, *new);
}

str CLTchangePassword(Client cntxt, MalBlkPtr mb, MalStkPtr stk, InstrPtr pci) {
	str *old = getArgReference_str(stk, pci, 1);
	str *new = getArgReference_str(stk, pci, 2);

	(void)mb;

	return AUTHchangePassword(cntxt, *old, *new);
}

str CLTsetPassword(Client cntxt, MalBlkPtr mb, MalStkPtr stk, InstrPtr pci) {
	str *usr = getArgReference_str(stk, pci, 1);
	str *new = getArgReference_str(stk, pci, 2);

	(void)mb;

	return AUTHsetPassword(cntxt, *usr, *new);
}

str CLTcheckPermission(Client cntxt, MalBlkPtr mb, MalStkPtr stk, InstrPtr pci) {
#ifdef HAVE_SHA1_UPDATE
	str *usr = getArgReference_str(stk, pci, 1);
	str *pw = getArgReference_str(stk, pci, 2);
	str ch = "";
	str algo = "SHA1";
	oid id;
	str pwd,msg;

	(void)mb;

	if (!(pwd = mcrypt_SHA1Sum(*pw, strlen(*pw))))
		throw(MAL, "clients.checkPermission", SQLSTATE(HY001) MAL_MALLOC_FAIL);
	msg = AUTHcheckCredentials(&id, cntxt, *usr, pwd, ch, algo);
	free(pwd);
	return msg;
#else
	(void) cntxt;
	(void) mb;
	(void) stk;
	(void) pci;
	throw(MAL, "mal.checkPermission", "Required digest algorithm SHA-1 missing");
#endif
}

str CLTgetUsers(Client cntxt, MalBlkPtr mb, MalStkPtr stk, InstrPtr pci) {
	bat *ret1 = getArgReference_bat(stk, pci, 0);
	bat *ret2 = getArgReference_bat(stk, pci, 1);
	BAT *uid, *nme;
	str tmp;

	(void)mb;

	tmp = AUTHgetUsers(&uid, &nme, cntxt);
	if (tmp)
		return tmp;
	BBPkeepref(*ret1 = uid->batCacheid);
	BBPkeepref(*ret2 = nme->batCacheid);
	return(MAL_SUCCEED);
}

str
CLTshutdown(Client cntxt, MalBlkPtr mb, MalStkPtr stk, InstrPtr pci) {
	str *ret  = getArgReference_str(stk,pci,0);
	int delay;
	bit force = FALSE;
	int leftover;
	char buf[1024]={"safe to stop last connection"};

	if ( pci->argc == 3) 
		force = *getArgReference_bit(stk,pci,2);

	(void) mb;
	switch( getArgType(mb,pci,1)){
	case TYPE_bte:
		delay = *getArgReference_bte(stk,pci,1);
		break;
	case TYPE_sht:
		delay = *getArgReference_sht(stk,pci,1);
		break;
	default:
		delay = *getArgReference_int(stk,pci,1);
		break;
	}

	if (cntxt->user != MAL_ADMIN)
		throw(MAL,"mal.shutdown", "Administrator rights required");
	MCstopClients(cntxt);
	do{
		if ( (leftover = MCactiveClients()-1) )
			MT_sleep_ms(1000);
		delay --;
	} while (delay > 0 && leftover > 1);
	if( delay == 0 && leftover > 1)
		snprintf(buf, 1024,"%d client sessions still running",leftover);
	*ret = GDKstrdup(buf);
	if ( force)
		mal_exit(0);
	if(*ret == NULL)
		throw(MAL, "mal.shutdown", SQLSTATE(HY001) MAL_MALLOC_FAIL);
	return MAL_SUCCEED;
}

str
CLTsessions(Client cntxt, MalBlkPtr mb, MalStkPtr stk, InstrPtr pci)
{
	BAT *user = NULL, *login = NULL, *stimeout = NULL, *qtimeout = NULL, *last= NULL, *active= NULL;
	bat *userId = getArgReference_bat(stk,pci,0);
	bat *loginId = getArgReference_bat(stk,pci,1);
	bat *stimeoutId = getArgReference_bat(stk,pci,2);
	bat *lastId = getArgReference_bat(stk,pci,3);
	bat *qtimeoutId = getArgReference_bat(stk,pci,4);
	bat *activeId = getArgReference_bat(stk,pci,5);
    Client c;
	timestamp ret;
	lng timeout;
	str msg = NULL;

	(void) cntxt;
	(void) mb;

	user = COLnew(0, TYPE_str, 0, TRANSIENT);
	login = COLnew(0, TYPE_timestamp, 0, TRANSIENT);
	stimeout = COLnew(0, TYPE_lng, 0, TRANSIENT);
	last = COLnew(0, TYPE_timestamp, 0, TRANSIENT);
	qtimeout = COLnew(0, TYPE_lng, 0, TRANSIENT);
	active = COLnew(0, TYPE_bit, 0, TRANSIENT);
	if ( user == NULL || login == NULL || stimeout == NULL || last == NULL || qtimeout == NULL || active == NULL){
		if ( user) BBPunfix(user->batCacheid);
		if ( login) BBPunfix(login->batCacheid);
		if ( stimeout) BBPunfix(stimeout->batCacheid);
		if ( qtimeout) BBPunfix(qtimeout->batCacheid);
		if ( last) BBPunfix(last->batCacheid);
		if ( active) BBPunfix(active->batCacheid);
		throw(SQL,"sql.sessions", SQLSTATE(HY001) MAL_MALLOC_FAIL);
	}

    MT_lock_set(&mal_contextLock);
	
    for (c = mal_clients; c < mal_clients + MAL_MAXCLIENTS; c++) {
	    if (c->mode == RUNCLIENT) {
		    if (BUNappend(user, c->username, false) != GDK_SUCCEED)
			    goto bailout;
		    ret = timestamp_fromtime(c->login);
		    if (is_timestamp_nil(ret)) {
			    msg = createException(SQL, "sql.sessions",
						  SQLSTATE(22003) "cannot convert time");
			    goto bailout;
		    }
		    if (BUNappend(login, &ret, false) != GDK_SUCCEED)
			    goto bailout;
		    timeout = c->stimeout / 1000000;
		    if (BUNappend(stimeout, &timeout, false) != GDK_SUCCEED)
			    goto bailout;
		    ret = timestamp_fromtime(c->lastcmd);
		    if (is_timestamp_nil(ret)) {
			    msg = createException(SQL, "sql.sessions",
						  SQLSTATE(22003) "cannot convert time");
			    goto bailout;
		    }
		    if (BUNappend(last, &ret, false) != GDK_SUCCEED)
			    goto bailout;
		    timeout = c->qtimeout / 1000000;
		    if (BUNappend(qtimeout, &timeout, false) != GDK_SUCCEED ||
			BUNappend(active, &c->active, false) != GDK_SUCCEED)
			    goto bailout;
	    }
    }
    MT_lock_unset(&mal_contextLock);
	BBPkeepref(*userId = user->batCacheid);
	BBPkeepref(*loginId = login->batCacheid);
	BBPkeepref(*stimeoutId = stimeout->batCacheid);
	BBPkeepref(*qtimeoutId = qtimeout->batCacheid);
	BBPkeepref(*lastId = last->batCacheid);
	BBPkeepref(*activeId = active->batCacheid);
	return MAL_SUCCEED;

  bailout:
    MT_lock_unset(&mal_contextLock);
	BBPunfix(user->batCacheid);
	BBPunfix(login->batCacheid);
	BBPunfix(stimeout->batCacheid);
	BBPunfix(qtimeout->batCacheid);
	BBPunfix(last->batCacheid);
	BBPunfix(active->batCacheid);
	return msg;
}
