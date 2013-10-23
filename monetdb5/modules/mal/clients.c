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
 * Copyright August 2008-2013 MonetDB B.V.
 * All Rights Reserved.
*/

/*
 * author Martin Kersten, Fabian Groffen
 * Client Management
 * Each online client is represented with an entry in the clients table.
 * The client may inspect his record at run-time and partially change its
 * properties.
 * The administrator sees all client records and has the right to
 * adjust global properties.
 */


#include "monetdb_config.h"
#include "clients.h"
#include "mcrypt.h"
#include "mal_scenario.h"
#include "mal_instruction.h"
#include "mal_client.h"
#include "mal_authorize.h"

#ifdef HAVE_LIBREADLINE
#include <readline/readline.h>
#include <readline/history.h>
#endif

static void
pseudo(int *ret, BAT *b, str X1,str X2) {
	char buf[BUFSIZ];
	snprintf(buf,BUFSIZ,"%s_%s", X1,X2);
	if (BBPindex(buf) <= 0)
		BATname(b,buf);
	BATroles(b,X1,X2);
	BATmode(b,TRANSIENT);
	BATfakeCommit(b);
	*ret = b->batCacheid;
	BBPkeepref(*ret);
}

str
CLTsetListing(Client cntxt, MalBlkPtr mb, MalStkPtr stk, InstrPtr pci)
{
	(void) mb;
	*(int*) getArgReference(stk,pci,0) = cntxt->listing;
	cntxt->listing = *(int*) getArgReference(stk,pci,1);
	return MAL_SUCCEED;
}

str
CLTgetClientId(Client cntxt, MalBlkPtr mb, MalStkPtr stk, InstrPtr pci)
{
	(void) mb;
	assert(cntxt - mal_clients <= INT_MAX);
	*(int*) getArgReference(stk,pci,0) = (int) (cntxt - mal_clients);
	return MAL_SUCCEED;
}

str
CLTgetScenario(Client cntxt, MalBlkPtr mb, MalStkPtr stk, InstrPtr pci)
{
	(void) mb;
	if (cntxt->scenario)
		*(str *) getArgReference(stk,pci,0) = GDKstrdup(cntxt->scenario);
	else
		*(str *) getArgReference(stk,pci,0) = GDKstrdup("nil");
	return MAL_SUCCEED;
}

str
CLTsetScenario(Client cntxt, MalBlkPtr mb, MalStkPtr stk, InstrPtr pci)
{
	str msg = MAL_SUCCEED;

	(void) mb;
	msg = setScenario(cntxt, *(str *) getArgReference(stk,pci,1));
	*(str *) getArgReference(stk,pci,0) = 0;
	if (msg == NULL)
		*(str *) getArgReference(stk,pci,0) = GDKstrdup(cntxt->scenario);
	return msg;
}

static char *
local_itoa(int i)
{
	static char buf[32];

	sprintf(buf, "%d", i);
	return buf;
}

static void
CLTtimeConvert(time_t l, char *s){
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
			strncpy(s, asctime(&localt), 26);
#endif
#endif
			s[24] = 0;
}

str
CLTInfo(Client cntxt, MalBlkPtr mb, MalStkPtr stk, InstrPtr pci)
{
	int *ret=  (int *) getArgReference(stk,pci,0);
	int *ret2=  (int *) getArgReference(stk,pci,0);
	BAT *b = BATnew(TYPE_void, TYPE_str, 12);
	BAT *bn = BATnew(TYPE_void, TYPE_str, 12);
	char s[26];

	(void) mb;
	if (b == 0 || bn == 0){
		if ( b != 0) BBPreleaseref(b->batCacheid);
		if ( bn != 0) BBPreleaseref(bn->batCacheid);
		throw(MAL, "clients.info", MAL_MALLOC_FAIL);
	}

	BUNappend(b, "user", FALSE);
	BUNappend(bn, local_itoa((int)cntxt->user), FALSE);

	BUNappend(b, "password", FALSE); /* FIXME: get rid of this */
	BUNappend(bn, "", FALSE); /* FIXME: get rid of this */

	BUNappend(b, "scenario", FALSE);
	BUNappend(bn, cntxt->scenario, FALSE);

	BUNappend(b, "timer", FALSE);
	BUNappend(bn, local_itoa((int) cntxt->timer), FALSE);

	BUNappend(b, "trace", FALSE);
	BUNappend(bn, local_itoa(cntxt->itrace), FALSE);

	BUNappend(b, "listing", FALSE);
	BUNappend(bn, local_itoa(cntxt->listing), FALSE);

	BUNappend(b, "debug", FALSE);
	BUNappend(bn, local_itoa(cntxt->debug), FALSE);

	CLTtimeConvert((time_t) cntxt->login,s);
	BUNappend(b, "login", FALSE);
	BUNappend(bn, s, FALSE);
	if (!(b->batDirty&2)) b = BATsetaccess(b, BAT_READ);
	pseudo(ret,b,"client","info");
	BBPkeepref(*ret2= bn->batCacheid);
	return MAL_SUCCEED;
}

str
CLTLogin(int *ret)
{
	BAT *b = BATnew(TYPE_void, TYPE_str, 12);
	int i;
	char s[26];

	if (b == 0)
		throw(MAL, "clients.getLogins", MAL_MALLOC_FAIL);
	BATseqbase(b,0);

	for (i = 0; i < MAL_MAXCLIENTS; i++) {
		Client c = mal_clients+i;
		if (c->mode >= RUNCLIENT && c->user != oid_nil) {
			CLTtimeConvert((time_t) c->login,s);
			BUNappend(b, s, FALSE);
		}
	}
	if (!(b->batDirty&2)) b = BATsetaccess(b, BAT_READ);
	pseudo(ret,b,"client","login");
	return MAL_SUCCEED;
}

str
CLTLastCommand(int *ret)
{
	BAT *b = BATnew(TYPE_void, TYPE_str, 12);
	int i;
	char s[26];

	if (b == 0)
		throw(MAL, "clients.getLastCommand", MAL_MALLOC_FAIL);
	BATseqbase(b,0);
	for (i = 0; i < MAL_MAXCLIENTS; i++) {
		Client c = mal_clients+i;
		if (c->mode >= RUNCLIENT && c->user != oid_nil) {
			CLTtimeConvert((time_t) c->lastcmd,s);
			BUNappend(b, s, FALSE);
		}
	}
	if (!(b->batDirty&2)) b = BATsetaccess(b, BAT_READ);
	pseudo(ret,b,"client","lastcommand");
	return MAL_SUCCEED;
}

str
CLTActions(int *ret)
{
	BAT *b = BATnew(TYPE_void, TYPE_int, 12);
	int i;

	if (b == 0)
		throw(MAL, "clients.getActions", MAL_MALLOC_FAIL);
	BATseqbase(b,0);
	for (i = 0; i < MAL_MAXCLIENTS; i++) {
		Client c = mal_clients+i;
		if (c->mode >= RUNCLIENT && c->user != oid_nil) {
			BUNappend(b, &c->actions, FALSE);
		}
	}
	if (!(b->batDirty&2)) b = BATsetaccess(b, BAT_READ);
	pseudo(ret,b,"client","actions");
	return MAL_SUCCEED;
}
str
CLTTime(int *ret)
{
	BAT *b = BATnew(TYPE_void, TYPE_lng, 12);
	int i;

	if (b == 0)
		throw(MAL, "clients.getTime", MAL_MALLOC_FAIL);
	BATseqbase(b,0);
	for (i = 0; i < MAL_MAXCLIENTS; i++) {
		Client c = mal_clients+i;
		if (c->mode >= RUNCLIENT && c->user != oid_nil) {
			BUNappend(b, &c->totaltime, FALSE);
		}
	}
	if (!(b->batDirty&2)) b = BATsetaccess(b, BAT_READ);
	pseudo(ret,b,"client","usec");
	return MAL_SUCCEED;
}

/*
 * Produce a list of clients currently logged in
 */
str
CLTusers(int *ret)
{
	BAT *b = BATnew(TYPE_void, TYPE_str, 12);
	int i;

	if (b == 0)
		throw(MAL, "clients.users", MAL_MALLOC_FAIL);
	BATseqbase(b,0);
	for (i = 0; i < MAL_MAXCLIENTS; i++) {
		Client c = mal_clients+i;
		if (c->mode >= RUNCLIENT && c->user != oid_nil)
			BUNappend(b, &i, FALSE);
	}
	if (!(b->batDirty&2)) b = BATsetaccess(b, BAT_READ);
	pseudo(ret,b,"client","users");
	return MAL_SUCCEED;
}

str
CLTsetHistory(Client cntxt, MalBlkPtr mb, MalStkPtr stk, InstrPtr pci)
{
	str* fname = (str *) getArgReference(stk,pci,1);
	(void) mb;

	if( cntxt->history){
#ifdef HAVE_LIBREADLINE
		write_history(cntxt->history);
#endif
		GDKfree(cntxt->history);
	}
	if( *fname == str_nil)
		cntxt->history = NULL;
	else {
		cntxt->history = GDKstrdup(*fname);
#ifdef HAVE_LIBREADLINE
		read_history(cntxt->history);
#endif
	}
	return MAL_SUCCEED;
}

str
CLTquit(Client cntxt, MalBlkPtr mb, MalStkPtr stk, InstrPtr pci)
{
	int id;
	(void) mb;		/* fool compiler */
	
	if ( pci->argc==2 && cntxt->idx != 0)
		throw(MAL, "client.quit", INVCRED_ACCESS_DENIED);
	if ( pci->argc==2)
		id = *(int*) getArgReference(stk,pci,1);
	else id =cntxt->idx;

	if (id == 0 && cntxt->fdout != GDKout )
		throw(MAL, "client.quit", INVCRED_ACCESS_DENIED);
	if ( cntxt->idx == mal_clients[id].idx)
		mal_clients[id].mode = FINISHCLIENT;
	/* the console should be finished with an exception */
	if (id == 0)
		throw(MAL,"client.quit",SERVER_STOPPED);
	return MAL_SUCCEED;
}

str
CLTstop(Client cntxt, MalBlkPtr mb, MalStkPtr stk, InstrPtr pci)
{
	int id=  *(int *) getArgReference(stk,pci,1);
	(void) mb;
	if ( cntxt->user == mal_clients[id].user || 
		mal_clients[0].user == cntxt->user){
		mal_clients[id].itrace = 'x';
	}
	/* this forces the designated client to stop at the next instruction */
    return MAL_SUCCEED;
}

str
CLTsuspend(Client cntxt, MalBlkPtr mb, MalStkPtr stk, InstrPtr pci)
{
	int *id=  (int *) getArgReference(stk,pci,1);
	(void) cntxt;
	(void) mb;
    return MCsuspendClient(*id);
}

str
CLTsetTimeout(Client cntxt, MalBlkPtr mb, MalStkPtr stk, InstrPtr pci)
{
	int qto=  *(int *) getArgReference(stk,pci,1);
	int sto=  *(int *) getArgReference(stk,pci,2);
	(void) mb;
	cntxt->qtimeout = qto;
	cntxt->stimeout = sto;
    return MAL_SUCCEED;
}
str
CLTgetTimeout(Client cntxt, MalBlkPtr mb, MalStkPtr stk, InstrPtr pci)
{
	int *qto=  (int *) getArgReference(stk,pci,0);
	int *sto=  (int *) getArgReference(stk,pci,1);
	(void) mb;
	*qto = cntxt->qtimeout;
	*sto = cntxt->stimeout;
    return MAL_SUCCEED;
}

str
CLTwakeup(int *ret, int *id)
{
    (void) ret;     /* fool compiler */
    return MCawakeClient(*id);
}

str CLTmd5sum(str *ret, str *pw) {
	char *mret = mcrypt_MD5Sum(*pw, strlen(*pw));
	*ret = GDKstrdup(mret);
	free(mret);
	return MAL_SUCCEED;
}

str CLTsha1sum(str *ret, str *pw) {
	char *mret = mcrypt_SHA1Sum(*pw, strlen(*pw));
	*ret = GDKstrdup(mret);
	free(mret);
	return MAL_SUCCEED;
}

str CLTripemd160sum(str *ret, str *pw) {
	char *mret = mcrypt_RIPEMD160Sum(*pw, strlen(*pw));
	*ret = GDKstrdup(mret);
	free(mret);
	return MAL_SUCCEED;
}

str CLTsha2sum(str *ret, str *pw, int *bits) {
	char *mret;
	switch (*bits) {
		case 512:
			mret = mcrypt_SHA512Sum(*pw, strlen(*pw));
			break;
		case 384:
			mret = mcrypt_SHA384Sum(*pw, strlen(*pw));
			break;
		case 256:
			mret = mcrypt_SHA256Sum(*pw, strlen(*pw));
			break;
		case 224:
			mret = mcrypt_SHA224Sum(*pw, strlen(*pw));
			break;
		default:
			throw(ILLARG, "clients.sha2sum", "wrong number of bits "
					"for SHA2 sum: %d", *bits);
	}
	*ret = GDKstrdup(mret);
	free(mret);
	return MAL_SUCCEED;
}

str CLTbackendsum(str *ret, str *pw) {
	char *mret = mcrypt_BackendSum(*pw, strlen(*pw));
	*ret = GDKstrdup(mret);
	free(mret);
	return MAL_SUCCEED;
}

str CLTaddUser(Client cntxt, MalBlkPtr mb, MalStkPtr stk, InstrPtr pci) {
	oid *ret = (oid *)getArgReference(stk, pci, 0);
	str *usr = (str *)getArgReference(stk, pci, 1);
	str *pw = (str *)getArgReference(stk, pci, 2);

	(void)mb;
	
	return AUTHaddUser(ret, &cntxt, usr, pw);
}

str CLTremoveUser(Client cntxt, MalBlkPtr mb, MalStkPtr stk, InstrPtr pci) {
	str *usr;
	(void)mb;

	usr = (str *)getArgReference(stk, pci, 1);

	return AUTHremoveUser(&cntxt, usr);
}

str CLTgetUsername(Client cntxt, MalBlkPtr mb, MalStkPtr stk, InstrPtr pci) {
	str *ret = (str *)getArgReference(stk, pci, 0);
	(void)mb;

	return AUTHgetUsername(ret, &cntxt);
}

str CLTgetPasswordHash(Client cntxt, MalBlkPtr mb, MalStkPtr stk, InstrPtr pci) {
	str *ret = (str *)getArgReference(stk, pci, 0);
	str *user = (str *)getArgReference(stk, pci, 1);

	(void)mb;

	return AUTHgetPasswordHash(ret, &cntxt, user);
}

str CLTchangeUsername(Client cntxt, MalBlkPtr mb, MalStkPtr stk, InstrPtr pci) {
	str *old = (str *)getArgReference(stk, pci, 1);
	str *new = (str *)getArgReference(stk, pci, 2);

	(void)mb;

	return AUTHchangeUsername(&cntxt, old, new);
}

str CLTchangePassword(Client cntxt, MalBlkPtr mb, MalStkPtr stk, InstrPtr pci) {
	str *old = (str *)getArgReference(stk, pci, 1);
	str *new = (str *)getArgReference(stk, pci, 2);

	(void)mb;

	return AUTHchangePassword(&cntxt, old, new);
}

str CLTsetPassword(Client cntxt, MalBlkPtr mb, MalStkPtr stk, InstrPtr pci) {
	str *usr = (str *)getArgReference(stk, pci, 1);
	str *new = (str *)getArgReference(stk, pci, 2);

	(void)mb;

	return AUTHsetPassword(&cntxt, usr, new);
}

str CLTcheckPermission(Client cntxt, MalBlkPtr mb, MalStkPtr stk, InstrPtr pci) {
	str *usr = (str *)getArgReference(stk, pci, 1);
	str *pw = (str *)getArgReference(stk, pci, 2);
	str ch = "";
	str algo = "SHA1";
	oid id;
	str pwd,msg;

	(void)mb;

	pwd = mcrypt_SHA1Sum(*pw, strlen(*pw));
	msg = AUTHcheckCredentials(&id, &cntxt, usr, &pwd, &ch, &algo);
	free(pwd);
	return msg;
}

str CLTgetUsers(Client cntxt, MalBlkPtr mb, MalStkPtr stk, InstrPtr pci) {
	bat *ret = (bat *)getArgReference(stk, pci, 0);
	BAT *r = NULL;
	str tmp;

	(void)mb;

	tmp = AUTHgetUsers(&r, &cntxt);
	if (tmp)
		return tmp;
	BBPkeepref(*ret = r->batCacheid);
	return(MAL_SUCCEED);
}

str CLTshutdown(int *ret, bit *forced) {
	(void) ret;
	(void) forced;
	throw(MAL,"clients.shutdown", PROGRAM_NYI);
}
