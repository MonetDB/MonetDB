/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0.  If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 *
 * Copyright 1997 - July 2008 CWI, August 2008 - 2021 MonetDB B.V.
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
#include "mal_runtime.h"
#include "mal_client.h"
#include "mal_authorize.h"
#include "mal_internal.h"
#include "opt_pipes.h"
#include "gdk_time.h"

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

static str
CLTsetListing(Client cntxt, MalBlkPtr mb, MalStkPtr stk, InstrPtr pci)
{
	(void) mb;
	*getArgReference_int(stk,pci,0) = cntxt->listing;
	cntxt->listing = *getArgReference_int(stk,pci,1);
	return MAL_SUCCEED;
}

static str
CLTgetClientId(Client cntxt, MalBlkPtr mb, MalStkPtr stk, InstrPtr pci)
{
	(void) mb;
	if(cntxt - mal_clients < 0 || cntxt - mal_clients >= MAL_MAXCLIENTS)
		throw(MAL, "clients.getClientId", "Illegal client index");
	*getArgReference_int(stk,pci,0) = (int) (cntxt - mal_clients);
	return MAL_SUCCEED;
}

static str
CLTgetScenario(Client cntxt, MalBlkPtr mb, MalStkPtr stk, InstrPtr pci)
{
	(void) mb;
	if (cntxt->scenario)
		*getArgReference_str(stk,pci,0) = GDKstrdup(cntxt->scenario);
	else
		*getArgReference_str(stk,pci,0) = GDKstrdup("nil");
	if(*getArgReference_str(stk,pci,0) == NULL)
		throw(MAL, "clients.getScenario", SQLSTATE(HY013) MAL_MALLOC_FAIL);
	return MAL_SUCCEED;
}

static str
CLTsetScenario(Client cntxt, MalBlkPtr mb, MalStkPtr stk, InstrPtr pci)
{
	str msg = MAL_SUCCEED;

	(void) mb;
	msg = setScenario(cntxt, *getArgReference_str(stk,pci,1));
	*getArgReference_str(stk,pci,0) = 0;
	if (msg == NULL) {
		*getArgReference_str(stk,pci,0) = GDKstrdup(cntxt->scenario);
		if(*getArgReference_str(stk,pci,0) == NULL)
			throw(MAL, "clients.setScenario", SQLSTATE(HY013) MAL_MALLOC_FAIL);
	}
	return msg;
}

static void
CLTtimeConvert(time_t l, char *s)
{
	struct tm localt = (struct tm) {0};

	(void) localtime_r(&l, &localt);

#ifdef HAVE_ASCTIME_R3
	asctime_r(&localt, s, 26);
#else
	asctime_r(&localt, s);
#endif
	s[24] = 0;		/* remove newline */
}

static str
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
		throw(MAL, "clients.info", SQLSTATE(HY013) MAL_MALLOC_FAIL);
	}

	(void) snprintf(buf, sizeof(buf), ""LLFMT"", (lng) cntxt->user);
	if (BUNappend(b, "user", false) != GDK_SUCCEED ||
		BUNappend(bn, buf, false) != GDK_SUCCEED)
		goto bailout;

	if (BUNappend(b, "scenario", false) != GDK_SUCCEED ||
		BUNappend(bn, cntxt->scenario, false) != GDK_SUCCEED)
		goto bailout;

	(void) snprintf(buf, sizeof(buf), "%d", cntxt->listing);
	if (BUNappend(b, "listing", false) != GDK_SUCCEED ||
		BUNappend(bn, buf, false) != GDK_SUCCEED)
		goto bailout;

	(void) snprintf(buf, sizeof(buf), "%d", cntxt->debug);
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
	throw(MAL, "clients.info", SQLSTATE(HY013) MAL_MALLOC_FAIL);
}

static str
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
	throw(MAL, "clients.getLogins", SQLSTATE(HY013) MAL_MALLOC_FAIL);
}

static str
CLTquit(Client cntxt, MalBlkPtr mb, MalStkPtr stk, InstrPtr pci)
{
	str msg = MAL_SUCCEED;
	int idx = cntxt->idx;
	(void) mb;		/* fool compiler */

	if ( pci->argc == 2 && cntxt->user == MAL_ADMIN)
		idx = *getArgReference_int(stk,pci,1);

	if ( idx < 0 || idx > MAL_MAXCLIENTS)
		throw(MAL,"clients.quit", "Illegal session id");

	/* A user can only quite a session under the same id */
	MT_lock_set(&mal_contextLock);
	if (mal_clients[idx].mode == FREECLIENT)
		msg = createException(MAL,"clients.stop","Session not active anymore");
	else
		mal_clients[idx].mode = FINISHCLIENT;
	MT_lock_unset(&mal_contextLock);
	return msg;
}

/* Stopping a client in a softmanner by setting the time out marker */
static str
CLTstop(Client cntxt, MalBlkPtr mb, MalStkPtr stk, InstrPtr pci)
{
	int idx = cntxt->idx;
	str msg = MAL_SUCCEED;

	(void) mb;
	if (cntxt->user == MAL_ADMIN)
		idx = *getArgReference_int(stk,pci,1);

	if ( idx < 0 || idx > MAL_MAXCLIENTS)
		throw(MAL,"clients.stop","Illegal session id");

	MT_lock_set(&mal_contextLock);
	if (mal_clients[idx].mode == FREECLIENT)
		msg = createException(MAL,"clients.stop","Session not active anymore");
	else
		mal_clients[idx].querytimeout = 1; /* stop client in one microsecond */
	/* this forces the designated client to stop at the next instruction */
	MT_lock_unset(&mal_contextLock);
	return msg;
}

static str
CLTsetoptimizer(Client cntxt, MalBlkPtr mb, MalStkPtr stk, InstrPtr pci)
{
	int idx = cntxt->idx;
	str opt, msg = MAL_SUCCEED;

	(void) mb;
	if( pci->argc == 3 && cntxt->user == MAL_ADMIN){
		idx = *getArgReference_int(stk,pci,1);
		opt = *getArgReference_str(stk,pci,2);
	} else {
		opt = *getArgReference_str(stk,pci,1);
	}

	if (idx < 0 || idx > MAL_MAXCLIENTS)
		throw(MAL,"clients.setoptimizer","Illegal session id");
	if (strNil(opt))
		throw(MAL,"clients.setoptimizer","Input string cannot be NULL");
	if (strlen(opt) >= sizeof(mal_clients[idx].optimizer))
		throw(MAL,"clients.setoptimizer","Input string is too large");
	if (!isOptimizerPipe(opt))
		throw(MAL, "clients.setoptimizer", "Valid optimizer pipe expected");

	MT_lock_set(&mal_contextLock);
	if (mal_clients[idx].mode == FREECLIENT)
		msg = createException(MAL,"clients.setoptimizer","Session not active anymore");
	else
		strcpy_len(mal_clients[idx].optimizer, opt, sizeof(mal_clients[idx].optimizer));
	MT_lock_unset(&mal_contextLock);
	return msg;
}

static str
CLTsetworkerlimit(Client cntxt, MalBlkPtr mb, MalStkPtr stk, InstrPtr pci)
{
	str msg = MAL_SUCCEED;
	int idx = cntxt->idx, limit;

	(void) mb;
	if (pci->argc == 3 && cntxt->user == MAL_ADMIN){
		idx = *getArgReference_int(stk,pci,1);
		limit = *getArgReference_int(stk,pci,2);
	} else {
		limit = *getArgReference_int(stk,pci,1);
	}

	if( idx < 0 || idx > MAL_MAXCLIENTS)
		throw(MAL,"clients.setworkerlimit","Illegal session id");
	if( is_int_nil(limit))
		throw(MAL, "clients.setworkerlimit","The number of workers cannot be NULL");
	if( limit < 0)
		throw(MAL, "clients.setworkerlimit","The number of workers cannot be negative");

	MT_lock_set(&mal_contextLock);
	if (mal_clients[idx].mode == FREECLIENT)
		msg = createException(MAL,"clients.setworkerlimit","Session not active anymore");
	else
		mal_clients[idx].workerlimit = limit;
	MT_lock_unset(&mal_contextLock);
	return msg;
}

static str
CLTsetmemorylimit(Client cntxt, MalBlkPtr mb, MalStkPtr stk, InstrPtr pci)
{
	str msg = MAL_SUCCEED;
	int idx = cntxt->idx, limit;

	(void) mb;
	if (pci->argc == 3 && cntxt->user == MAL_ADMIN){
		idx = *getArgReference_sht(stk,pci,1);
		limit = *getArgReference_int(stk,pci,2);
	} else {
		limit = *getArgReference_int(stk,pci,1);
	}

	if( idx < 0 || idx > MAL_MAXCLIENTS)
		throw(MAL,"clients.setmemorylimit","Illegal session id");
	if( is_int_nil(limit))
		throw(MAL, "clients.setmemorylimit", "The memmory limit cannot be NULL");
	if( limit < 0)
		throw(MAL, "clients.setmemorylimit", "The memmory limit cannot be negative");

	MT_lock_set(&mal_contextLock);
	if (mal_clients[idx].mode == FREECLIENT)
		msg = createException(MAL,"clients.setmemorylimit","Session not active anymore");
	else
		mal_clients[idx].memorylimit = limit;
	MT_lock_unset(&mal_contextLock);
	return msg;
}

static str
CLTstopSession(Client cntxt, MalBlkPtr mb, MalStkPtr stk, InstrPtr pci)
{
	str msg = MAL_SUCCEED;
	int idx = cntxt->idx;

	if (cntxt->user == MAL_ADMIN) {
		switch( getArgType(mb,pci,1)){
		case TYPE_bte:
			idx = *getArgReference_bte(stk,pci,1);
			break;
		case TYPE_sht:
			idx = *getArgReference_sht(stk,pci,1);
			break;
		case TYPE_int:
			idx = *getArgReference_int(stk,pci,1);
			break;
		default:
			throw(MAL,"clients.stopSession","Unexpected index type");
		}
	}
	if( idx < 0 || idx > MAL_MAXCLIENTS)
		throw(MAL,"clients.stopSession","Illegal session id");

	MT_lock_set(&mal_contextLock);
	if (mal_clients[idx].mode == FREECLIENT) {
		msg = createException(MAL,"clients.stopSession","Session not active anymore");
	} else {
		mal_clients[idx].querytimeout = 1; /* stop client in one microsecond */
		mal_clients[idx].sessiontimeout = 1; /* stop client session */
	}
	MT_lock_unset(&mal_contextLock);
	/* this forces the designated client to stop at the next instruction */
	return msg;
}

/* Queries can be temporarily suspended */
static str
CLTsuspend(Client cntxt, MalBlkPtr mb, MalStkPtr stk, InstrPtr pci)
{
	str msg = MAL_SUCCEED;
	int idx = cntxt->idx;

	if (cntxt->user == MAL_ADMIN)
		idx = *getArgReference_int(stk,pci,1);
	(void) mb;

	if( idx < 0 || idx > MAL_MAXCLIENTS)
		throw(MAL,"clients.suspend", "Illegal session id");

	MT_lock_set(&mal_contextLock);
	if (mal_clients[idx].mode == FREECLIENT)
		msg = createException(MAL,"clients.suspend","Session not active anymore");
	else
		msg = MCsuspendClient(idx);
	MT_lock_unset(&mal_contextLock);
	return msg;
}

static str
CLTwakeup(Client cntxt, MalBlkPtr mb, MalStkPtr stk, InstrPtr pci)
{
	str msg = MAL_SUCCEED;
	int idx = cntxt->idx;

	if (cntxt->user == MAL_ADMIN)
		idx = *getArgReference_int(stk,pci,1);
	(void) mb;

	if( idx < 0 || idx > MAL_MAXCLIENTS)
		throw(MAL,"clients.wakeup", "Illegal session id");

	MT_lock_set(&mal_contextLock);
	if (mal_clients[idx].mode == FREECLIENT)
		msg = createException(MAL,"clients.wakeup","Session not active anymore");
	else
		msg = MCawakeClient(idx);
	MT_lock_unset(&mal_contextLock);
	return msg;
}

/* Set session time out based in seconds. As of December 2019, this function is deprecated */
static str
CLTsetSessionTimeout(Client cntxt, MalBlkPtr mb, MalStkPtr stk, InstrPtr pci)
{
	str msg = MAL_SUCCEED;
	lng sto;
	int idx = cntxt->idx;

	(void) mb;
	if( idx < 0 || idx > MAL_MAXCLIENTS)
		throw(MAL,"clients.setsession","Illegal session id %d", idx);
	sto = *getArgReference_lng(stk,pci,1);
	if (is_lng_nil(sto))
		throw(MAL,"clients.setsession","Session timeout cannot be NULL");
	if (sto < 0)
		throw(MAL,"clients.setsession","Session timeout should be >= 0");

	MT_lock_set(&mal_contextLock);
	if (mal_clients[idx].mode == FREECLIENT)
		msg = createException(MAL,"clients.setsession","Session not active anymore");
	else
		mal_clients[idx].sessiontimeout = sto * 1000000;
	MT_lock_unset(&mal_contextLock);
	return msg;
}

/* Set the current query timeout in seconds. As of December 2019, this function is deprecated */
static str
CLTsetTimeout(Client cntxt, MalBlkPtr mb, MalStkPtr stk, InstrPtr pci)
{
	str msg = MAL_SUCCEED;
	lng qto,sto = 0;
	int idx = cntxt->idx;

	(void) mb;
	if( idx < 0 || idx > MAL_MAXCLIENTS)
		throw(MAL,"clients.settimeout","Illegal session id %d", idx);
	qto = *getArgReference_lng(stk,pci,1);
	if (is_lng_nil(qto))
		throw(MAL,"clients.settimeout","Query timeout cannot be NULL");
	if (qto < 0)
		throw(MAL,"clients.settimeout","Query timeout should be >= 0");
	if (pci->argc == 3) {
		sto = *getArgReference_lng(stk,pci,2);
		if (is_lng_nil(sto))
			throw(MAL,"clients.settimeout","Session timeout cannot be NULL");
		if( sto < 0)
			throw(MAL,"clients.settimeout","Session timeout should be >= 0");
	}

	MT_lock_set(&mal_contextLock);
	if (mal_clients[idx].mode == FREECLIENT) {
		msg = createException(MAL, "clients.settimeout","Session not active anymore");
	} else {
		if (pci->argc == 3)
			mal_clients[idx].sessiontimeout = sto * 1000000;
		mal_clients[idx].querytimeout = qto * 1000000;
	}
	MT_lock_unset(&mal_contextLock);
	return msg;
}

/* set session time out based in seconds, converted into microseconds */
static str
CLTqueryTimeout(Client cntxt, MalBlkPtr mb, MalStkPtr stk, InstrPtr pci)
{
	str msg = MAL_SUCCEED;
	int qto, idx = cntxt->idx;

	if (cntxt->user == MAL_ADMIN && pci->argc == 3){
		switch( getArgType(mb,pci,1)){
		case TYPE_bte:
			idx = *getArgReference_bte(stk,pci,1);
			break;
		case TYPE_sht:
			idx = *getArgReference_sht(stk,pci,1);
			break;
		case TYPE_int:
			idx = *getArgReference_int(stk,pci,1);
			break;
		default:
			throw(MAL,"clients.queryTimeout","Unexpected index type");
		}
		qto = *getArgReference_int(stk,pci,2);
	} else {
		qto = *getArgReference_int(stk,pci,1);
	}
	if (is_int_nil(qto))
		throw(MAL,"clients.queryTimeout","Query timeout cannot be NULL");
	if( qto < 0)
		throw(MAL,"clients.queryTimeout","Query timeout should be >= 0");

	MT_lock_set(&mal_contextLock);
	if (mal_clients[idx].mode == FREECLIENT)
		msg = createException(MAL,"clients.queryTimeout","Session not active anymore");
	else
		mal_clients[idx].querytimeout = (lng) qto * 1000000;
	MT_lock_unset(&mal_contextLock);
	return msg;
}

/* Set the current session timeout in seconds */
static str
CLTsessionTimeout(Client cntxt, MalBlkPtr mb, MalStkPtr stk, InstrPtr pci)
{
	str msg = MAL_SUCCEED;
	int sto = -1, idx = cntxt->idx;

	if(cntxt->user == MAL_ADMIN && pci->argc == 3){
		switch( getArgType(mb,pci,1)){
		case TYPE_bte:
			idx = *getArgReference_bte(stk,pci,1);
			break;
		case TYPE_sht:
			idx = *getArgReference_sht(stk,pci,1);
			break;
		case TYPE_int:
			idx = *getArgReference_int(stk,pci,1);
			break;
		default:
			throw(MAL,"clients.sessionTimeout","Unexpected index type");
		}
		sto = *getArgReference_int(stk,pci,2);
	} else {
		sto = *getArgReference_int(stk,pci,1);
	}
	if (is_int_nil(sto))
		throw(MAL,"clients.sessionTimeout","Session timeout cannot be NULL");
	if( sto < 0)
		throw(MAL,"clients.sessionTimeout","Session timeout should be >= 0");
	if( idx < 0 || idx > MAL_MAXCLIENTS)
		throw(MAL,"clients.sessionTimeout","Illegal session id %d", idx);

	MT_lock_set(&mal_contextLock);
	if (mal_clients[idx].mode == FREECLIENT)
		msg = createException(MAL,"clients.sessionTimeout","Session not active anymore");
	else
		mal_clients[idx].sessiontimeout = (lng) sto * 1000000;
	MT_lock_unset(&mal_contextLock);
	return msg;
}

/* Retrieve the session time out */
static str
CLTgetProfile(Client cntxt, MalBlkPtr mb, MalStkPtr stk, InstrPtr pci)
{
	str *opt=  getArgReference_str(stk,pci,0);
	int *qto=  getArgReference_int(stk,pci,1);
	int *sto=  getArgReference_int(stk,pci,2);
	int *wlim=  getArgReference_int(stk,pci,3);
	int *mlim=  getArgReference_int(stk,pci,4);
	(void) mb;
	if (!(*opt = GDKstrdup(cntxt->optimizer)))
		throw(MAL, "clients.getProfile", SQLSTATE(HY013) MAL_MALLOC_FAIL);
	*qto = (int)(cntxt->querytimeout / 1000000);
	*sto = (int)(cntxt->sessiontimeout / 1000000);
	*wlim = cntxt->workerlimit;
	*mlim = cntxt->memorylimit;
	return MAL_SUCCEED;
}

/* Long running queries are traced in the logger
 * with a message from the interpreter.
 * This value should be set to minutes to avoid a lengthly log */
static str
CLTsetPrintTimeout(void *ret, int *secs)
{
	(void) ret;
	if (is_int_nil(*secs))
		setqptimeout(0);
	else
		setqptimeout((lng) *secs * 60 * 1000000);
	return MAL_SUCCEED;
}

static str CLTmd5sum(str *ret, str *pw) {
#ifdef HAVE_MD5_UPDATE
	if (strNil(*pw)) {
		*ret = GDKstrdup(str_nil);
	} else {
		char *mret = mcrypt_MD5Sum(*pw, strlen(*pw));

		if (!mret)
			throw(MAL, "clients.md5sum", SQLSTATE(HY013) MAL_MALLOC_FAIL);
		*ret = GDKstrdup(mret);
		free(mret);
	}
	if (*ret == NULL)
		throw(MAL, "clients.md5sum", SQLSTATE(HY013) MAL_MALLOC_FAIL);
	return MAL_SUCCEED;
#else
	(void) ret;
	(void) pw;
	throw(MAL, "clients.md5sum", SQLSTATE(0A000) PROGRAM_NYI);
#endif
}

static str CLTsha1sum(str *ret, str *pw) {
#ifdef HAVE_SHA1_UPDATE
	if (strNil(*pw)) {
		*ret = GDKstrdup(str_nil);
	} else {
		char *mret = mcrypt_SHA1Sum(*pw, strlen(*pw));

		if (!mret)
			throw(MAL, "clients.sha1sum", SQLSTATE(HY013) MAL_MALLOC_FAIL);
		*ret = GDKstrdup(mret);
		free(mret);
	}
	if (*ret == NULL)
		throw(MAL, "clients.sha1sum", SQLSTATE(HY013) MAL_MALLOC_FAIL);
	return MAL_SUCCEED;
#else
	(void) ret;
	(void) pw;
	throw(MAL, "clients.sha1sum", SQLSTATE(0A000) PROGRAM_NYI);
#endif
}

static str CLTripemd160sum(str *ret, str *pw) {
#ifdef HAVE_RIPEMD160_UPDATE
	if (strNil(*pw)) {
		*ret = GDKstrdup(str_nil);
	} else {
		char *mret = mcrypt_RIPEMD160Sum(*pw, strlen(*pw));

		if (!mret)
			throw(MAL, "clients.ripemd160sum", SQLSTATE(HY013) MAL_MALLOC_FAIL);
		*ret = GDKstrdup(mret);
		free(mret);
	}
	if (*ret == NULL)
		throw(MAL, "clients.ripemd160sum", SQLSTATE(HY013) MAL_MALLOC_FAIL);
	return MAL_SUCCEED;
#else
	(void) ret;
	(void) pw;
	throw(MAL, "clients.ripemd160sum", SQLSTATE(0A000) PROGRAM_NYI);
#endif
}

static str CLTsha2sum(str *ret, str *pw, int *bits) {
	if (strNil(*pw) || is_int_nil(*bits)) {
		*ret = GDKstrdup(str_nil);
	} else {
		char *mret = 0;
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
				(void)mret;
				throw(ILLARG, "clients.sha2sum", "wrong number of bits "
						"for SHA2 sum: %d", *bits);
		}
#if defined(HAVE_SHA512_UPDATE) || defined(HAVE_SHA384_UPDATE) || defined(HAVE_SHA256_UPDATE) || defined(HAVE_SHA224_UPDATE)
		if (!mret)
			throw(MAL, "clients.sha2sum", SQLSTATE(HY013) MAL_MALLOC_FAIL);
		*ret = GDKstrdup(mret);
		free(mret);
#endif
	}
	if (*ret == NULL)
		throw(MAL, "clients.sha2sum", SQLSTATE(HY013) MAL_MALLOC_FAIL);
	return MAL_SUCCEED;
}

static str CLTbackendsum(str *ret, str *pw) {
	if (strNil(*pw)) {
		*ret = GDKstrdup(str_nil);
	} else {
		char *mret = mcrypt_BackendSum(*pw, strlen(*pw));
		if (mret == NULL)
			throw(MAL, "clients.backendsum", SQLSTATE(HY013) MAL_MALLOC_FAIL);
		*ret = GDKstrdup(mret);
		free(mret);
	}
	if (*ret == NULL)
		throw(MAL, "clients.backendsum", SQLSTATE(HY013) MAL_MALLOC_FAIL);
	return MAL_SUCCEED;
}

static str CLTaddUser(Client cntxt, MalBlkPtr mb, MalStkPtr stk, InstrPtr pci) {
	oid *ret = getArgReference_oid(stk, pci, 0);
	str *usr = getArgReference_str(stk, pci, 1);
	str *pw = getArgReference_str(stk, pci, 2);

	(void)mb;

	return AUTHaddUser(ret, cntxt, *usr, *pw);
}

static str CLTremoveUser(Client cntxt, MalBlkPtr mb, MalStkPtr stk, InstrPtr pci) {
	str *usr;
	(void)mb;

	usr = getArgReference_str(stk, pci, 1);

	return AUTHremoveUser(cntxt, *usr);
}

static str CLTgetUsername(Client cntxt, MalBlkPtr mb, MalStkPtr stk, InstrPtr pci) {
	str *ret = getArgReference_str(stk, pci, 0);
	(void)mb;

	return AUTHgetUsername(ret, cntxt);
}

static str CLTgetPasswordHash(Client cntxt, MalBlkPtr mb, MalStkPtr stk, InstrPtr pci) {
	str *ret = getArgReference_str(stk, pci, 0);
	str *user = getArgReference_str(stk, pci, 1);

	(void)mb;

	return AUTHgetPasswordHash(ret, cntxt, *user);
}

static str CLTchangeUsername(Client cntxt, MalBlkPtr mb, MalStkPtr stk, InstrPtr pci) {
	str *old = getArgReference_str(stk, pci, 1);
	str *new = getArgReference_str(stk, pci, 2);

	(void)mb;

	return AUTHchangeUsername(cntxt, *old, *new);
}

static str CLTchangePassword(Client cntxt, MalBlkPtr mb, MalStkPtr stk, InstrPtr pci) {
	str *old = getArgReference_str(stk, pci, 1);
	str *new = getArgReference_str(stk, pci, 2);

	(void)mb;

	return AUTHchangePassword(cntxt, *old, *new);
}

static str CLTsetPassword(Client cntxt, MalBlkPtr mb, MalStkPtr stk, InstrPtr pci) {
	str *usr = getArgReference_str(stk, pci, 1);
	str *new = getArgReference_str(stk, pci, 2);

	(void)mb;

	return AUTHsetPassword(cntxt, *usr, *new);
}

static str CLTcheckPermission(Client cntxt, MalBlkPtr mb, MalStkPtr stk, InstrPtr pci) {
#ifdef HAVE_SHA1_UPDATE
	str *usr = getArgReference_str(stk, pci, 1);
	str *pw = getArgReference_str(stk, pci, 2);
	str ch = "";
	str algo = "SHA1";
	oid id;
	str pwd,msg;

	(void)mb;

	if (!(pwd = mcrypt_SHA1Sum(*pw, strlen(*pw))))
		throw(MAL, "clients.checkPermission", SQLSTATE(HY013) MAL_MALLOC_FAIL);
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

static str CLTgetUsers(Client cntxt, MalBlkPtr mb, MalStkPtr stk, InstrPtr pci) {
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
	if (is_int_nil(delay))
		throw(MAL,"mal.shutdown", "Delay cannot be NULL");
	if (delay < 0)
		throw(MAL,"mal.shutdown", "Delay cannot be negative");
	if (is_bit_nil(force))
		throw(MAL,"mal.shutdown", "Force cannot be NULL");
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
	if (*ret == NULL)
		throw(MAL, "mal.shutdown", SQLSTATE(HY013) MAL_MALLOC_FAIL);
	return MAL_SUCCEED;
}

str
CLTsessions(Client cntxt, MalBlkPtr mb, MalStkPtr stk, InstrPtr pci)
{
	BAT *id = NULL, *user = NULL, *login = NULL, *sessiontimeout = NULL, *querytimeout = NULL, *idle= NULL;
	BAT *opt = NULL, *wlimit = NULL, *mlimit = NULL;
	bat *idId = getArgReference_bat(stk,pci,0);
	bat *userId = getArgReference_bat(stk,pci,1);
	bat *loginId = getArgReference_bat(stk,pci,2);
	bat *idleId = getArgReference_bat(stk,pci,3);
	bat *optId = getArgReference_bat(stk,pci,4);
	bat *sessiontimeoutId = getArgReference_bat(stk,pci,5);
	bat *querytimeoutId = getArgReference_bat(stk,pci,6);
	bat *wlimitId = getArgReference_bat(stk,pci,7);
	bat *mlimitId = getArgReference_bat(stk,pci,8);
	Client c;
	timestamp ret;
	int timeout;
	str msg = NULL;

	(void) cntxt;
	(void) mb;

	id = COLnew(0, TYPE_int, 0, TRANSIENT);
	user = COLnew(0, TYPE_str, 0, TRANSIENT);
	login = COLnew(0, TYPE_timestamp, 0, TRANSIENT);
	opt = COLnew(0, TYPE_str, 0, TRANSIENT);
	sessiontimeout = COLnew(0, TYPE_int, 0, TRANSIENT);
	querytimeout = COLnew(0, TYPE_int, 0, TRANSIENT);
	wlimit = COLnew(0, TYPE_int, 0, TRANSIENT);
	mlimit = COLnew(0, TYPE_int, 0, TRANSIENT);
	idle = COLnew(0, TYPE_timestamp, 0, TRANSIENT);

	if (id == NULL || user == NULL || login == NULL || sessiontimeout == NULL || idle == NULL || querytimeout == NULL ||
	   opt == NULL || wlimit == NULL || mlimit == NULL ){
		if ( id) BBPunfix(id->batCacheid);
		if ( user) BBPunfix(user->batCacheid);
		if ( login) BBPunfix(login->batCacheid);
		if ( sessiontimeout) BBPunfix(sessiontimeout->batCacheid);
		if ( querytimeout) BBPunfix(querytimeout->batCacheid);
		if ( idle) BBPunfix(idle->batCacheid);

		if ( opt) BBPunfix(opt->batCacheid);
		if ( wlimit) BBPunfix(wlimit->batCacheid);
		if ( mlimit) BBPunfix(mlimit->batCacheid);
		throw(SQL,"sql.sessions", SQLSTATE(HY013) MAL_MALLOC_FAIL);
	}

	MT_lock_set(&mal_contextLock);

	for (c = mal_clients; c < mal_clients + MAL_MAXCLIENTS; c++) {
		if (c->mode == RUNCLIENT) {
			if (BUNappend(user, c->username, false) != GDK_SUCCEED)
				goto bailout;
			ret = timestamp_fromtime(c->login);
			if (is_timestamp_nil(ret)) {
				msg = createException(SQL, "sql.sessions", SQLSTATE(22003) "Failed to convert user logged time");
				goto bailout;
			}
			if (BUNappend(id, &c->idx, false) != GDK_SUCCEED)
				goto bailout;
			if (BUNappend(login, &ret, false) != GDK_SUCCEED)
				goto bailout;
			timeout = (int)(c->sessiontimeout / 1000000);
			if (BUNappend(sessiontimeout, &timeout, false) != GDK_SUCCEED)
				goto bailout;
			timeout = (int)(c->querytimeout / 1000000);
			if (BUNappend(querytimeout, &timeout, false) != GDK_SUCCEED)
				goto bailout;
			if( c->idle){
				ret = timestamp_fromtime(c->idle);
				if (is_timestamp_nil(ret)) {
					msg = createException(SQL, "sql.sessions", SQLSTATE(22003) "Failed to convert user logged time");
					goto bailout;
				}
			} else
				ret = timestamp_nil;
			if (BUNappend(idle, &ret, false) != GDK_SUCCEED)
				goto bailout;
			if (BUNappend(opt, &c->optimizer, false) != GDK_SUCCEED)
				goto bailout;
			if (BUNappend(wlimit, &c->workerlimit, false) != GDK_SUCCEED)
				goto bailout;
			if (BUNappend(mlimit, &c->memorylimit, false) != GDK_SUCCEED)
				goto bailout;
		}
	}
	MT_lock_unset(&mal_contextLock);
	BBPkeepref(*idId = id->batCacheid);
	BBPkeepref(*userId = user->batCacheid);
	BBPkeepref(*loginId = login->batCacheid);
	BBPkeepref(*sessiontimeoutId = sessiontimeout->batCacheid);
	BBPkeepref(*querytimeoutId = querytimeout->batCacheid);
	BBPkeepref(*idleId = idle->batCacheid);

	BBPkeepref(*optId = opt->batCacheid);
	BBPkeepref(*wlimitId = wlimit->batCacheid);
	BBPkeepref(*mlimitId = mlimit->batCacheid);
	return MAL_SUCCEED;

bailout:
	MT_lock_unset(&mal_contextLock);
	BBPunfix(id->batCacheid);
	BBPunfix(user->batCacheid);
	BBPunfix(login->batCacheid);
	BBPunfix(sessiontimeout->batCacheid);
	BBPunfix(querytimeout->batCacheid);
	BBPunfix(idle->batCacheid);

	BBPunfix(opt->batCacheid);
	BBPunfix(wlimit->batCacheid);
	BBPunfix(mlimit->batCacheid);
	return msg;
}

#include "mel.h"
mel_func clients_init_funcs[] = {
 pattern("clients", "setListing", CLTsetListing, false, "Turn on/off echo of MAL instructions:\n1 - echo input,\n2 - show mal instruction,\n4 - show details of type resolutoin, \n8 - show binding information.", args(1,2, arg("",int),arg("flag",int))),
 pattern("clients", "getId", CLTgetClientId, false, "Return a number that uniquely represents the current client.", args(1,1, arg("",int))),
 pattern("clients", "getInfo", CLTInfo, false, "Pseudo bat with client attributes.", args(2,2, batarg("",str),batarg("",str))),
 pattern("clients", "getScenario", CLTgetScenario, false, "Retrieve current scenario name.", args(1,1, arg("",str))),
 pattern("clients", "setScenario", CLTsetScenario, false, "Switch to other scenario handler, return previous one.", args(1,2, arg("",str),arg("msg",str))),
 pattern("clients", "quit", CLTquit, false, "Terminate the client session.", args(1,1, arg("",void))),
 pattern("clients", "quit", CLTquit, false, "Terminate the session for a single client using a soft error.\nIt is the privilige of the console user.", args(1,2, arg("",void),arg("idx",int))),
 command("clients", "getLogins", CLTLogin, false, "Pseudo bat of client id and login time.", args(2,2, batarg("user",oid),batarg("start",str))),
 pattern("clients", "stop", CLTstop, false, "Stop the query execution at the next eligble statement.", args(0,1, arg("id",int))),
 pattern("clients", "suspend", CLTsuspend, false, "Put a client process to sleep for some time.\nIt will simple sleep for a second at a time, until\nthe awake bit has been set in its descriptor", args(1,2, arg("",void),arg("id",int))),
 pattern("clients", "wakeup", CLTwakeup, false, "Wakeup a client process", args(1,2, arg("",void),arg("id",int))),
 pattern("clients", "getprofile", CLTgetProfile, false, "Retrieve the profile settings for a client", args(5,5, arg("opt",str),arg("q",int),arg("s",int),arg("w",int),arg("m",int))),
 pattern("clients", "setsession", CLTsetSessionTimeout, false, "Abort a session after  n seconds.", args(1,2, arg("",void),arg("n",lng))),
 pattern("clients", "settimeout", CLTsetTimeout, false, "Abort a query after  n seconds.", args(1,2, arg("",void),arg("n",lng))),
 pattern("clients", "settimeout", CLTsetTimeout, false, "Abort a query after q seconds (q=0 means run undisturbed).\nThe session timeout aborts the connection after spending too\nmany seconds on query processing.", args(1,3, arg("",void),arg("q",lng),arg("s",lng))),
 pattern("clients", "setquerytimeout", CLTqueryTimeout, false, "", args(1,2, arg("",void),arg("n",int))),
 pattern("clients", "setquerytimeout", CLTqueryTimeout, false, "", args(1,3, arg("",void),arg("sid",bte),arg("n",int))),
 pattern("clients", "setquerytimeout", CLTqueryTimeout, false, "", args(1,3, arg("",void),arg("sid",sht),arg("n",int))),
 pattern("clients", "setquerytimeout", CLTqueryTimeout, false, "A query is aborted after q seconds (q=0 means run undisturbed).", args(1,3, arg("",void),arg("sid",int),arg("n",int))),
 pattern("clients", "setsessiontimeout", CLTsessionTimeout, false, "", args(1,2, arg("",void),arg("n",int))),
 pattern("clients", "setsessiontimeout", CLTsessionTimeout, false, "", args(1,3, arg("",void),arg("sid",bte),arg("n",int))),
 pattern("clients", "setsessiontimeout", CLTsessionTimeout, false, "", args(1,3, arg("",void),arg("sid",sht),arg("n",int))),
 pattern("clients", "setsessiontimeout", CLTsessionTimeout, false, "Set the session timeout for a particulat session id", args(1,3, arg("",void),arg("sid",int),arg("n",int))),
 pattern("clients", "setoptimizer", CLTsetoptimizer, false, "", args(1,2, arg("",void),arg("opt",str))),
 pattern("clients", "setoptimizer", CLTsetoptimizer, false, "Set the session optimizer", args(1,3, arg("",void),arg("sid",int),arg("opt",str))),
 pattern("clients", "setworkerlimit", CLTsetworkerlimit, false, "", args(1,2, arg("",void),arg("n",int))),
 pattern("clients", "setworkerlimit", CLTsetworkerlimit, false, "Limit the number of worker threads per query", args(1,3, arg("",void),arg("sid",int),arg("n",int))),
 pattern("clients", "setmemorylimit", CLTsetmemorylimit, false, "", args(1,2, arg("",void),arg("n",int))),
 pattern("clients", "setmemorylimit", CLTsetmemorylimit, false, "Limit the memory claim in MB per query", args(1,3, arg("",void),arg("sid",int),arg("n",int))),
 pattern("clients", "stopsession", CLTstopSession, false, "", args(1,2, arg("",void),arg("sid",bte))),
 pattern("clients", "stopsession", CLTstopSession, false, "", args(1,2, arg("",void),arg("sid",sht))),
 pattern("clients", "stopsession", CLTstopSession, false, "Stop a particular session", args(1,2, arg("",void),arg("sid",int))),
 command("clients", "setprinttimeout", CLTsetPrintTimeout, false, "Print running query every so many seconds.", args(1,2, arg("",void),arg("n",int))),
 pattern("clients", "shutdown", CLTshutdown, false, "", args(1,2, arg("",str),arg("delay",int))),
 pattern("clients", "shutdown", CLTshutdown, false, "Close all other client connections. Return if it succeeds.\nIf forced is set then always stop the system the hard way", args(1,3, arg("",str),arg("delay",int),arg("forced",bit))),
 command("clients", "md5sum", CLTmd5sum, false, "Return hex string representation of the MD5 hash of the given string", args(1,2, arg("",str),arg("pw",str))),
 command("clients", "sha1sum", CLTsha1sum, false, "Return hex string representation of the SHA-1 hash of the given string", args(1,2, arg("",str),arg("pw",str))),
 command("clients", "sha2sum", CLTsha2sum, false, "Return hex string representation of the SHA-2 hash with bits of the given string", args(1,3, arg("",str),arg("pw",str),arg("bits",int))),
 command("clients", "ripemd160sum", CLTripemd160sum, false, "Return hex string representation of the RIPEMD160 hash of the given string", args(1,2, arg("",str),arg("pw",str))),
 command("clients", "backendsum", CLTbackendsum, false, "Return hex string representation of the currently used hash of the given string", args(1,2, arg("",str),arg("pw",str))),
 pattern("clients", "addUser", CLTaddUser, false, "Allow user with password access to the given scenarios", args(1,3, arg("",oid),arg("nme",str),arg("pw",str))),
 pattern("clients", "removeUser", CLTremoveUser, false, "Remove the given user from the system", args(1,2, arg("",void),arg("nme",str))),
 pattern("clients", "getUsername", CLTgetUsername, false, "Return the username of the currently logged in user", args(1,1, arg("",str))),
 pattern("clients", "getPasswordHash", CLTgetPasswordHash, false, "Return the password hash of the given user", args(1,2, arg("",str),arg("user",str))),
 pattern("clients", "changeUsername", CLTchangeUsername, false, "Change the username of the user into the new string", args(1,3, arg("",void),arg("old",str),arg("new",str))),
 pattern("clients", "changePassword", CLTchangePassword, false, "Change the password for the current user", args(1,3, arg("",void),arg("old",str),arg("new",str))),
 pattern("clients", "setPassword", CLTsetPassword, false, "Set the password for the given user", args(1,3, arg("",void),arg("user",str),arg("pass",str))),
 pattern("clients", "checkPermission", CLTcheckPermission, false, "Check permission for a user, requires hashed password (backendsum)", args(1,3, arg("",void),arg("usr",str),arg("pw",str))),
 pattern("clients", "getUsers", CLTgetUsers, false, "return a BAT with user id and one with name available in the system", args(2,2, batarg("",oid),batarg("",str))),
 { .imp=NULL }
};
#include "mal_import.h"
#ifdef _MSC_VER
#undef read
#pragma section(".CRT$XCU",read)
#endif
LIB_STARTUP_FUNC(init_clients_mal)
{ mal_module("clients", NULL, clients_init_funcs); }
