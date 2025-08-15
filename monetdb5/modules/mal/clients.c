/*
 * SPDX-License-Identifier: MPL-2.0
 *
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0.  If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 *
 * Copyright 2024, 2025 MonetDB Foundation;
 * Copyright August 2008 - 2023 MonetDB B.V.;
 * Copyright 1997 - July 2008 CWI.
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

static str
CLTsetListing(Client ctx, MalBlkPtr mb, MalStkPtr stk, InstrPtr pci)
{
	(void) mb;
	*getArgReference_int(stk, pci, 0) = ctx->listing;
	ctx->listing = *getArgReference_int(stk, pci, 1);
	return MAL_SUCCEED;
}

static str
CLTgetClientId(Client ctx, MalBlkPtr mb, MalStkPtr stk, InstrPtr pci)
{
	(void) mb;
	if (ctx - mal_clients < 0 || ctx - mal_clients >= MAL_MAXCLIENTS)
		throw(MAL, "clients.getClientId", "Illegal client index");
	*getArgReference_int(stk, pci, 0) = (int) (ctx - mal_clients);
	return MAL_SUCCEED;
}

static str
CLTgetScenario(Client ctx, MalBlkPtr mb, MalStkPtr stk, InstrPtr pci)
{
	(void) mb;
	if (ctx->scenario)
		*getArgReference_str(stk, pci, 0) = MA_STRDUP(ctx->alloc, ctx->scenario);
	else
		*getArgReference_str(stk, pci, 0) = MA_STRDUP(ctx->alloc, "nil");
	if (*getArgReference_str(stk, pci, 0) == NULL)
		throw(MAL, "clients.getScenario", SQLSTATE(HY013) MAL_MALLOC_FAIL);
	return MAL_SUCCEED;
}

static str
CLTsetScenario(Client ctx, MalBlkPtr mb, MalStkPtr stk, InstrPtr pci)
{
	str msg = MAL_SUCCEED;

	(void) mb;
	msg = setScenario(ctx, *getArgReference_str(stk, pci, 1));
	*getArgReference_str(stk, pci, 0) = 0;
	if (msg == NULL) {
		*getArgReference_str(stk, pci, 0) = MA_STRDUP(ctx->alloc, ctx->scenario);
		if (*getArgReference_str(stk, pci, 0) == NULL)
			throw(MAL, "clients.setScenario", SQLSTATE(HY013) MAL_MALLOC_FAIL);
	}
	return msg;
}

static void
CLTtimeConvert(time_t l, char *s)
{
	struct tm localt = (struct tm) { 0 };

	(void) localtime_r(&l, &localt);

#ifdef HAVE_ASCTIME_R3
	asctime_r(&localt, s, 26);
#else
	asctime_r(&localt, s);
#endif
	s[24] = 0;					/* remove newline */
}

static str
CLTInfo(Client ctx, MalBlkPtr mb, MalStkPtr stk, InstrPtr pci)
{
	bat *ret = getArgReference_bat(stk, pci, 0);
	bat *ret2 = getArgReference_bat(stk, pci, 1);
	BAT *b = COLnew(0, TYPE_str, 12, TRANSIENT);
	BAT *bn = COLnew(0, TYPE_str, 12, TRANSIENT);
	char buf[32];				/* 32 bytes are enough */

	(void) mb;
	if (b == 0 || bn == 0) {
		BBPreclaim(b);
		BBPreclaim(bn);
		throw(MAL, "clients.info", SQLSTATE(HY013) MAL_MALLOC_FAIL);
	}

	(void) snprintf(buf, sizeof(buf), "" LLFMT "", (lng) ctx->user);
	if (BUNappend(b, "user", false) != GDK_SUCCEED ||
		BUNappend(bn, buf, false) != GDK_SUCCEED)
		goto bailout;

	if (BUNappend(b, "scenario", false) != GDK_SUCCEED ||
		BUNappend(bn, ctx->scenario, false) != GDK_SUCCEED)
		goto bailout;

	(void) snprintf(buf, sizeof(buf), "%d", ctx->listing);
	if (BUNappend(b, "listing", false) != GDK_SUCCEED ||
		BUNappend(bn, buf, false) != GDK_SUCCEED)
		goto bailout;

	CLTtimeConvert(ctx->login, buf);
	if (BUNappend(b, "login", false) != GDK_SUCCEED ||
		BUNappend(bn, buf, false) != GDK_SUCCEED)
		goto bailout;
	*ret = b->batCacheid;
	BBPkeepref(b);
	*ret2 = bn->batCacheid;
	BBPkeepref(bn);
	return MAL_SUCCEED;

  bailout:
	BBPunfix(b->batCacheid);
	BBPunfix(bn->batCacheid);
	throw(MAL, "clients.info", SQLSTATE(HY013) MAL_MALLOC_FAIL);
}

static str
CLTLogin(Client ctx, bat *nme, bat *ret)
{
	(void) ctx;
	BAT *b = COLnew(0, TYPE_str, 12, TRANSIENT);
	BAT *u = COLnew(0, TYPE_oid, 12, TRANSIENT);
	int i;
	char s[32];

	if (b == 0 || u == 0)
		goto bailout;

	for (i = 0; i < MAL_MAXCLIENTS; i++) {
		Client c = mal_clients + i;
		if (c->mode >= RUNCLIENT && !is_oid_nil(c->user)) {
			CLTtimeConvert(c->login, s);
			if (BUNappend(b, s, false) != GDK_SUCCEED ||
				BUNappend(u, &c->user, false) != GDK_SUCCEED)
				goto bailout;
		}
	}
	*ret = b->batCacheid;
	BBPkeepref(b);
	*nme = u->batCacheid;
	BBPkeepref(u);
	return MAL_SUCCEED;

  bailout:
	BBPreclaim(b);
	BBPreclaim(u);
	throw(MAL, "clients.getLogins", SQLSTATE(HY013) MAL_MALLOC_FAIL);
}

static str
CLTquit(Client ctx, MalBlkPtr mb, MalStkPtr stk, InstrPtr pci)
{
	str msg = MAL_SUCCEED;
	int idx = ctx->idx;
	(void) mb;

	if (pci->argc == 2) {
		if (ctx->user == MAL_ADMIN)
			idx = *getArgReference_int(stk, pci, 1);
		else
			throw(MAL, "clients.quit",
				  SQLSTATE(42000) "Administrator rights required");
	}

	if (idx < 0 || idx > MAL_MAXCLIENTS)
		throw(MAL, "clients.quit", "Illegal session id");

	/* A user can only quit a session under the same id */
	MT_lock_set(&mal_contextLock);
	if (mal_clients[idx].mode == FREECLIENT)
		msg = createException(MAL, "clients.stop",
							  "Session not active anymore");
	else
		mal_clients[idx].mode = FINISHCLIENT;
	MT_lock_unset(&mal_contextLock);
	return msg;
}

/* Stopping a client in a soft manner by setting the time out marker */
static str
CLTstop(Client ctx, MalBlkPtr mb, MalStkPtr stk, InstrPtr pci)
{
	int idx = ctx->idx;
	str msg = MAL_SUCCEED;

	(void) mb;
	if (ctx->user != MAL_ADMIN)
		throw(MAL, "clients.stop",
			  SQLSTATE(42000) "Administrator rights required");

	idx = *getArgReference_int(stk, pci, 1);

	if (idx < 0 || idx > MAL_MAXCLIENTS)
		throw(MAL, "clients.stop", "Illegal session id");

	MT_lock_set(&mal_contextLock);
	if (mal_clients[idx].mode == FREECLIENT)
		msg = createException(MAL, "clients.stop",
							  "Session not active anymore");
	else
		mal_clients[idx].qryctx.endtime = 1;	/* stop client now */
	/* this forces the designated client to stop at the next instruction */
	MT_lock_unset(&mal_contextLock);
	return msg;
}

static str
CLTsetoptimizer(Client ctx, MalBlkPtr mb, MalStkPtr stk, InstrPtr pci)
{
	int idx = ctx->idx;
	str opt, msg = MAL_SUCCEED;

	(void) mb;
	if (pci->argc == 3) {
		if (ctx->user == MAL_ADMIN) {
			idx = *getArgReference_int(stk, pci, 1);
			opt = *getArgReference_str(stk, pci, 2);
		} else {
			throw(MAL, "clients.setoptimizer",
				  SQLSTATE(42000) "Administrator rights required");
		}
	} else {
		opt = *getArgReference_str(stk, pci, 1);
	}

	if (idx < 0 || idx > MAL_MAXCLIENTS)
		throw(MAL, "clients.setoptimizer", "Illegal session id");
	if (strNil(opt))
		 throw(MAL, "clients.setoptimizer", "Input string cannot be NULL");
	if (strlen(opt) >= sizeof(mal_clients[idx].optimizer))
		 throw(MAL, "clients.setoptimizer", "Input string is too large");
	if (!isOptimizerPipe(opt))
		 throw(MAL, "clients.setoptimizer", "Valid optimizer pipe expected");

	MT_lock_set(&mal_contextLock);
	if (mal_clients[idx].mode == FREECLIENT)
		msg = createException(MAL, "clients.setoptimizer",
							  "Session not active anymore");
	else
		strcpy_len(mal_clients[idx].optimizer, opt,
				   sizeof(mal_clients[idx].optimizer));
	MT_lock_unset(&mal_contextLock);
	return msg;
}

static str
CLTsetworkerlimit(Client ctx, MalBlkPtr mb, MalStkPtr stk, InstrPtr pci)
{
	str msg = MAL_SUCCEED;
	int idx = ctx->idx, limit;

	(void) mb;
	if (pci->argc == 3) {
		if (ctx->user == MAL_ADMIN) {
			idx = *getArgReference_int(stk, pci, 1);
			limit = *getArgReference_int(stk, pci, 2);
		} else {
			throw(MAL, "clients.setworkerlimit",
				  SQLSTATE(42000) "Administrator rights required");
		}
	} else {
		limit = *getArgReference_int(stk, pci, 1);
	}

	if (idx < 0 || idx > MAL_MAXCLIENTS)
		throw(MAL, "clients.setworkerlimit", "Illegal session id");
	if (is_int_nil(limit))
		throw(MAL, "clients.setworkerlimit",
			  "The number of workers cannot be NULL");
	if (limit < 0)
		throw(MAL, "clients.setworkerlimit",
			  "The number of workers cannot be negative");

	MT_lock_set(&mal_contextLock);
	if (mal_clients[idx].mode == FREECLIENT)
		msg = createException(MAL, "clients.setworkerlimit",
							  "Session not active anymore");
	else {
		if (limit == 0) {
			if (mal_clients[idx].maxworkers > 0)
				limit = mal_clients[idx].maxworkers;
		} else if (ctx->user != MAL_ADMIN &&
				   mal_clients[idx].maxworkers > 0 &&
				   mal_clients[idx].maxworkers < limit) {
			limit = mal_clients[idx].maxworkers;
		}
		mal_clients[idx].workerlimit = limit;
	}
	MT_lock_unset(&mal_contextLock);
	return msg;
}

static str
CLTsetmemorylimit(Client ctx, MalBlkPtr mb, MalStkPtr stk, InstrPtr pci)
{
	str msg = MAL_SUCCEED;
	int idx = ctx->idx, limit;

	(void) mb;
	if (pci->argc == 3) {
		if (ctx->user == MAL_ADMIN) {
			idx = *getArgReference_sht(stk, pci, 1);
			limit = *getArgReference_int(stk, pci, 2);
		} else {
			throw(MAL, "clients.setmemorylimit",
				  SQLSTATE(42000) "Administrator rights required");
		}
	} else {
		limit = *getArgReference_int(stk, pci, 1);
	}

	if (idx < 0 || idx > MAL_MAXCLIENTS)
		throw(MAL, "clients.setmemorylimit", "Illegal session id");
	if (is_int_nil(limit))
		throw(MAL, "clients.setmemorylimit",
			  "The memory limit cannot be NULL");
	if (limit < 0)
		throw(MAL, "clients.setmemorylimit",
			  "The memory limit cannot be negative");

	lng mlimit = (lng) limit << 20;

	MT_lock_set(&mal_contextLock);
	if (mal_clients[idx].mode == FREECLIENT)
		msg = createException(MAL, "clients.setmemorylimit",
							  "Session not active anymore");
	else {
		if (mlimit == 0) {
			if (mal_clients[idx].maxmem > 0)
				mlimit = mal_clients[idx].maxmem;
		} else if (ctx->user != MAL_ADMIN &&
				   mal_clients[idx].maxmem > 0 &&
				   mal_clients[idx].maxmem < mlimit) {
			mlimit = mal_clients[idx].maxmem;
		}
		mal_clients[idx].memorylimit = (int) (mlimit >> 20);
		mal_clients[idx].qryctx.maxmem = (ATOMIC_BASE_TYPE) mlimit;
	}
	MT_lock_unset(&mal_contextLock);
	return msg;
}

static str
CLTstopSession(Client ctx, MalBlkPtr mb, MalStkPtr stk, InstrPtr pci)
{
	str msg = MAL_SUCCEED;
	int idx = ctx->idx;
	(void) mb;

	if (ctx->user != MAL_ADMIN) {
		throw(MAL, "clients.stopsession",
			  SQLSTATE(42000) "Administrator rights required");
	}
	idx = *getArgReference_int(stk, pci, 1);
	if (idx < 0 || idx > MAL_MAXCLIENTS)
		throw(MAL, "clients.stopSession", "Illegal session id");

	MT_lock_set(&mal_contextLock);
	if (mal_clients[idx].mode == FREECLIENT) {
		msg = createException(MAL, "clients.stopSession",
							  "Session not active anymore");
	} else {
		mal_clients[idx].qryctx.endtime = 1;	/* stop client now */
		mal_clients[idx].sessiontimeout = 1;	/* stop client session */
	}
	MT_lock_unset(&mal_contextLock);
	/* this forces the designated client to stop at the next instruction */
	return msg;
}

/* Queries can be temporarily suspended */
static str
CLTsuspend(Client ctx, MalBlkPtr mb, MalStkPtr stk, InstrPtr pci)
{
	str msg = MAL_SUCCEED;
	int idx = ctx->idx;

	if (ctx->user != MAL_ADMIN)
		throw(MAL, "clients.suspend",
			  SQLSTATE(42000) "Administrator rights required");

	idx = *getArgReference_int(stk, pci, 1);
	(void) mb;

	if (idx < 0 || idx > MAL_MAXCLIENTS)
		throw(MAL, "clients.suspend", "Illegal session id");

	MT_lock_set(&mal_contextLock);
	if (mal_clients[idx].mode == FREECLIENT)
		msg = createException(MAL, "clients.suspend",
							  "Session not active anymore");
	else
		msg = MCsuspendClient(idx);
	MT_lock_unset(&mal_contextLock);
	return msg;
}

static str
CLTwakeup(Client ctx, MalBlkPtr mb, MalStkPtr stk, InstrPtr pci)
{
	str msg = MAL_SUCCEED;
	int idx = ctx->idx;

	if (ctx->user != MAL_ADMIN)
		throw(MAL, "clients.wakeup",
			  SQLSTATE(42000) "Administrator rights required");

	idx = *getArgReference_int(stk, pci, 1);
	(void) mb;

	if (idx < 0 || idx > MAL_MAXCLIENTS)
		throw(MAL, "clients.wakeup", "Illegal session id");

	MT_lock_set(&mal_contextLock);
	if (mal_clients[idx].mode == FREECLIENT)
		msg = createException(MAL, "clients.wakeup",
							  "Session not active anymore");
	else
		msg = MCawakeClient(idx);
	MT_lock_unset(&mal_contextLock);
	return msg;
}

/* set query timeout based in seconds, converted into microseconds */
static str
CLTqueryTimeout(Client ctx, MalBlkPtr mb, MalStkPtr stk, InstrPtr pci)
{
	str msg = MAL_SUCCEED;
	int qto, idx = ctx->idx;
	(void) mb;

	if (pci->argc == 3) {
		if (ctx->user == MAL_ADMIN) {
			idx = *getArgReference_int(stk, pci, 1);
			qto = *getArgReference_int(stk, pci, 2);
		} else {
			throw(MAL, "clients.setquerytimeout",
				  SQLSTATE(42000) "Administrator rights required");
		}
	} else {
		qto = *getArgReference_int(stk, pci, 1);
	}
	if (is_int_nil(qto))
		throw(MAL, "clients.setquerytimeout", "Query timeout cannot be NULL");
	if (qto < 0)
		throw(MAL, "clients.setquerytimeout", "Query timeout should be >= 0");

	MT_lock_set(&mal_contextLock);
	if (mal_clients[idx].mode == FREECLIENT)
		msg = createException(MAL, "clients.setquerytimeout",
							  "Session not active anymore");
	else {
		/* when testing (TESTINGMASK), reduce timeout of 1 sec to 1 msec */
		lng timeout_micro = ATOMIC_GET(&GDKdebug) & TESTINGMASK
				&& qto == 1 ? 1000 : (lng) qto * 1000000;
		mal_clients[idx].querytimeout = timeout_micro;
	}
	MT_lock_unset(&mal_contextLock);
	return msg;
}

// set query timeout based in microseconds
static str
CLTqueryTimeoutMicro(Client ctx, MalBlkPtr mb, MalStkPtr stk, InstrPtr pci)
{
	str msg = MAL_SUCCEED;
	int idx = ctx->idx;
	lng qto = *getArgReference_lng(stk, pci, 1);
	(void) mb;

	if (is_lng_nil(qto))
		throw(MAL, "clients.queryTimeout", "Query timeout cannot be NULL");
	if (qto < 0)
		throw(MAL, "clients.queryTimeout", "Query timeout should be >= 0");

	MT_lock_set(&mal_contextLock);
	if (mal_clients[idx].mode == FREECLIENT)
		msg = createException(MAL, "clients.queryTimeout",
							  "Session not active anymore");
	else {
		mal_clients[idx].querytimeout = qto;
		QryCtx *qry_ctx = MT_thread_get_qry_ctx();
		if (qry_ctx) {
			qry_ctx->endtime = qry_ctx->starttime && qto ? qry_ctx->starttime + qto : 0;
		}
	}
	MT_lock_unset(&mal_contextLock);
	return msg;
}

/* Set the current session timeout in seconds */
static str
CLTsessionTimeout(Client ctx, MalBlkPtr mb, MalStkPtr stk, InstrPtr pci)
{
	str msg = MAL_SUCCEED;
	int sto = -1, idx = ctx->idx;
	(void) mb;

	if (pci->argc == 3) {
		if (ctx->user == MAL_ADMIN) {
			idx = *getArgReference_int(stk, pci, 1);
			sto = *getArgReference_int(stk, pci, 2);
		} else {
			throw(MAL, "clients.setsessiontimeout",
				  SQLSTATE(42000) "Administrator rights required");
		}
	} else {
		sto = *getArgReference_int(stk, pci, 1);
	}
	if (is_int_nil(sto))
		throw(MAL, "clients.setsessiontimeout",
			  "Session timeout cannot be NULL");
	if (sto < 0)
		throw(MAL, "clients.setsessiontimeout",
			  "Session timeout should be >= 0");
	if (idx < 0 || idx > MAL_MAXCLIENTS)
		throw(MAL, "clients.setsessiontimeout", "Illegal session id %d", idx);

	MT_lock_set(&mal_contextLock);
	if (mal_clients[idx].mode == FREECLIENT)
		msg = createException(MAL, "clients.setsessiontimeout",
							  "Session not active anymore");
	else {
		mal_clients[idx].sessiontimeout = sto > 0 ? (lng) sto *1000000 + (GDKusec() - mal_clients[idx].session) : 0;
		mal_clients[idx].logical_sessiontimeout = (lng) sto;
	}
	MT_lock_unset(&mal_contextLock);
	return msg;
}

/* Retrieve the session time out */
static str
CLTgetProfile(Client ctx, MalBlkPtr mb, MalStkPtr stk, InstrPtr pci)
{
	str *opt = getArgReference_str(stk, pci, 0);
	int *qto = getArgReference_int(stk, pci, 1);
	int *sto = getArgReference_int(stk, pci, 2);
	int *wlim = getArgReference_int(stk, pci, 3);
	int *mlim = getArgReference_int(stk, pci, 4);
	(void) mb;
	if (!(*opt = MA_STRDUP(ctx->alloc, ctx->optimizer)))
		throw(MAL, "clients.getProfile", SQLSTATE(HY013) MAL_MALLOC_FAIL);
	*qto = (int) (ctx->querytimeout / 1000000);
	*sto = (int) (ctx->sessiontimeout / 1000000);
	*wlim = ctx->workerlimit;
	*mlim = ctx->memorylimit;
	return MAL_SUCCEED;
}

/* Long running queries are traced in the logger
 * with a message from the interpreter.
 * This value should be set to minutes to avoid a lengthy log */
static str
CLTsetPrintTimeout(Client ctx, void *ret, const int *secs)
{
	(void) ctx;
	(void) ret;
	if (is_int_nil(*secs))
		setqptimeout(0);
	else
		setqptimeout((lng) *secs * 60 * 1000000);
	return MAL_SUCCEED;
}

static str
CLTmd5sum(Client ctx, str *ret, const char *const *pw)
{
	(void) ctx;
	if (strNil(*pw)) {
		*ret = MA_STRDUP(ctx->alloc, str_nil);
	} else {
		char *mret = mcrypt_MD5Sum(*pw, strlen(*pw));

		if (!mret)
			throw(MAL, "clients.md5sum", SQLSTATE(HY013) MAL_MALLOC_FAIL);
		*ret = MA_STRDUP(ctx->alloc, mret);
		free(mret);
	}
	if (*ret == NULL)
		throw(MAL, "clients.md5sum", SQLSTATE(HY013) MAL_MALLOC_FAIL);
	return MAL_SUCCEED;
}

static str
CLTsha1sum(Client ctx, str *ret, const char *const *pw)
{
	(void) ctx;
	if (strNil(*pw)) {
		*ret = MA_STRDUP(ctx->alloc, str_nil);
	} else {
		char *mret = mcrypt_SHA1Sum(*pw, strlen(*pw));

		if (!mret)
			throw(MAL, "clients.sha1sum", SQLSTATE(HY013) MAL_MALLOC_FAIL);
		*ret = MA_STRDUP(ctx->alloc, mret);
		free(mret);
	}
	if (*ret == NULL)
		throw(MAL, "clients.sha1sum", SQLSTATE(HY013) MAL_MALLOC_FAIL);
	return MAL_SUCCEED;
}

static str
CLTripemd160sum(Client ctx, str *ret, const char *const *pw)
{
	(void) ctx;
	if (strNil(*pw)) {
		*ret = MA_STRDUP(ctx->alloc, str_nil);
	} else {
		char *mret = mcrypt_RIPEMD160Sum(*pw, strlen(*pw));

		if (!mret)
			throw(MAL, "clients.ripemd160sum", SQLSTATE(HY013) MAL_MALLOC_FAIL);
		*ret = MA_STRDUP(ctx->alloc, mret);
		free(mret);
	}
	if (*ret == NULL)
		throw(MAL, "clients.ripemd160sum", SQLSTATE(HY013) MAL_MALLOC_FAIL);
	return MAL_SUCCEED;
}

static str
CLTsha2sum(Client ctx, str *ret, const char *const *pw, const int *bits)
{
	(void) ctx;
	if (strNil(*pw) || is_int_nil(*bits)) {
		*ret = MA_STRDUP(ctx->alloc, str_nil);
	} else {
		char *mret = 0;
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
			(void) mret;
			throw(ILLARG, "clients.sha2sum", "wrong number of bits "
				  "for SHA2 sum: %d", *bits);
		}
		if (!mret)
			throw(MAL, "clients.sha2sum", SQLSTATE(HY013) MAL_MALLOC_FAIL);
		*ret = MA_STRDUP(ctx->alloc, mret);
		free(mret);
	}
	if (*ret == NULL)
		throw(MAL, "clients.sha2sum", SQLSTATE(HY013) MAL_MALLOC_FAIL);
	return MAL_SUCCEED;
}

static str
CLTbackendsum(Client ctx, str *ret, const char *const *pw)
{
	(void) ctx;
	if (strNil(*pw)) {
		*ret = MA_STRDUP(ctx->alloc, str_nil);
	} else {
		char *mret = mcrypt_BackendSum(*pw, strlen(*pw));
		if (mret == NULL)
			throw(MAL, "clients.backendsum", SQLSTATE(HY013) MAL_MALLOC_FAIL);
		*ret = MA_STRDUP(ctx->alloc, mret);
		free(mret);
	}
	if (*ret == NULL)
		throw(MAL, "clients.backendsum", SQLSTATE(HY013) MAL_MALLOC_FAIL);
	return MAL_SUCCEED;
}

static str
CLTgetUsername(Client ctx, MalBlkPtr mb, MalStkPtr stk, InstrPtr pci)
{
	str *ret = getArgReference_str(stk, pci, 0);
	(void) mb;

	*ret = MA_STRDUP(ctx->alloc, ctx->username);
	return MAL_SUCCEED;
}

static str
CLTgetPasswordHash(Client ctx, MalBlkPtr mb, MalStkPtr stk, InstrPtr pci)
{
	(void) ctx;
	(void) mb;
	(void) stk;
	(void) pci;

	throw(MAL, "clients.getPassword",
		  SQLSTATE(0A000) PROGRAM_NYI);
}

static str
CLTcheckPermission(Client ctx, MalBlkPtr mb, MalStkPtr stk, InstrPtr pci)
{
	(void) ctx;
	(void) mb;
	(void) stk;
	(void) pci;

	throw(MAL, "clients.checkPermission",
		  SQLSTATE(0A000) PROGRAM_NYI);
}

str
CLTshutdown(Client ctx, MalBlkPtr mb, MalStkPtr stk, InstrPtr pci)
{
	str *ret = getArgReference_str(stk, pci, 0);
	int delay;
	bit force = FALSE;
	int leftover;
	char buf[1024] = { "safe to stop last connection" };

	if (pci->argc == 3)
		force = *getArgReference_bit(stk, pci, 2);

	(void) mb;

	delay = *getArgReference_bte(stk, pci, 1);

	if (ctx->user != MAL_ADMIN)
		throw(MAL, "mal.shutdown",
			  SQLSTATE(42000) "Administrator rights required");
	if (is_int_nil(delay))
		throw(MAL, "mal.shutdown", "Delay cannot be NULL");
	if (delay < 0)
		throw(MAL, "mal.shutdown", "Delay cannot be negative");
	if (is_bit_nil(force))
		throw(MAL, "mal.shutdown", "Force cannot be NULL");
	MCstopClients(ctx);
	do {
		if ((leftover = MCactiveClients() - 1))
			MT_sleep_ms(1000);
		delay--;
	} while (delay > 0 && leftover > 1);
	if (delay == 0 && leftover > 1)
		snprintf(buf, 1024, "%d client sessions still running", leftover);
	*ret = MA_STRDUP(ctx->alloc, buf);
	if (force)
		GDKprepareExit();
	if (*ret == NULL)
		throw(MAL, "mal.shutdown", SQLSTATE(HY013) MAL_MALLOC_FAIL);
	return MAL_SUCCEED;
}

static str
CLTgetSessionID(Client ctx, MalBlkPtr mb, MalStkPtr stk, InstrPtr pci)
{
	(void) mb;
	(void) stk;
	(void) pci;
	*getArgReference_int(stk, pci, 0) = ctx->idx;
	return MAL_SUCCEED;
}

static str
CLTsetClientInfo(Client ctx, MalBlkPtr mb, MalStkPtr stk, InstrPtr pci)
{
	(void)mb;
	str property = *getArgReference_str(stk, pci, 1);
	str value = *getArgReference_str(stk, pci, 2);

	MCsetClientInfo(ctx, property, value);
	return MAL_SUCCEED;
}

#include "mel.h"
mel_func clients_init_funcs[] = {
 pattern("clients", "setListing", CLTsetListing, true, "Turn on/off echo of MAL instructions:\n1 - echo input,\n2 - show mal instruction,\n4 - show details of type resolutoin, \n8 - show binding information.", args(1,2, arg("",int),arg("flag",int))),
 pattern("clients", "getId", CLTgetClientId, false, "Return a number that uniquely represents the current client.", args(1,1, arg("",int))),
 pattern("clients", "getInfo", CLTInfo, false, "Pseudo bat with client attributes.", args(2,2, batarg("",str),batarg("",str))),
 pattern("clients", "getScenario", CLTgetScenario, false, "Retrieve current scenario name.", args(1,1, arg("",str))),
 pattern("clients", "setScenario", CLTsetScenario, true, "Switch to other scenario handler, return previous one.", args(1,2, arg("",str),arg("msg",str))),
 pattern("clients", "quit", CLTquit, true, "Terminate the client session.", args(1,1, arg("",void))),
 pattern("clients", "quit", CLTquit, true, "Terminate the session for a single client using a soft error.\nIt is the privilege of the console user.", args(1,2, arg("",void),arg("idx",int))),
 command("clients", "getLogins", CLTLogin, false, "Pseudo bat of client id and login time.", args(2,2, batarg("user",oid),batarg("start",str))),
 pattern("clients", "stop", CLTstop, true, "Stop the query execution at the next eligible statement.", args(0,1, arg("id",int))),
 pattern("clients", "suspend", CLTsuspend, true, "Put a client process to sleep for some time.\nIt will simple sleep for a second at a time, until\nthe awake bit has been set in its descriptor", args(1,2, arg("",void),arg("id",int))),
 pattern("clients", "wakeup", CLTwakeup, true, "Wakeup a client process", args(1,2, arg("",void),arg("id",int))),
 pattern("clients", "getprofile", CLTgetProfile, false, "Retrieve the profile settings for a client", args(5,5, arg("opt",str),arg("q",int),arg("s",int),arg("w",int),arg("m",int))),
 pattern("clients", "setQryTimeoutMicro", CLTqueryTimeoutMicro, true, "", args(1,2, arg("",void),arg("n",lng))),
 pattern("clients", "setquerytimeout", CLTqueryTimeout, true, "", args(1,2, arg("",void),arg("n",int))),
 pattern("clients", "setquerytimeout", CLTqueryTimeout, true, "A query is aborted after q seconds (q=0 means run undisturbed).", args(1,3, arg("",void),arg("sid",int),arg("n",int))),
 pattern("clients", "setsessiontimeout", CLTsessionTimeout, true, "", args(1,2, arg("",void),arg("n",int))),
 pattern("clients", "setsessiontimeout", CLTsessionTimeout, true, "Set the session timeout for a particulat session id", args(1,3, arg("",void),arg("sid",int),arg("n",int))),
 pattern("clients", "setoptimizer", CLTsetoptimizer, true, "", args(1,2, arg("",void),arg("opt",str))),
 pattern("clients", "setoptimizer", CLTsetoptimizer, true, "Set the session optimizer", args(1,3, arg("",void),arg("sid",int),arg("opt",str))),
 pattern("clients", "setworkerlimit", CLTsetworkerlimit, true, "", args(1,2, arg("",void),arg("n",int))),
 pattern("clients", "setworkerlimit", CLTsetworkerlimit, true, "Limit the number of worker threads per query", args(1,3, arg("",void),arg("sid",int),arg("n",int))),
 pattern("clients", "setmemorylimit", CLTsetmemorylimit, true, "", args(1,2, arg("",void),arg("n",int))),
 pattern("clients", "setmemorylimit", CLTsetmemorylimit, true, "Limit the memory claim in MB per query", args(1,3, arg("",void),arg("sid",int),arg("n",int))),
 pattern("clients", "stopsession", CLTstopSession, true, "Stop a particular session", args(1,2, arg("",void),arg("sid",int))),
 command("clients", "setprinttimeout", CLTsetPrintTimeout, true, "Print running query every so many seconds.", args(1,2, arg("",void),arg("n",int))),
 pattern("clients", "shutdown", CLTshutdown, true, "", args(1,2, arg("",str),arg("delay",bte))),
 pattern("clients", "shutdown", CLTshutdown, true, "Close all other client connections. Return if it succeeds.\nIf forced is set then always stop the system the hard way", args(1,3, arg("",str),arg("delay",bte),arg("forced",bit))),
 command("clients", "md5sum", CLTmd5sum, false, "Return hex string representation of the MD5 hash of the given string", args(1,2, arg("",str),arg("pw",str))),
 command("clients", "sha1sum", CLTsha1sum, false, "Return hex string representation of the SHA-1 hash of the given string", args(1,2, arg("",str),arg("pw",str))),
 command("clients", "sha2sum", CLTsha2sum, false, "Return hex string representation of the SHA-2 hash with bits of the given string", args(1,3, arg("",str),arg("pw",str),arg("bits",int))),
 command("clients", "ripemd160sum", CLTripemd160sum, false, "Return hex string representation of the RIPEMD160 hash of the given string", args(1,2, arg("",str),arg("pw",str))),
 command("clients", "backendsum", CLTbackendsum, false, "Return hex string representation of the currently used hash of the given string", args(1,2, arg("",str),arg("pw",str))),
 pattern("clients", "getUsername", CLTgetUsername, false, "Return the username of the currently logged in user", args(1,1, arg("",str))),
 pattern("clients", "getPasswordHash", CLTgetPasswordHash, false, "Return the password hash of the given user", args(1,2, arg("",str),arg("user",str))),
 pattern("clients", "checkPermission", CLTcheckPermission, false, "Check permission for a user, requires hashed password (backendsum)", args(1,3, arg("",void),arg("usr",str),arg("pw",str))),
 pattern("clients", "current_sessionid", CLTgetSessionID, false, "return current session ID", args(1,1, arg("",int))),
 pattern("clients", "setinfo", CLTsetClientInfo, true, "set a clientinfo property", args(1, 3, arg("",str), arg("property", str), arg("value", str))),
 { .imp=NULL }
};
#include "mal_import.h"
#ifdef _MSC_VER
#undef read
#pragma section(".CRT$XCU",read)
#endif
LIB_STARTUP_FUNC(init_clients_mal)
{ mal_module("clients", NULL, clients_init_funcs); }
