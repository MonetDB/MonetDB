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
 * author Martin Kersten
 * DataCell Emitter
 *
 * Each emitter is supported by an independent thread
 * that reads the data from a container composed of a series of baskets.
 * The emitter keeps the events until it can create the channel.
 *
 * The emitter behaves as an ordinary continuous query.
 * It is is awakened when new tuples have arrived in the
 * baskets.
 */

#include "monetdb_config.h"
#include "emitter.h"
#include "dcsocket.h"
#include "stream_socket.h"

/* #define _DEBUG_EMITTER_ */

static Emitter emAnchor = NULL;

static str EMstartThread(Emitter em);

static Emitter
EMnew(str nme)
{
	Emitter em;
	em = (Emitter) GDKzalloc(sizeof(EMrecord));
	if (em == NULL)
		return em;
	em->name = GDKstrdup(nme);
	if (emAnchor)
		emAnchor->prv = em;
	em->nxt = emAnchor;
	emAnchor = em;
	return em;
}

Emitter
EMfind(str nme)
{
	Emitter r;
	char buf[BUFSIZ];

	for (r = emAnchor; r; r = r->nxt)
		if (strcmp(nme, r->name) == 0)
			return r;
	snprintf(buf,BUFSIZ,"datacell.%s",nme);
	BSKTtolower(buf);
	for (r = emAnchor; r; r = r->nxt)
		if (strcmp(buf, r->name) == 0)
			return r;
	return NULL;
}
/*
 * The MAL interface for creation of the emitter thread.
 * The baskets should already be defined. 
 */
static str
EMemitterStartInternal(int *ret, str *tbl, str *host, int *port, int mode, int protocol, int delay)
{
	Emitter em;
	int idx, i, j, len;
	BAT *b;

	if (EMfind(*tbl))
		throw(MAL, "emitter.new", "Duplicate emitter '%s'",*tbl);
	for (em = emAnchor; em; em = em->nxt)
		if (em->port == *port)
			throw(MAL, "emitter.new", "Port '%d' already in use", em->port);

	idx = BSKTlocate(*tbl);
	if (idx == 0) /* should not happen */
		throw(MAL, "emitter.new", "Basket '%s' not found",*tbl);

	em = EMnew(*tbl);
	if (em == NULL)
		throw(MAL, "emitter.new", MAL_MALLOC_FAIL);
	em->host = GDKstrdup(*host);
	em->port = *port;
	em->lck = 0;
	em->error = NULL;
	em->mode = mode;
	em->protocol = protocol;
	em->bskt = idx;
	em->delay = delay;
	em->lastseen = *timestamp_nil;
	/*
	 * All tables are prepended with a default tick bat.
	 * It becomes the synchronization handle.
	 */
	len = BSKTmemberCount(*tbl);
	if (len == 0)
		throw(MAL, "emitter.new", "Group '%s' has no members", *tbl);

	em->table.format = GDKzalloc(sizeof(Column) * (len + 1));
	em->table.format[0].c[0] = NULL;
	em->table.format[0].name = NULL;
	em->table.format[0].sep = GDKstrdup("[ ");
	em->table.format[0].seplen = (int) strlen(em->table.format[0].sep);

	for (j = 0, i = 0; i < baskets[idx].colcount; i++) {
		b = baskets[idx].primary[j];
		if (b == NULL) {
			em->table.nr_attrs = j;   /* ensure a consistent structure*/
			throw(MAL, "receptor.new", "Could not access descriptor");
		}

		em->table.format[j].c[0] = BATcopy(b, b->htype, b->ttype, FALSE);
		em->table.format[j].ci[0] = bat_iterator(em->table.format[j].c[0]);
		em->table.format[j].name = GDKstrdup(baskets[idx].cols[i]);
		em->table.format[j].sep = GDKstrdup(",");
		em->table.format[j].seplen = (int) strlen(em->table.format[j].sep);
		em->table.format[j].type = GDKstrdup(ATOMname(em->table.format[j].c[0]->ttype));
		em->table.format[j].adt = em->table.format[j].c[0]->ttype;
		em->table.format[j].nullstr = GDKstrdup("");
		em->table.format[j].tostr = &TABLETadt_toStr;
		em->table.format[j].frstr = &TABLETadt_frStr;
		em->table.format[j].extra = em->table.format + j;
		em->table.format[j].len = 0;
		em->table.format[j].nillen = 0;
		em->table.format[j].data = NULL;
		j++;
	}
	GDKfree(em->table.format[j - 1].sep);
	em->table.format[j - 1].sep = GDKstrdup("\n");
	em->table.format[j - 1].seplen = (int) strlen(em->table.format[j - 1].sep);
	em->table.nr_attrs = j;
	em->status = BSKTPAUSE;

	(void) ret;
#ifdef _DEBUG_EMITTER_
	mnstr_printf(EMout, "#Instantiate a new emitter %d fields\n", i);
#endif
	if (MT_create_thread(&em->pid, (void (*)(void *))EMstartThread, em, MT_THR_DETACHED) != 0)
		throw(MAL, "emitter.start", "Emitter '%s' initiation failed",em->name);
	return MAL_SUCCEED;
}

str
EMemitterStart(int *ret, str *tbl, str *host, int *port){
	return EMemitterStartInternal(ret,tbl,host,port, BSKTACTIVE, UDP, PAUSEDEFAULT);
}

str EMemitterPause(int *ret, str *nme)
{
	Emitter em;

	em = EMfind(*nme);
	if (em == NULL)
		throw(MAL, "emitter.pause", "Emitter '%s' not defined",*nme);

	em->status = BSKTPAUSE;

#ifdef _DEBUG_EMITTER_
	mnstr_printf(EMout, "#Pause emitter '%s'\n",*nme);
#endif
	(void) ret;
	return MAL_SUCCEED;
}

str EMemitterResume(int *ret, str *nme)
{
	Emitter em;

	(void) ret;
	em = EMfind(*nme);
	if (em == NULL)
		throw(MAL, "emitter.pause", "Emitter '%s' not defined",*nme);
#ifdef _DEBUG_EMITTER_
	mnstr_printf(EMout, "#Resume emitter '%s'\n",*nme);
#endif
	em->status = BSKTRUNNING;
	return MAL_SUCCEED;
}

str
EMpause(int *ret)
{
	Emitter em;
	str msg= MAL_SUCCEED;
	for (em = emAnchor; em && msg == MAL_SUCCEED; em = em->nxt)
		if (em->status == BSKTRUNNING) 
			msg = EMemitterPause(ret, &em->name);
	return msg;
}

str
EMresume(int *ret)
{
	Emitter em;
	str msg= MAL_SUCCEED;
	for (em = emAnchor; em && msg == MAL_SUCCEED; em = em->nxt)
		if (em->status == BSKTPAUSE)
			msg= EMemitterResume(ret, &em->name);
	return msg;
}

str EMemitterStop(int *ret, str *nme)
{
	Emitter em, rb;

	em = EMfind(*nme);
	if (em == NULL)
		throw(MAL, "emitter.pause", "Emitter '%s' not defined",*nme);
#ifdef _DEBUG_EMITTER_
	mnstr_printf(EMout, "#Drop a emitter\n");
#endif
	(void) ret;
	if (emAnchor == em)
		emAnchor = em->nxt;
	rb = em->prv;
	if (em->nxt)
		em->nxt->prv = em->prv;
	if (rb)
		rb->nxt = em->nxt;
	em->status = BSKTSTOP;
	if (em->lck)
		BSKTunlock(&em->lck, &em->name);
	MT_join_thread(em->pid);
	return MAL_SUCCEED;
}

str EMstop(int *ret)
{
	Emitter r, o;
	for (r = emAnchor; r; r = o) {
		o = r->nxt;
		EMemitterStop(ret, &r->name);
	}
	return MAL_SUCCEED;
}

/*
 * The hard part starts here. Each emitter is turned into
 * a separate thread that prepares the results for outside
 * actuators. Since they may be configured as a server,
 * we have to attempt a reconnect upon failure.
 */

static void
EMreconnect(Emitter em)
{
	do {
		em->error = socket_client_connect(&em->newsockfd, em->host, em->port);
		if (em->error) {
			mnstr_printf(EMout, "#Emitter connect fails: %s\n", em->error);
			MT_sleep_ms(em->delay);
		}
	} while (em->error);
}

static void
EMbody(Emitter em)
{
	BUN cnt;
	size_t j;
	int k, ret;
	BAT *b;

bodyRestart:
	/* create the actual channel */
#ifdef _DEBUG_EMITTER_
	mnstr_printf(EMout, "#create emitter %s channel %s %d mode %d protocol=%d\n", em->name, em->host, em->port, em->mode, em->protocol);
#endif
	if (em->mode == BSKTACTIVE && em->protocol == UDP)
		em->emitter = udp_wastream(em->host, em->port, em->name);
	else
		em->emitter = socket_wastream(em->newsockfd, em->name);
	if (em->emitter == NULL) {
		perror("Emitter: Could not open stream");
		mnstr_printf(EMout, "#stream %s.%d.%s\n", em->host, em->port, em->name);
		socket_close(em->newsockfd);
#ifdef _DEBUG_EMITTER_
		mnstr_printf(EMout, "#Exit emitter body loop\n");
#endif
		return;
	}
	/*
	 * Consume each event and store the result.
	 * If the thread is suspended, we sleep for at least one second.
	 */
	for (ret = 1; ret >= 0;) {
		if ( em->status == BSKTPAUSE){
#ifdef _DEBUG_EMITTER_
			mnstr_printf(EMout, "#Pause emitter %s\n",em->name);
#endif
			while (em->status == BSKTPAUSE)
				MT_sleep_ms(em->delay);
#ifdef _DEBUG_EMITTER_
			mnstr_printf(EMout, "#Pause emitter %s ended\n",em->name);
#endif
		}
		if (em->status == BSKTSTOP) {
			/* request to finalize the emitter*/
			mnstr_close(em->emitter);
			for (j = 0; j < em->table.nr_attrs; j++) {
				GDKfree(em->table.format[j].sep);
				GDKfree(em->table.format[j].name);
			}
			GDKfree(em->table.format);
			shutdown(em->newsockfd, SHUT_RDWR);
			GDKfree(em);
			em = NULL;
			return;
		}
		/* to speed up parallel processing the emitter should
		   grab the BATs from the basket primary store and
		   replace it with empty BATs.
		 */
		BSKTlock(&em->lck, &em->name, &em->delay);
		for (k = 0; k < baskets[em->bskt].colcount; k++) {
			if (em->table.format[k].c[0])
				BBPunfix(em->table.format[k].c[0]->batCacheid);
			b = baskets[em->bskt].primary[k];
			em->table.format[k].c[0] = BATcopy(b, b->htype, b->ttype, TRUE);
			em->table.format[k].ci[0] = bat_iterator(b);
			BATclear(b, FALSE);
		}
		BSKTunlock(&em->lck, &em->name);
		if ((cnt = BATcount(em->table.format[0].c[0]))) {
			MTIMEcurrent_timestamp(&baskets[em->bskt].seen);
			em->cycles++;

			cnt = BATcount(em->table.format[1].c[0]);
#ifdef _DEBUG_EMITTER_
			mnstr_printf(EMout, "#Emit " BUNFMT " tuples \n", cnt);
#endif
			em->table.nr = cnt;

			(void) MTIMEcurrent_timestamp(&em->lastseen);
			ret = TABLEToutput_file(&em->table, em->table.format[1].c[0], em->emitter);
			em->sent += (int) BATcount(em->table.format[1].c[0]);
#ifdef _DEBUG_EMITTER_
			if (ret < 0)
				mnstr_printf(EMout, "#Tuple emission failed\n");
#endif
			if (ret < 0)
				/* keep the events and try to setup a new connection */
				break;
			if (em->table.error) {
				mnstr_printf(GDKerr, "%s", em->table.error);
				em->table.error = 0;
			}
		} else if (em->delay)
			MT_sleep_ms(em->delay);
	}
	/* writing failed, lets restart */
	if (em->mode == BSKTPASSIVE) {
#ifdef _DEBUG_EMITTER_
		mnstr_printf(EMout, "#Restart the connection\n");
#endif
		if (em->status != BSKTSTOP)
			EMreconnect(em);
		goto bodyRestart;
	}
#ifdef _DEBUG_EMITTER_
	mnstr_printf(EMout, "#Exit emitter body loop\n");
#endif
	mnstr_close(em->emitter);
}

str
EMstartThread(Emitter em)
{
#ifdef _DEBUG_EMITTER_
	mnstr_printf(EMout, "#Emitter body %s started at %s:%d, servermode=%d\n",
			em->name, em->host, em->port, em->mode);
#endif
	if (em->mode == BSKTACTIVE) {
		EMbody(em);
#ifdef _DEBUG_EMITTER_
		mnstr_printf(EMout, "#End of emitter thread\n");
#endif
		return MAL_SUCCEED;
	}

	/* Handling the TCP connection */
	if (em->mode == BSKTPASSIVE &&
		(em->error = socket_server_connect(&em->sockfd, em->port))) {
		em->status = BSKTERROR;
		mnstr_printf(EMout, "#EMSTART THREAD: failed to start server:%s\n", em->error);
		return MAL_SUCCEED;
	}
	em->status = baskets[em->bskt].status = BSKTRUNNING;
	while (em->status != BSKTSTOP) {
		if (em->mode == BSKTPASSIVE) {
			/* in server mode you should expect new connections */
#ifdef _DEBUG_EMITTER_
			mnstr_printf(EMout, "#Emitter listens\n");
#endif
			em->error = socket_server_listen(em->sockfd, &em->newsockfd);
			if (em->error) {
				em->status = BSKTERROR;
				mnstr_printf(EMout, "#Emitter listen fails: %s\n", em->error);
			}

			if (MT_create_thread(&em->pid, (void (*)(void *))EMbody, em, MT_THR_DETACHED) != 0) {
				close_stream(em->emitter);
				throw(MAL, "emitter.start", "Process '%s' creation failed",em->name);
			}
		} else if (em->mode == BSKTACTIVE) {
			/* connect the actuator */
			EMreconnect(em);
			EMbody(em);
		}
	}
	shutdown(em->newsockfd, SHUT_RDWR);
	return MAL_SUCCEED;
}

static void
dumpEmitter(Emitter em)
{
	mnstr_printf(GDKout, "#emitter %s at %s:%d protocol=%s mode=%s status=%s delay=%d\n",
			em->name, em->host, em->port, protocolname[em->protocol], modename[em->mode], statusname[em->status], em->delay);
}

str
EMdump(void)
{
	Emitter rc = emAnchor;
	for (; rc; rc = rc->nxt)
		dumpEmitter(rc);
	return MAL_SUCCEED;
}
/* provide a tabular view for inspection */
str
EMtable(int *nameId, int *hostId, int *portId, int *protocolId, int *modeId, int *statusId, int *seenId, int *cyclesId, int *sentId, int *pendingId)
{
	BAT *name = NULL, *seen = NULL, *pending = NULL, *sent = NULL, *cycles = NULL;
	BAT *protocol = NULL, *mode = NULL, *status = NULL, *port = NULL, *host = NULL;
	Emitter em = emAnchor;

	name = BATnew(TYPE_oid, TYPE_str, BATTINY);
	if (name == 0)
		goto wrapup;
	BATseqbase(name, 0);
	host = BATnew(TYPE_oid, TYPE_str, BATTINY);
	if (host == 0)
		goto wrapup;
	BATseqbase(host, 0);
	port = BATnew(TYPE_oid, TYPE_int, BATTINY);
	if (port == 0)
		goto wrapup;
	BATseqbase(port, 0);
	protocol = BATnew(TYPE_oid, TYPE_str, BATTINY);
	if (protocol == 0)
		goto wrapup;
	BATseqbase(protocol, 0);
	mode = BATnew(TYPE_oid, TYPE_str, BATTINY);
	if (mode == 0)
		goto wrapup;
	BATseqbase(mode, 0);

	seen = BATnew(TYPE_oid, TYPE_timestamp, BATTINY);
	if (seen == 0)
		goto wrapup;
	BATseqbase(seen, 0);
	cycles = BATnew(TYPE_oid, TYPE_int, BATTINY);
	if (cycles == 0)
		goto wrapup;
	BATseqbase(cycles, 0);
	pending = BATnew(TYPE_oid, TYPE_int, BATTINY);
	if (pending == 0)
		goto wrapup;
	BATseqbase(pending, 0);
	sent = BATnew(TYPE_oid, TYPE_int, BATTINY);
	if (sent == 0)
		goto wrapup;
	BATseqbase(sent, 0);
	status = BATnew(TYPE_oid, TYPE_str, BATTINY);
	if (status == 0)
		goto wrapup;
	BATseqbase(status, 0);

	for (; em; em = em->nxt)
	if ( em->table.format[1].c[0]){
		BUNappend(name, em->name, FALSE);
		BUNappend(host, em->host, FALSE);
		BUNappend(port, &em->port, FALSE);
		BUNappend(protocol, protocolname[em->protocol], FALSE);
		BUNappend(mode, modename[em->mode], FALSE);
		BUNappend(status, statusname[em->status], FALSE);
		BUNappend(seen, &em->lastseen, FALSE);
		BUNappend(cycles, &em->cycles, FALSE);
		em->pending += (int) BATcount(em->table.format[1].c[0]);
		BUNappend(pending, &em->pending, FALSE);
		BUNappend(sent, &em->sent, FALSE);
	}

	BBPkeepref(*nameId = name->batCacheid);
	BBPkeepref(*hostId = host->batCacheid);
	BBPkeepref(*portId = port->batCacheid);
	BBPkeepref(*protocolId = protocol->batCacheid);
	BBPkeepref(*modeId = mode->batCacheid);
	BBPkeepref(*statusId = status->batCacheid);
	BBPkeepref(*seenId = seen->batCacheid);
	BBPkeepref(*cyclesId = cycles->batCacheid);
	BBPkeepref(*pendingId = pending->batCacheid);
	BBPkeepref(*sentId = sent->batCacheid);
	return MAL_SUCCEED;
wrapup:
	if (name)
		BBPreleaseref(name->batCacheid);
	if (host)
		BBPreleaseref(host->batCacheid);
	if (port)
		BBPreleaseref(port->batCacheid);
	if (protocol)
		BBPreleaseref(protocol->batCacheid);
	if (mode)
		BBPreleaseref(mode->batCacheid);
	if (status)
		BBPreleaseref(status->batCacheid);
	if (seen)
		BBPreleaseref(seen->batCacheid);
	if (cycles)
		BBPreleaseref(cycles->batCacheid);
	if (pending)
		BBPreleaseref(pending->batCacheid);
	if (sent)
		BBPreleaseref(sent->batCacheid);
	throw(MAL, "datacell.baskets", MAL_MALLOC_FAIL);
}
