/*
 * The contents of this file are subject to the MonetDB Public License
 * Version 1.1 (the "License"); you may not use this file except in
 * compliance with the License. You may obtain a copy of the License at
 * http://monetdb.cwi.nl/Legal/MonetDBLicense-1.1.html
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
 * @' The contents of this file are subject to the MonetDB Public License
 * @' Version 1.1 (the "License"); you may not use this file except in
 * @' compliance with the License. You may obtain a copy of the License at
 * @' http://monetdb.cwi.nl/Legal/MonetDBLicense-1.1.html
 * @'
 * @' Software distributed under the License is distributed on an "AS IS"
 * @' basis, WITHOUT WARRANTY OF ANY KIND, either express or implied. See the
 * @' License for the specific language governing rights and limitations
 * @' under the License.
 * @'
 * @' The Original Code is the MonetDB Database System.
 * @'
 * @' The Initial Developer of the Original Code is CWI.
 * @' Portions created by CWI are Copyright (C) 1997-2008 CWI.
 * @' All Rights Reserved.
 *
 * @f emitter
 * @a Martin Kersten
 * @v 1
 * @+ DataCell Emitter
 * This module is a prototype for the implementation of a
 * DataCell emitter.  It can be used as follows.
 * @example
 * @end example
 * After this call it will sent tuples from basket X_p1
 * to the stream Y at the localhost default port.
 *
 * Each emitter is supported by an independent thread
 * that reads the data from a container composed of a series of baskets.
 * The emitter keeps the events until it can create the channel.
 *
 * The emitter behaves as an ordinary continuous query.
 * It is is awakened when new tuples have arrived in the
 * baskets.
 *
 */
#include "emitter.h"
#include "batcalc.h"
#include "dcsocket.h"
#include "stream_socket.h"
#define EMPAUSE 1		/* connected but not reading the channel */
#define EMLISTEN 2		/* connected and reading the channel */
#define EMSTOP 3		/* not connected */
#define EMRESHUFFLE 4	/* reorganization of the BATs */
#define EMDROP 5
#define EMERROR 8               /* failed to establish the stream */
static str statusname[6]= {"<unknown>", "pause", "listen", "stop", "drop","error"};

#define EMPASSIVE 2
#define EMACTIVE 1
static str modename[3]= {"<unknown>","active","passive"};

#define TCP 1
#define UDP 2
#define CSV 3

static str protocolname[4] = {"<unknown>","TCP","UDP","CSV"};

#define PAUSEDEFAULT 1000

typedef struct EMITTER{
	str name;
	str host;
	int port;
    int mode;   /* active/passive */
    int protocol;   /* event protocol UDP,TCP,CSV */
    int bskt;   /* connected to a basket */
	int status;
	int delay;/* control the delay between attempts to connect */
	int lck;
	SOCKET sockfd;
	SOCKET newsockfd;
	stream *emitter;
	str error;
	MT_Id pid;
	lng sent;
	Tablet table;
	struct EMITTER *nxt, *prv;
} EMrecord, *Emitter;

static Emitter emAnchor = NULL;

static str EMstartThread(Emitter em);

static Emitter
EMnew(str nme)
{
	Emitter em;
	em = (Emitter)GDKzalloc(sizeof(EMrecord));
	if ( em == NULL)
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
	for (r = emAnchor; r; r = r->nxt)
		if (strcmp(nme, r->name) == 0)
			break;
	return r;
}
/*
 * @-
 * The MAL interface for managing the emitter pool
 * The baskets should already be defined. There order
 * is used to interpret the messages sent.
 */
str DCemitterNew(int *ret, str *tbl, str *host, int *port)
{
	Emitter em;
	int idx, i, j, len;
	BAT *b;

	if (EMfind(*tbl))
		throw(MAL, "emitter.new", "Duplicate emitter");
	for ( i = 1; i < bsktTop; i++)
	if ( baskets[i].port == *port)
		throw(MAL,"receptor.new","Port already in use");

	idx = BSKTlocate(*tbl);
	if (idx == 0) /* should not happen */
		throw(MAL, "emitter.new", "Basket not found");

	em = EMnew(*tbl);
	if( em == NULL)
		throw(MAL, "emitter.new", MAL_MALLOC_FAIL);
	em->host = GDKstrdup(*host);
	em->port = *port;
	em->delay = PAUSEDEFAULT;
	em->lck = 0;
	em->error = NULL;
	em->mode = EMACTIVE;
	em->protocol = UDP;
	em->bskt = idx;
	em->delay = 10;
	/*
	 * @-
	 * All tables are prepended with a default tick bat.
	 * It becomes the synchronization handle.
	 */
	len = BSKTmemberCount(*tbl);
	if (len == 0)
		throw(MAL, "emitter.new", "Group has no members");

	em->table.format = GDKzalloc(sizeof(Column) * (len + 1));
	em->table.format[0].c[0] = NULL;
	em->table.format[0].name = NULL;
	em->table.format[0].sep = GDKstrdup("[ ");
	em->table.format[0].seplen = (int)strlen(em->table.format[0].sep);
	em->status = EMSTOP;

	baskets[idx].port = *port;
	for (j = 0, i = 0; i < baskets[idx].colcount; i++) {
		b = baskets[idx].primary[j];
		if (b == NULL) {
			em->table.nr_attrs = j;   /* ensure a consistent structure*/
			throw(MAL, "receptor.new", "Could not access descriptor");
		}

		em->table.format[j].c[0] = BATcopy(b,b->htype, b->ttype,FALSE);;
		em->table.format[j].ci[0] = bat_iterator(b);
		em->table.format[j].name = GDKstrdup(baskets[idx].cols[i]);
		em->table.format[j].sep = GDKstrdup(",");
		em->table.format[j].seplen = (int)strlen(em->table.format[j].sep);
		em->table.format[j].type = ATOMname(b->ttype);
		em->table.format[j].adt = (b)->ttype;
		em->table.format[j].nullstr = GDKstrdup("");
		em->table.format[j].tostr = &TABLETadt_toStr;
		em->table.format[j].frstr = &TABLETadt_frStr;
		em->table.format[j].extra = em->table.format + j;
		em->table.format[j].len = em->table.format[j].nillen =
									  ATOMlen(em->table.format[j].adt, ATOMnilptr(em->table.format[j].adt));
		em->table.format[j].data = GDKmalloc(em->table.format[j].len);
		j++;
	}
	GDKfree(em->table.format[j-1].sep);
	em->table.format[j-1].sep = GDKstrdup("\n");
	em->table.format[j-1].seplen = (int)strlen(em->table.format[j-1].sep);
	em->table.nr_attrs = j;

	(void)ret;
#ifdef _DEBUG_EMITTER_
	mnstr_printf(EMout, "Instantiate a new emitter %d fields\n", i);
#endif
	return MAL_SUCCEED;
}

str DCemitterPause(int *ret, str *nme)
{
	Emitter em;

	em = EMfind(*nme);
	if (em == NULL)
		throw(MAL, "emitter.pause", "Emitter not defined");
	if (em->status == EMLISTEN)
		throw(MAL, "emitter.pause", "Emitter not started");

	em->status = EMPAUSE;

#ifdef _DEBUG_EMITTER_
	mnstr_printf(EMout, "Pause a emitter\n");
#endif
	(void)ret;
	return MAL_SUCCEED;
}

str DCemitterResume(int *ret, str *nme)
{
	Emitter em;

	(void) ret;
	em = EMfind(*nme);
	if (em == NULL)
		throw(MAL, "emitter.resume", "Emitter not defined");
	if (em->status == EMLISTEN)
		throw(MAL, "emitter.resume", "Emitter running, stop it first");
#ifdef _DEBUG_EMITTER_
	mnstr_printf(EMout, "resume an emitter\n");
#endif
	em->status = EMLISTEN;
	(void)ret;
	if ( MT_create_thread(&em->pid, (void (*)(void *))EMstartThread, em, MT_THR_DETACHED) != 0)
		throw(MAL, "emitter.start", "Emitter initiation failed");
	return MAL_SUCCEED;
}

str
EMresume(int *ret)
{
	Emitter em;
	for( em = emAnchor; em ; em= em->nxt)
	if (em ->status == EMSTOP)
		DCemitterResume(ret, &em->name);
	return MAL_SUCCEED;
}

str EMstop(int *ret, str *nme)
{
	Emitter em, rb;

	em = EMfind(*nme);
	if (em == NULL)
		throw(MAL, "emitter.drop", "Emitter not defined");
#ifdef _DEBUG_EMITTER_
	mnstr_printf(EMout, "Drop a emitter\n");
#endif
	(void)ret;
	if (emAnchor == em)
		emAnchor = em->nxt;
	rb = em->prv;
	if (em->nxt)
		em->nxt->prv = em->prv;
	if (rb)
		rb->nxt = em->nxt;
	em->status = EMDROP;
	if (em->lck)
		BSKTunlock(&em->lck, &em->name);
	MT_join_thread(em->pid);
	return MAL_SUCCEED;
}

str EMreset(int *ret)
{
	Emitter r,o;
	for( r= emAnchor; r; r = o){
		o= r->nxt;
		EMstop(ret, &r->name);
	}
	return MAL_SUCCEED;
}

str EMmode(int *ret, str *nme, str *arg)
{
	Emitter em;
	em = EMfind(*nme);
	if (em == NULL)
		throw(MAL, "emitter.mode", "Emitter not defined");
#ifdef _DEBUG_EMITTER_
	mnstr_printf(EMout, "#Define emitter mode\n");
#endif
	(void)ret;
	if ( strcmp(*arg,"passive") == 0 )
		em->mode = EMPASSIVE;
	else
	if ( strcmp(*arg,"active") == 0 )
		em->mode = EMACTIVE;
	else
		throw(MAL,"emitter.mode","Must be either passive/active");
	return MAL_SUCCEED;
}

str EMprotocol(int *ret, str *nme, str *mode)
{
	Emitter em;
	em = EMfind(*nme);
	if (em == NULL)
		throw(MAL, "emitter.protocol", "Emitter not defined");
#ifdef _DEBUG_EMITTER_
	mnstr_printf(EMout, "#Define emitter protocol\n");
#endif
	(void)ret;
	if ( strcmp(*mode,"udp") == 0 )
		em->protocol = UDP;
	else
	if ( strcmp(*mode,"tcp") == 0)
		em->protocol = TCP;
	else
	if ( strcmp(*mode,"csv") == 0)
		em->protocol = CSV;
	else
		throw(MAL,"emitter.protocol","Must be either udp/tcp/csv");
	return MAL_SUCCEED;
}


/*open a stream socket to a certain server:port*/


/*
 * @-
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
			mnstr_printf(EMout, "Emitter connect fails: %s\n", em->error);
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
	if ( em->status == EMDROP)
		return;
	/* create the actual channel */
	if (em->mode == EMACTIVE && em->protocol == UDP)
			em->emitter = udp_wastream(em->host, em->port, em->name);
	else
		em->emitter = socket_wastream(em->newsockfd, em->name);
	if (em->emitter == NULL) {
		perror("Emitter: Could not open stream");
		mnstr_printf(EMout, "stream %s.%d.%s\n", em->host, em->port, em->name);
		socket_close(em->newsockfd);
#ifdef _DEBUG_EMITTER_
		mnstr_printf(EMout, "Exit emitter body loop\n");
#endif
		return;
	}
	/*
	 * @-
	 * Consume each event and store the result.
	 * If the thread is suspended, we sleep for at least one second.
	 */
	for (ret = 1; ret >= 0; )
	{
		while (em->status == EMPAUSE) {
#ifdef _DEBUG_EMITTER_
			mnstr_printf(EMout, "pause emitter\n");
#endif
			MT_sleep_ms(em->delay);
		}
		if (em->status == EMSTOP)
			break;
		if (em->status == EMDROP) {
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
		for ( k =0 ; k < baskets[em->bskt].colcount; k++ ) {
			if ( em->table.format[k].c[0] )
				BBPunfix( em->table.format[k].c[0]->batCacheid );
			b = baskets[em->bskt].primary[k];
			em->table.format[k].c[0] =  BATcopy(b, b->htype, b->ttype,TRUE);
			em->table.format[k].ci[0] = bat_iterator(b);
			BATclear(b);
		}
		BSKTunlock(&em->lck, &em->name);
		if ((cnt = BATcount(em->table.format[0].c[0]))) {
			MTIMEcurrent_timestamp(&baskets[em->bskt].seen);
			baskets[em->bskt].events += cnt;
			baskets[em->bskt].grabs ++;
			if (em->status != EMLISTEN)
				break;

			cnt = BATcount(em->table.format[1].c[0]);
#ifdef _DEBUG_EMITTER_
			mnstr_printf(EMout, "Emit " BUNFMT " tuples \n", cnt);
#endif
			em->table.nr = cnt;

			ret = TABLEToutput_file(&em->table, em->table.format[1].c[0], em->emitter);
#ifdef _DEBUG_EMITTER_
			if (ret < 0)
				mnstr_printf(EMout, "Tuple emission failed\n");
#endif
			if (ret < 0)
				/* keep the events and try to setup a new connection */
				break;
			if (em->table.error) {
				mnstr_printf(GDKerr, em->table.error);
				em->table.error = 0;
			}
		} else
		if ( em->delay)
			MT_sleep_ms(em->delay);
	}
	/* writing failed, lets restart */
	if (em->mode == EMPASSIVE) {
#ifdef _DEBUG_EMITTER_
		mnstr_printf(EMout, "Restart the connection\n");
#endif
		if ( em->status != EMSTOP)
			EMreconnect(em);
		goto bodyRestart;
	}
#ifdef _DEBUG_EMITTER_
	mnstr_printf(EMout, "Exit emitter body loop\n");
#endif
	mnstr_close(em->emitter);
}

str
EMstartThread(Emitter em)
{
	GDKprotect();

#ifdef _DEBUG_EMITTER_
	mnstr_printf(EMout, "Emitter body %s started at %s:%d, servermode=%d\n",
		em->name, em->host, em->port, em->mode);
#endif
	if (em->mode == EMACTIVE) {
		EMbody(em);
#ifdef _DEBUG_EMITTER_
		mnstr_printf(EMout, "End of emitter thread\n");
#endif
		return MAL_SUCCEED;
	}

	/* Handling the TCP connection */
	if (em->mode == EMPASSIVE &&
		(em->error = socket_server_connect(&em->sockfd, em->port))) {
		em->status = EMERROR;
		mnstr_printf(EMout, "EMSTART THREAD: failed to start server:%s\n", em->error);
		return MAL_SUCCEED;
	}
	while (em->status != EMSTOP)
	{
		if (em->mode == EMPASSIVE) {
			/* in server mode you should expect new connections */
#ifdef _DEBUG_EMITTER_
			mnstr_printf(EMout, "Emitter listens\n");
#endif
			em->error = socket_server_listen(em->sockfd, &em->newsockfd);
			if (em->error) {
				em->status = EMERROR;
				mnstr_printf(EMout, "Emitter listen fails: %s\n", em->error);
			}

			if (MT_create_thread(&em->pid, (void (*)(void *))EMbody, em, MT_THR_DETACHED) != 0) {
				close_stream(em->emitter);
				throw(MAL, "emitter.start", "Process creation failed");
			}
		} else
		if ( em->mode == EMACTIVE) {
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
	mnstr_printf(GDKout,"#emitter %s at %s:%d protocol=%s mode=%s status=%s delay=%d\n",
		em->name, em->host, em->port, protocolname[em->protocol], modename[em->mode], statusname[em->status], em->delay);
}

str
EMdump()
{
	Emitter rc = emAnchor;
	for ( ; rc; rc= rc->nxt)
		dumpEmitter(rc);
	return MAL_SUCCEED;
}
