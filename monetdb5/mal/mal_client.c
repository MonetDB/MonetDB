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
 * Clients gain access to the Monet server through a internet connection.
 * Access through the internet requires a client program at the source,
 * which addresses the default port of a running server. It is a textual
 * interface for expert use.
 *
 * At the server side, each client is represented by a session record
 * with the current status, such as name, file descriptors, namespace,
 * and local stack.  Each client session has a dedicated thread of
 * control.
 *
 * The number of clients permitted concurrent access is a run time
 * option.
 *
 * Client sessions remain in existence until the corresponding
 * communication channels break.
 *
 * A client record is initialized upon acceptance of a connection.  The
 * client runs in his own thread of control until it finds a
 * soft-termination request mode (FINISHCLIENT) or its IO file descriptors
 * are closed. The latter generates an IO error, which leads to a safe
 * termination.
 *
 * The system administrator client runs in the primary thread of control
 * to simplify debugging with external debuggers.
 *
 * Searching a free client record is encapsulated in a critical section
 * to hand them out one-at-a-time.  Marking them as being claimed avoids
 * any interference from parallel actions to obtain client records.
 */

/* (author) M.L. Kersten */
#include "monetdb_config.h"
#include "mal_client.h"
#include "mal_import.h"
#include "mal_parser.h"
#include "mal_namespace.h"
#include "mal_private.h"
#include "mal_internal.h"
#include "mal_interpreter.h"
#include "mal_runtime.h"
#include "mal_authorize.h"
#include "mapi_prompt.h"

int MAL_MAXCLIENTS = 0;
ClientRec *mal_clients = NULL;

void
mal_client_reset(void)
{
	if (mal_clients) {
		GDKfree(mal_clients);
		mal_clients = NULL;
	}
	MAL_MAXCLIENTS = 0;
}

bool
MCinit(void)
{
	const char *max_clients = GDKgetenv("max_clients");
	int maxclients = 0;

	if (max_clients != NULL)
		maxclients = atoi(max_clients);
	if (maxclients <= 0) {
		maxclients = 64;
		if (GDKsetenv("max_clients", "64") != GDK_SUCCEED) {
			TRC_CRITICAL(MAL_SERVER,
						 "Initialization failed: " MAL_MALLOC_FAIL "\n");
			return false;
		}
	}

	MAL_MAXCLIENTS = /* client connections */ maxclients;
	mal_clients = GDKzalloc(sizeof(ClientRec) * MAL_MAXCLIENTS);
	if (mal_clients == NULL) {
		TRC_CRITICAL(MAL_SERVER,
					 "Initialization failed: " MAL_MALLOC_FAIL "\n");
		return false;
	}
	for (int i = 0; i < MAL_MAXCLIENTS; i++) {
		ATOMIC_INIT(&mal_clients[i].lastprint, 0);
		ATOMIC_INIT(&mal_clients[i].workers, 1);
		ATOMIC_INIT(&mal_clients[i].qryctx.datasize, 0);
		mal_clients[i].idx = -1;	/* indicate it's available */
	}
	return true;
}

/* stack the files from which you read */
int
MCpushClientInput(Client c, bstream *new_input, int listing, const char *prompt)
{
	ClientInput *x = (ClientInput *) GDKmalloc(sizeof(ClientInput));
	if (x == 0)
		return -1;
	*x = (ClientInput) {
		.fdin = c->fdin,
		.yycur = c->yycur,
		.listing = c->listing,
		.prompt = c->prompt,
		.next = c->bak,
	};
	c->bak = x;
	c->fdin = new_input;
	c->qryctx.bs = new_input;
	c->listing = listing;
	c->prompt = prompt ? prompt : "";
	c->promptlength = strlen(c->prompt);
	c->yycur = 0;
	return 0;
}

void
MCpopClientInput(Client c)
{
	ClientInput *x = c->bak;
	if (c->fdin) {
		/* missing protection against closing stdin stream */
		bstream_destroy(c->fdin);
	}
	c->fdin = x->fdin;
	c->qryctx.bs = c->fdin;
	c->yycur = x->yycur;
	c->listing = x->listing;
	c->prompt = x->prompt;
	c->promptlength = strlen(c->prompt);
	c->bak = x->next;
	GDKfree(x);
}

static Client
MCnewClient(void)
{
	for (Client c = mal_clients; c < mal_clients + MAL_MAXCLIENTS; c++) {
		if (c->idx == -1) {
			assert(c->mode == FREECLIENT);
			c->mode = RUNCLIENT;
			c->idx = (int) (c - mal_clients);
			return c;
		}
	}

	return NULL;
}

/*
 * You can always retrieve a client record using the thread identifier,
 * because we maintain a 1-1 mapping between client and thread of
 * control.  Therefore, we don't need locks either.
 * If the number of clients becomes too large, we have to change the
 * allocation and lookup scheme.
 *
 * Finding a client record is tricky when we are spawning threads as
 * co-workers. It is currently passed as an argument.
 */

Client
MCgetClient(int id)
{
	if (id <0 || id >=MAL_MAXCLIENTS)
		return NULL;
	return mal_clients + id;
}

/*
 * The resetProfiler is called when the owner of the event stream
 * leaves the scene. (Unclear if parallelism may cause errors)
 */

static void
MCresetProfiler(stream *fdout)
{
	MT_lock_set(&mal_profileLock);
	if (fdout == maleventstream) {
		maleventstream = NULL;
		profilerStatus = 0;
		profilerMode = 0;
	}
	MT_lock_unset(&mal_profileLock);
}

static void
MCexitClient(Client c)
{
	MCresetProfiler(c->fdout);
	// Remove any left over constant symbols
	if (c->curprg)
		resetMalBlk(c->curprg->def);
	if (c->father == NULL) {	/* normal client */
		if (c->fdout && c->fdout != GDKstdout)
			close_stream(c->fdout);
		assert(c->bak == NULL);
		if (c->fdin) {
			/* protection against closing stdin stream */
			if (c->fdin->s == GDKstdin)
				c->fdin->s = NULL;
			bstream_destroy(c->fdin);
		}
		c->fdout = NULL;
		c->fdin = NULL;
		c->qryctx.bs = NULL;
	}
	assert(c->query == NULL);
	if (profilerStatus > 0) {
		lng Tend = GDKusec();
		profilerEvent(NULL,
					  &(struct NonMalEvent)
					  { CLIENT_END, c, Tend, NULL, NULL, 0,
					  Tend - (c->session) });
	}
}

static Client
MCinitClientRecord(Client c, oid user, bstream *fin, stream *fout)
{
	/* mal_contextLock is held when this is called */
	c->user = user;
	c->username = 0;
	c->scenario = NULL;
	c->srcFile = NULL;
	c->blkmode = 0;

	c->fdin = fin ? fin : bstream_create(GDKstdin, 0);
	if (c->fdin == NULL) {
		c->mode = FREECLIENT;
		c->idx = -1;
		TRC_ERROR(MAL_SERVER, "No stdin channel available\n");
		return NULL;
	}
	c->qryctx.bs = c->fdin;
	c->yycur = 0;
	c->bak = NULL;

	c->listing = 0;
	c->fdout = fout ? fout : GDKstdout;
	c->curprg = c->backup = 0;
	c->glb = 0;

	/* remove garbage from previous connection
	 * be aware, a user can introduce several modules
	 * that should be freed to avoid memory leaks */
	c->usermodule = c->curmodule = 0;

	c->father = NULL;
	c->idle = c->login = c->lastcmd = time(0);
	c->session = GDKusec();
	strcpy_len(c->optimizer, "default_pipe", sizeof(c->optimizer));
	c->workerlimit = 0;
	c->memorylimit = 0;
	c->querytimeout = 0;
	c->sessiontimeout = 0;
	c->logical_sessiontimeout = 0;
	c->qryctx.starttime = 0;
	c->qryctx.endtime = 0;
	ATOMIC_SET(&c->qryctx.datasize, 0);
	c->qryctx.maxmem = 0;
	c->maxmem = 0;
	c->errbuf = 0;

	c->prompt = PROMPT1;
	c->promptlength = strlen(c->prompt);

	c->profticks = c->profstmt = c->profevents = NULL;
	c->error_row = c->error_fld = c->error_msg = c->error_input = NULL;
	c->sqlprofiler = 0;
	c->blocksize = BLOCK;
	c->protocol = PROTOCOL_9;

	c->filetrans = false;
	c->handshake_options = NULL;
	c->query = NULL;

	char name[MT_NAME_LEN];
	snprintf(name, sizeof(name), "Client%d->s", (int) (c - mal_clients));
	MT_sema_init(&c->s, 0, name);
	return c;
}

Client
MCinitClient(oid user, bstream *fin, stream *fout)
{
	Client c = NULL;

	MT_lock_set(&mal_contextLock);
	c = MCnewClient();
	if (c) {
		c = MCinitClientRecord(c, user, fin, fout);
		MT_thread_set_qry_ctx(&c->qryctx);
	}
	MT_lock_unset(&mal_contextLock);

	if (c && profilerStatus > 0)
		profilerEvent(NULL,
					  &(struct NonMalEvent)
					  { CLIENT_START, c, c->session, NULL, NULL, 0, 0 }
	);
	return c;
}


/*
 * The administrator should be initialized to enable interpretation of
 * the command line arguments, before it starts servicing statements
 */
int
MCinitClientThread(Client c)
{
	/*
	 * The GDK thread administration should be set to reflect use of
	 * the proper IO descriptors.
	 */
	c->mythread = MT_thread_getname();
	c->errbuf = GDKerrbuf;
	if (c->errbuf == NULL) {
		char *n = GDKzalloc(GDKMAXERRLEN);
		if (n == NULL) {
			MCresetProfiler(c->fdout);
			return -1;
		}
		GDKsetbuf(n);
		c->errbuf = GDKerrbuf;
	} else
		c->errbuf[0] = 0;
	return 0;
}

static bool shutdowninprogress = false;

bool
MCshutdowninprogress(void)
{
	MT_lock_set(&mal_contextLock);
	bool ret = shutdowninprogress;
	MT_lock_unset(&mal_contextLock);
	return ret;
}

/*
 * When a client needs to be terminated then the file descriptors for
 * its input/output are simply closed.  This leads to a graceful
 * degradation, but may take some time when the client is busy.  A more
 * forceful method is to kill the client thread, but this may leave
 * locks and semaphores in an undesirable state.
 *
 * The routine freeClient ends a single client session, but through side
 * effects of sharing IO descriptors, also its children. Conversely, a
 * child can not close a parent.
 */
void
MCcloseClient(Client c)
{
	MT_lock_set(&mal_contextLock);
	if (c->mode == FREECLIENT) {
		assert(c->idx == -1);
		MT_lock_unset(&mal_contextLock);
		return;
	}
	c->mode = FINISHCLIENT;
	MT_lock_unset(&mal_contextLock);

	MCexitClient(c);

	/* scope list and curprg can not be removed, because the client may
	 * reside in a quit() command. Therefore the scopelist is re-used.
	 */
	c->scenario = NULL;
	c->prompt = NULL;
	c->promptlength = -1;
	if (c->errbuf) {
		/* no client threads in embedded mode */
		GDKsetbuf(NULL);
		if (c->father == NULL)
			GDKfree(c->errbuf);
		c->errbuf = NULL;
	}
	if (c->usermodule)
		freeModule(c->usermodule);
	c->usermodule = c->curmodule = 0;
	c->father = 0;
	strcpy_len(c->optimizer, "default_pipe", sizeof(c->optimizer));
	c->workerlimit = 0;
	c->memorylimit = 0;
	c->querytimeout = 0;
	c->qryctx.endtime = 0;
	c->sessiontimeout = 0;
	c->logical_sessiontimeout = 0;
	c->user = oid_nil;
	if (c->username) {
		GDKfree(c->username);
		c->username = 0;
	}
	if (c->peer) {
		GDKfree(c->peer);
		c->peer = 0;
	}
	if (c->client_hostname) {
		GDKfree(c->client_hostname);
		c->client_hostname = 0;
	}
	if (c->client_application) {
		GDKfree(c->client_application);
		c->client_application = 0;
	}
	if (c->client_library) {
		GDKfree(c->client_library);
		c->client_library = 0;
	}
	if (c->client_remark) {
		GDKfree(c->client_remark);
		c->client_remark = 0;
	}
	c->client_pid = 0;
	c->mythread = NULL;
	if (c->glb) {
		freeStack(c->glb);
		c->glb = NULL;
	}
	if (c->profticks) {
		BBPunfix(c->profticks->batCacheid);
		BBPunfix(c->profstmt->batCacheid);
		BBPunfix(c->profevents->batCacheid);
		c->profticks = c->profstmt = c->profevents = NULL;
	}
	if (c->error_row) {
		BBPunfix(c->error_row->batCacheid);
		BBPunfix(c->error_fld->batCacheid);
		BBPunfix(c->error_msg->batCacheid);
		BBPunfix(c->error_input->batCacheid);
		c->error_row = c->error_fld = c->error_msg = c->error_input = NULL;
	}
	c->sqlprofiler = 0;
	free(c->handshake_options);
	c->handshake_options = NULL;
	MT_thread_set_qry_ctx(NULL);
	assert(c->qryctx.datasize == 0);
	MT_sema_destroy(&c->s);
	MT_lock_set(&mal_contextLock);
	c->idle = c->login = c->lastcmd = 0;
	if (shutdowninprogress) {
		c->mode = BLOCKCLIENT;
	} else {
		c->mode = FREECLIENT;
		c->idx = -1;
	}
	MT_lock_unset(&mal_contextLock);
}

/*
 * If a client disappears from the scene (eof on stream), we should
 * terminate all its children. This is in principle a forceful action,
 * because the children may be ignoring the primary IO streams.
 * (Instead they may be blocked in an infinite loop)
 *
 * Special care should be taken by closing the 'adm' thread.  It is
 * permitted to leave only when it is the sole user of the system.
 *
 * Furthermore, once we enter closeClient, the process in which it is
 * raised has already lost its file descriptors.
 *
 * When the server is about to shutdown, we should softly terminate
 * all outstanding session.
 */
void
MCstopClients(Client cntxt)
{
	MT_lock_set(&mal_contextLock);
	for (int i = 0; i < MAL_MAXCLIENTS; i++) {
		Client c = mal_clients + i;
		if (cntxt != c) {
			if (c->mode == RUNCLIENT)
				c->mode = FINISHCLIENT;
			else if (c->mode == FREECLIENT) {
				assert(c->idx == -1);
				c->idx = i;
				c->mode = BLOCKCLIENT;
			}
		}
	}
	shutdowninprogress = true;
	MT_lock_unset(&mal_contextLock);
}

int
MCactiveClients(void)
{
	int active = 0;

	MT_lock_set(&mal_contextLock);
	for (Client cntxt = mal_clients; cntxt < mal_clients + MAL_MAXCLIENTS;
		 cntxt++) {
		active += (cntxt->idle == 0 && cntxt->mode == RUNCLIENT);
	}
	MT_lock_unset(&mal_contextLock);
	return active;
}

str
MCsuspendClient(int id)
{
	if (id <0 || id >=MAL_MAXCLIENTS)
		throw(INVCRED, "mal.clients", INVCRED_WRONG_ID);
	return MAL_SUCCEED;
}

str
MCawakeClient(int id)
{
	if (id <0 || id >=MAL_MAXCLIENTS)
		throw(INVCRED, "mal.clients", INVCRED_WRONG_ID);
	return MAL_SUCCEED;
}

/*
 * Input to be processed is collected in a Client specific buffer.  It
 * is filled by reading information from a stream, a terminal, or by
 * scheduling strings constructed internally.  The latter involves
 * removing any escape character needed to manipulate the string within
 * the kernel.  The buffer space is automatically expanded to
 * accommodate new information and the read pointers are adjusted.
 *
 * The input is read from a (blocked) stream and stored in the client
 * record input buffer. The storage area grows automatically upon need.
 * The origin of the input stream depends on the connectivity mode.
 *
 * Each operation received from a front-end consists of at least one
 * line.  To simplify misaligned communication with front-ends, we use
 * different prompts structures.
 *
 * The default action is to read information from an ascii-stream one
 * line at a time. This is the preferred mode for reading from terminal.
 *
 * The next statement block is to be read. Send a prompt to warn the
 * front-end to issue the request.
 */
int
MCreadClient(Client c)
{
	bstream *in = c->fdin;

	while (in->pos < in->len &&
		   (isspace((unsigned char) (in->buf[in->pos])) ||
			in->buf[in->pos] == ';' || !in->buf[in->pos]))
		in->pos++;

	if (in->pos >= in->len || in->mode) {
		ssize_t rd;

		if (in->eof || !isa_block_stream(c->fdout)) {
			if (!isa_block_stream(c->fdout) && c->promptlength > 0)
				mnstr_write(c->fdout, c->prompt, c->promptlength, 1);
			mnstr_flush(c->fdout, MNSTR_FLUSH_DATA);
			in->eof = false;
		}
		while ((rd = bstream_next(in)) > 0 && !in->eof) {
			if (!in->mode)		/* read one line at a time in line mode */
				break;
		}
		if (rd < 0) {
			/* force end of stream handling below */
			in->pos = in->len;
		} else if (in->mode) {			/* find last new line */
			char *p = in->buf + in->len - 1;

			while (p > in->buf && *p != '\n') {
				*(p + 1) = *p;
				p--;
			}
			if (p > in->buf)
				*(p + 1) = 0;
			if (p != in->buf + in->len - 1)
				in->len++;
		}
	}
	if (in->pos >= in->len) {
		/* end of stream reached */
		if (c->bak) {
			MCpopClientInput(c);
			if (c->fdin == NULL)
				return 0;
			return MCreadClient(c);
		}
		return 0;
	}
	return 1;
}

int
MCvalid(Client tc)
{
	if (tc == NULL) {
		return 0;
	}
	MT_lock_set(&mal_contextLock);
	for (Client c = mal_clients; c < mal_clients + MAL_MAXCLIENTS; c++) {
		if (c == tc && c->mode == RUNCLIENT) {
			MT_lock_unset(&mal_contextLock);
			return 1;
		}
	}
	MT_lock_unset(&mal_contextLock);
	return 0;
}

void
MCsetClientInfo(Client c, const char *property, const char *value)
{
	if (strlen(property) < 7)
		return;

	// 012345 6 78...
	// Client H ostname
	// Applic a tionName
	// Client L ibrary
	// Client R emark
	// Client P id
	int discriminant = toupper(property[6]);

	switch (discriminant) {
		case 'H':
			if (strcasecmp(property, "ClientHostname") == 0) {
				GDKfree(c->client_hostname);
				c->client_hostname = value ? GDKstrdup(value) : NULL;
			}
			break;
		case 'A':
			if (strcasecmp(property, "ApplicationName") == 0) {
				GDKfree(c->client_application);
				c->client_application = value ? GDKstrdup(value) : NULL;
			}
			break;
		case 'L':
			if (strcasecmp(property, "ClientLibrary") == 0) {
				GDKfree(c->client_library);
				c->client_library = value ? GDKstrdup(value) : NULL;
			}
			break;
		case 'R':
			if (strcasecmp(property, "ClientRemark") == 0) {
				GDKfree(c->client_remark);
				c->client_remark = value ? GDKstrdup(value) : NULL;
			}
			break;
		case 'P':
			if (strcasecmp(property, "ClientPid") == 0 && value != NULL) {
				char *end;
				long n = strtol(value, &end, 10);
				if (*value && !*end)
					c->client_pid = n;
			}
			break;
		default:
			break;
	}
}
