/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0.  If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 *
 * Copyright 1997 - July 2008 CWI, August 2008 - 2021 MonetDB B.V.
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
#include "mal_runtime.h"
#include "mal_authorize.h"
#include "mapi_prompt.h"

int MAL_MAXCLIENTS = 0;
ClientRec *mal_clients = NULL;

void
mal_client_reset(void)
{
	MAL_MAXCLIENTS = 0;
	if (mal_clients) {
		GDKfree(mal_clients);
		mal_clients = NULL;
	}
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
			TRC_CRITICAL(MAL_SERVER, "Initialization failed: " MAL_MALLOC_FAIL "\n");
			return false;
		}
	}

	MAL_MAXCLIENTS = /* client connections */ maxclients;
	mal_clients = GDKzalloc(sizeof(ClientRec) * MAL_MAXCLIENTS);
	if( mal_clients == NULL){
		TRC_CRITICAL(MAL_SERVER, "Initialization failed: " MAL_MALLOC_FAIL "\n");
		return false;
	}
	for (int i = 0; i < MAL_MAXCLIENTS; i++){
		ATOMIC_INIT(&mal_clients[i].lastprint, 0);
	}
	return true;
}

/* stack the files from which you read */
int
MCpushClientInput(Client c, bstream *new_input, int listing, char *prompt)
{
	ClientInput *x = (ClientInput *) GDKmalloc(sizeof(ClientInput));
	if (x == 0)
		return -1;
	x->fdin = c->fdin;
	x->yycur = c->yycur;
	x->listing = c->listing;
	x->prompt = c->prompt;
	x->next = c->bak;
	c->bak = x;
	c->fdin = new_input;
	c->listing = listing;
	c->prompt = prompt ? GDKstrdup(prompt) : GDKstrdup("");
	if(c->prompt == 0) {
		GDKfree(x);
		return -1;
	}
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
	GDKfree(c->prompt);
	c->fdin = x->fdin;
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
	Client c;

	for (c = mal_clients; c < mal_clients + MAL_MAXCLIENTS; c++) {
		if (c->mode == FREECLIENT) {
			c->mode = RUNCLIENT;
			break;
		}
	}

	if (c == mal_clients + MAL_MAXCLIENTS)
		return NULL;
	c->idx = (int) (c - mal_clients);
	return c;
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
	if (id < 0 || id >= MAL_MAXCLIENTS)
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
	if (fdout != maleventstream)
		return;
	MT_lock_set(&mal_profileLock);
	maleventstream = 0;
	MT_lock_unset(&mal_profileLock);
}

void
MCexitClient(Client c)
{
	MCresetProfiler(c->fdout);
	if (c->father == NULL) { /* normal client */
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
	}
}

static Client
MCinitClientRecord(Client c, oid user, bstream *fin, stream *fout)
{
	const char *prompt;

	/* mal_contextLock is held when this is called */
	c->user = user;
	c->username = 0;
	c->scenario = NULL;
	c->oldscenario = NULL;
	c->srcFile = NULL;
	c->blkmode = 0;

	c->fdin = fin ? fin : bstream_create(GDKstdin, 0);
	if ( c->fdin == NULL){
		c->mode = FREECLIENT;
		TRC_ERROR(MAL_SERVER, "No stdin channel available\n");
		return NULL;
	}
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
	c->idle  = c->login = c->lastcmd = time(0);
	c->session = GDKusec();
	strcpy_len(c->optimizer, "default_pipe", sizeof(c->optimizer));
	c->workerlimit = 0;
	c->memorylimit = 0;
	c->querytimeout = 0;
	c->sessiontimeout = 0;
	c->itrace = 0;
	c->errbuf = 0;

	prompt = PROMPT1;
	c->prompt = GDKstrdup(prompt);
	if ( c->prompt == NULL){
		if (fin == NULL) {
			c->fdin->s = NULL;
			bstream_destroy(c->fdin);
			c->mode = FREECLIENT;
		}
		return NULL;
	}
	c->promptlength = strlen(prompt);

	c->actions = 0;
	c->profticks = c->profstmt = NULL;
	c->error_row = c->error_fld = c->error_msg = c->error_input = NULL;
	c->sqlprofiler = 0;
	c->wlc_kind = 0;
	c->wlc = NULL;
	/* no authentication in embedded mode */
	if (!GDKembedded()) {
		str msg = AUTHgetUsername(&c->username, c);
		if (msg)				/* shouldn't happen */
			freeException(msg);
	}
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
	if (c)
		c = MCinitClientRecord(c, user, fin, fout);
	MT_lock_unset(&mal_contextLock);
	return c;
}


/*
 * The administrator should be initialized to enable interpretation of
 * the command line arguments, before it starts servicing statements
 */
int
MCinitClientThread(Client c)
{
	Thread t;

	t = MT_thread_getdata();	/* should succeed */
	if (t == NULL) {
		MCresetProfiler(c->fdout);
		return -1;
	}
	/*
	 * The GDK thread administration should be set to reflect use of
	 * the proper IO descriptors.
	 */
	t->data[1] = c->fdin;
	t->data[0] = c->fdout;
	c->mythread = t;
	c->errbuf = GDKerrbuf;
	if (c->errbuf == NULL) {
		char *n = GDKzalloc(GDKMAXERRLEN);
		if ( n == NULL){
			MCresetProfiler(c->fdout);
			return -1;
		}
		GDKsetbuf(n);
		c->errbuf = GDKerrbuf;
	} else
		c->errbuf[0] = 0;
	return 0;
}

/*
 * Forking is a relatively cheap way to create a new client.  The new
 * client record shares the IO descriptors.  To avoid interference, we
 * limit children to only produce output by closing the input-side.
 *
 * If the father itself is a temporary client, let the new child depend
 * on the grandfather.
 */
Client
MCforkClient(Client father)
{
	Client son = NULL;
	str prompt;

	if (father == NULL)
		return NULL;
	if (father->father != NULL)
		father = father->father;
	if((prompt = GDKstrdup(father->prompt)) == NULL)
		return NULL;
	if ((son = MCinitClient(father->user, father->fdin, father->fdout))) {
		son->fdin = NULL;
		son->fdout = father->fdout;
		son->bak = NULL;
		son->yycur = 0;
		son->father = father;
		son->login = father->login;
		son->idle = father->idle;
		son->scenario = father->scenario;
		strcpy_len(father->optimizer, son->optimizer, sizeof(father->optimizer));
		son->workerlimit = father->workerlimit;
		son->memorylimit = father->memorylimit;
		son->querytimeout = father->querytimeout;
		son->sessiontimeout = father->sessiontimeout;

		if (son->prompt)
			GDKfree(son->prompt);
		son->prompt = prompt;
		son->promptlength = strlen(prompt);
		/* reuse the scopes wherever possible */
		if (son->usermodule == 0) {
			son->usermodule = userModule();
			if(son->usermodule == 0) {
				MCcloseClient(son);
				return NULL;
			}
		}
	} else {
		GDKfree(prompt);
	}
	return son;
}

/*
 * When a client needs to be terminated then the file descriptors for
 * its input/output are simply closed.  This leads to a graceful
 * degradation, but may take some time when the client is busy.  A more
 * forcefull method is to kill the client thread, but this may leave
 * locks and semaphores in an undesirable state.
 *
 * The routine freeClient ends a single client session, but through side
 * effects of sharing IO descriptors, also its children. Conversely, a
 * child can not close a parent.
 */
void
MCfreeClient(Client c)
{
	if( c->mode == FREECLIENT)
		return;
	c->mode = FINISHCLIENT;

	MCexitClient(c);

	/* scope list and curprg can not be removed, because the client may
	 * reside in a quit() command. Therefore the scopelist is re-used.
	 */
	c->scenario = NULL;
	if (c->prompt)
		GDKfree(c->prompt);
	c->prompt = NULL;
	c->promptlength = -1;
	if (c->errbuf) {
		/* no client threads in embedded mode */
		//if (!GDKembedded())
		GDKsetbuf(0);
		if (c->father == NULL)
			GDKfree(c->errbuf);
		c->errbuf = 0;
	}
	if (c->usermodule)
		freeModule(c->usermodule);
	c->usermodule = c->curmodule = 0;
	c->father = 0;
	c->idle = c->login = c->lastcmd = 0;
	strcpy_len(c->optimizer, "default_pipe", sizeof(c->optimizer));
	c->workerlimit = 0;
	c->memorylimit = 0;
	c->querytimeout = 0;
	c->sessiontimeout = 0;
	c->user = oid_nil;
	if( c->username){
		GDKfree(c->username);
		c->username = 0;
	}
	c->mythread = 0;
	if (c->glb) {
		freeStack(c->glb);
		c->glb = NULL;
	}
	if( c->profticks){
		BBPunfix(c->profticks->batCacheid);
		BBPunfix(c->profstmt->batCacheid);
		c->profticks = c->profstmt = NULL;
	}
	if( c->error_row){
		BBPunfix(c->error_row->batCacheid);
		BBPunfix(c->error_fld->batCacheid);
		BBPunfix(c->error_msg->batCacheid);
		BBPunfix(c->error_input->batCacheid);
		c->error_row = c->error_fld = c->error_msg = c->error_input = NULL;
	}
	if( c->wlc)
		freeMalBlk(c->wlc);
	c->sqlprofiler = 0;
	c->wlc_kind = 0;
	c->wlc = NULL;
	free(c->handshake_options);
	c->handshake_options = NULL;
	MT_sema_destroy(&c->s);
	c->mode = MCshutdowninprogress()? BLOCKCLIENT: FREECLIENT;
}

/*
 * If a client disappears from the scene (eof on stream), we should
 * terminate all its children. This is in principle a forcefull action,
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
static volatile int shutdowninprogress = 0;

int
MCshutdowninprogress(void){
	return shutdowninprogress;
}

void
MCstopClients(Client cntxt)
{
	Client c = mal_clients;

	MT_lock_set(&mal_contextLock);
	for(c = mal_clients;  c < mal_clients+MAL_MAXCLIENTS; c++)
	if (cntxt != c){
		if (c->mode == RUNCLIENT)
			c->mode = FINISHCLIENT;
		else if (c->mode == FREECLIENT)
			c->mode = BLOCKCLIENT;
	}
	shutdowninprogress =1;
	MT_lock_unset(&mal_contextLock);
}

int
MCactiveClients(void)
{
	int active = 0;
	Client cntxt = mal_clients;

	for(cntxt = mal_clients;  cntxt<mal_clients+MAL_MAXCLIENTS; cntxt++){
		active += (cntxt->idle == 0 && cntxt->mode == RUNCLIENT);
	}
	return active;
}

void
MCcloseClient(Client c)
{
	/* free resources of a single thread */
	MCfreeClient(c);
}

str
MCsuspendClient(int id)
{
	if (id < 0 || id >= MAL_MAXCLIENTS)
		throw(INVCRED, "mal.clients", INVCRED_WRONG_ID);
	mal_clients[id].itrace = 'S';
	return MAL_SUCCEED;
}

str
MCawakeClient(int id)
{
	if (id < 0 || id >= MAL_MAXCLIENTS)
		throw(INVCRED, "mal.clients", INVCRED_WRONG_ID);
	mal_clients[id].itrace = 0;
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
		ssize_t rd, sum = 0;

		if (in->eof || !isa_block_stream(c->fdout)) {
			if (!isa_block_stream(c->fdout) && c->promptlength > 0)
				mnstr_write(c->fdout, c->prompt, c->promptlength, 1);
			mnstr_flush(c->fdout, MNSTR_FLUSH_DATA);
			in->eof = false;
		}
		while ((rd = bstream_next(in)) > 0 && !in->eof) {
			sum += rd;
			if (!in->mode) /* read one line at a time in line mode */
				break;
		}
		if (in->mode) { /* find last new line */
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
	Client c;
	if (tc == NULL) {
		return 0;
	}
	MT_lock_set(&mal_contextLock);
	for (c = mal_clients; c < mal_clients + MAL_MAXCLIENTS; c++) {
		if (c == tc && c->mode == RUNCLIENT) {
			MT_lock_unset(&mal_contextLock);
			return 1;
		}
	}
	MT_lock_unset(&mal_contextLock);
	return 0;
}
