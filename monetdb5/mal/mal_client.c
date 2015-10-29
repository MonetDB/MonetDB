/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0.  If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 *
 * Copyright 2008-2015 MonetDB B.V.
 */

/*
 * Clients gain access to the Monet server through a internet connection
 * or through its server console.  Access through the internet requires
 * a client program at the source, which addresses the default port of a
 * running server.  The functionality of the server console is limited.
 * It is a textual interface for expert use.
 *
 * At the server side, each client is represented by a session record
 * with the current status, such as name, file descriptors, namespace,
 * and local stack.  Each client session has a dedicated thread of
 * control.
 *
 * The number of clients permitted concurrent access is a run time
 * option. The console is the first and is always present.  It reads
 * from standard input and writes to standard output.
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
#include "mal_readline.h"
#include "mal_import.h"
#include "mal_parser.h"
#include "mal_namespace.h"
#include "mal_private.h"
#include "mal_runtime.h"
#include <mapi.h> /* for PROMPT1 */


/*
 * This should be in src/mal/mal.h, as the function is implemented in
 * src/mal/mal.c; however, it cannot, as "Client" isn't known there ...
 * |-( For now, we move the prototype here, as it it only used here.
 * Maybe, we should consider also moving the implementation here...
 */

static void freeClient(Client c);

int MAL_MAXCLIENTS = 0;
ClientRec *mal_clients;
int MCdefault = 0;

void
MCinit(void)
{
	char *max_clients = GDKgetenv("max_clients");
	int maxclients = 0;

	if (max_clients != NULL)
		maxclients = atoi(max_clients);
	if (maxclients <= 0) {
		maxclients = 64;
		GDKsetenv("max_clients", "64");
	}

	MAL_MAXCLIENTS =
		/* console */ 1 +
		/* client connections */ maxclients;
	mal_clients = GDKzalloc(sizeof(ClientRec) * MAL_MAXCLIENTS);
	if( mal_clients == NULL){
		showException(GDKout, MAL, "MCinit",MAL_MALLOC_FAIL);
		mal_exit();
	}
}

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
		(void) bstream_destroy(c->fdin);
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
	MT_lock_set(&mal_contextLock, "newClient");
	if (mal_clients[CONSOLE].user && mal_clients[CONSOLE].mode == FINISHCLIENT) {
		/*system shutdown in progress */
		MT_lock_unset(&mal_contextLock, "newClient");
		return NULL;
	}
	for (c = mal_clients; c < mal_clients + MAL_MAXCLIENTS; c++) {
		if (c->mode == FREECLIENT) {
			c->mode = RUNCLIENT;
			break;
		}
	}
	MT_lock_unset(&mal_contextLock, "newClient");

	if (c == mal_clients + MAL_MAXCLIENTS)
		return NULL;
	c->idx = (int) (c - mal_clients);
#ifdef MAL_CLIENT_DEBUG
	printf("New client created %d\n", (int) (c - mal_clients));
#endif
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

void
MCexitClient(Client c)
{
#ifdef MAL_CLIENT_DEBUG
	printf("# Exit client %d\n", c->idx);
#endif
	finishSessionProfiler(c);
	MPresetProfiler(c->fdout);
	if (c->father == NULL) { /* normal client */
		if (c->fdout && c->fdout != GDKstdout) {
			(void) mnstr_close(c->fdout);
			(void) mnstr_destroy(c->fdout);
		}
		assert(c->bak == NULL);
		if (c->fdin) {
			/* missing protection against closing stdin stream */
			(void) bstream_destroy(c->fdin);
		}
		c->fdout = NULL;
		c->fdin = NULL;
	}
}

Client
MCinitClientRecord(Client c, oid user, bstream *fin, stream *fout)
{
	str prompt;

	c->user = user;
	c->scenario = NULL;
	c->oldscenario = NULL;
	c->srcFile = NULL;
	c->blkmode = 0;

	c->fdin = fin ? fin : bstream_create(GDKin, 0);
	c->yycur = 0;
	c->bak = NULL;

	c->listing = 0;
	c->fdout = fout ? fout : GDKstdout;
	c->mdb = 0;
	c->history = 0;
	c->curprg = c->backup = 0;
	c->glb = 0;

	/* remove garbage from previous connection */
	if (c->nspace) {
		freeModule(c->nspace);
		c->nspace = 0;
	}

	c->father = NULL;
	c->login = c->lastcmd = time(0);
	//c->active = 0;
	c->session = GDKusec();
	c->qtimeout = 0;
	c->stimeout = 0;
	c->stage = 0;
	c->itrace = 0;
	c->debugOptimizer = c->debugScheduler = 0;
	c->flags = MCdefault;
	c->errbuf = 0;

	prompt = !fin ? GDKgetenv("monet_prompt") : PROMPT1;
	c->prompt = GDKstrdup(prompt);
	c->promptlength = strlen(prompt);

	c->actions = 0;
	c->totaltime = 0;
	/* create a recycler cache */
	c->exception_buf_initialized = 0;
	c->error_row = c->error_fld = c->error_msg = c->error_input = NULL;
	MT_sema_init(&c->s, 0, "Client->s");
	return c;
}

Client
MCinitClient(oid user, bstream *fin, stream *fout)
{
	Client c = NULL;

	if ((c = MCnewClient()) == NULL)
		return NULL;
	return MCinitClientRecord(c, user, fin, fout);
}

/*
 * The administrator should be initialized to enable interpretation of
 * the command line arguments, before it starts serviceing statements
 */
int
MCinitClientThread(Client c)
{
	Thread t;
	char cname[11 + 1];

	snprintf(cname, 11, OIDFMT, c->user);
	cname[11] = '\0';
	t = THRnew(cname);
	if (t == 0) {
		showException(c->fdout, MAL, "initClientThread", "Failed to initialize client");
		MPresetProfiler(c->fdout);
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
			showException(GDKout, MAL, "initClientThread", "Failed to initialize client");
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
	if (father == NULL)
		return NULL;
	if (father->father != NULL)
		father = father->father;
	if ((son = MCinitClient(father->user, father->fdin, father->fdout))) {
		son->fdin = NULL;
		son->fdout = father->fdout;
		son->bak = NULL;
		son->yycur = 0;
		son->father = father;
		son->scenario = father->scenario;
		if (son->prompt)
			GDKfree(son->prompt);
		son->prompt = GDKstrdup(father->prompt);
		son->promptlength = strlen(father->prompt);
		/* reuse the scopes wherever possible */
		if (son->nspace == 0)
			son->nspace = newModule(NULL, putName("child", 5));
		son->nspace->outer = father->nspace->outer;
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
freeClient(Client c)
{
	Thread t = c->mythread;
	c->mode = FINISHCLIENT;

#ifdef MAL_CLIENT_DEBUG
	printf("# Free client %d\n", c->idx);
#endif
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
		GDKsetbuf(0);
		if (c->father == NULL)
			GDKfree(c->errbuf);
		c->errbuf = 0;
	}
	c->father = 0;
	c->login = c->lastcmd = 0;
	//c->active = 0;
	c->qtimeout = 0;
	c->stimeout = 0;
	c->user = oid_nil;
	c->mythread = 0;
	c->mode = MCshutdowninprogress()? BLOCKCLIENT: FREECLIENT;
	GDKfree(c->glb);
	c->glb = NULL;
	if( c->error_row){
		BBPdecref(c->error_row->batCacheid,TRUE);
		BBPdecref(c->error_fld->batCacheid,TRUE);
		BBPdecref(c->error_msg->batCacheid,TRUE);
		BBPdecref(c->error_input->batCacheid,TRUE);
		c->error_row = c->error_fld = c->error_msg = c->error_input = NULL;
	}
	if (t)
		THRdel(t);  /* you may perform suicide */
	MT_sema_destroy(&c->s);
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
static int shutdowninprogress = 0;

int
MCshutdowninprogress(void){
	return shutdowninprogress;
}

void
MCstopClients(Client cntxt)
{
	Client c = mal_clients;

	MT_lock_set(&mal_contextLock,"stopClients");
	for(c= mal_clients +1;  c < mal_clients+MAL_MAXCLIENTS; c++)
	if( cntxt != c){
		if ( c->mode == RUNCLIENT)
			c->mode = FINISHCLIENT; 
		else if (c->mode == FREECLIENT)
			c->mode = BLOCKCLIENT;
	}
	shutdowninprogress =1;
	MT_lock_unset(&mal_contextLock,"stopClients");
}

int
MCactiveClients(void)
{
	int freeclient=0, finishing=0, running=0, blocked = 0;
	Client cntxt = mal_clients;

	for(cntxt= mal_clients+1;  cntxt<mal_clients+MAL_MAXCLIENTS; cntxt++){
		freeclient += (cntxt->mode == FREECLIENT);
		finishing += (cntxt->mode == FINISHCLIENT);
		running += (cntxt->mode == RUNCLIENT);
		blocked += (cntxt->mode == BLOCKCLIENT);
	}
	return finishing+running;
}

void
MCcloseClient(Client c)
{
#ifdef MAL_DEBUG_CLIENT
	printf("closeClient %d " OIDFMT "\n", (int) (c - mal_clients), c->user);
#endif
	/* free resources of a single thread */
	if (!isAdministrator(c)) {
		freeClient(c);
		return;
	}

	/* adm is set to disallow new clients entering */
	mal_clients[CONSOLE].mode = FINISHCLIENT;
	mal_exit();
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

#ifdef MAL_CLIENT_DEBUG
	printf("# streamClient %d %d\n", c->idx, isa_block_stream(in->s));
#endif

	while (in->pos < in->len &&
		   (isspace((int) (in->buf[in->pos])) ||
			in->buf[in->pos] == ';' || !in->buf[in->pos]))
		in->pos++;

	if (in->pos >= in->len || in->mode) {
		ssize_t rd, sum = 0;

		if (in->eof || !isa_block_stream(c->fdout)) {
			if (!isa_block_stream(c->fdout) && c->promptlength > 0)
				mnstr_write(c->fdout, c->prompt, c->promptlength, 1);
			mnstr_flush(c->fdout);
			in->eof = 0;
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
#ifdef MAL_CLIENT_DEBUG
		printf("# simple stream received %d sum " SZFMT "\n", c->idx, sum);
#endif
	}
	if (in->pos >= in->len) {
		/* end of stream reached */
#ifdef MAL_CLIENT_DEBUG
		printf("# end of stream received %d %d\n", c->idx, c->bak == 0);
#endif
		if (c->bak) {
			MCpopClientInput(c);
			if (c->fdin == NULL)
				return 0;
			return MCreadClient(c);
		}
		return 0;
	}
	if (*CURRENT(c) == '?') {
		showHelp(c->nspace, CURRENT(c) + 1, c->fdout);
		in->pos = in->len;
		return MCreadClient(c);
	}
#ifdef MAL_CLIENT_DEBUG
	printf("# finished stream read %d %d\n", (int) in->pos, (int) in->len);
	printf("#%s\n", in->buf);
#endif
	return 1;
}
