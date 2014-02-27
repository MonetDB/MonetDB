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
 * Martin Kersten
 * DataCell Receptor
 *
 * Each receptor is supported by an independent thread
 * that reads the input and stores the data in a container
 * composed of a series of event baskets.
 *
 * The critical issue is that the receptor should hand over
 * the events to the main thread in a safe/secure way.
 * The overhead should be kept to an absolute minimum.
 * Currently it is regulated using a simple locking
 * scheme for the baskets.
 *
 * The event format is currently strict and relies on the tablet
 * module to parse them.
 */

#include "monetdb_config.h"
#include "receptor.h"
#include "dcsocket.h"
#include "stream_socket.h"
#include "mal_builder.h"

/* #define _DEBUG_RECEPTOR_  */

/* default settings */
#define RCHOST "localhost"
#define RCPORT 55000


static Receptor rcAnchor = NULL;
static str rcError = NULL;
static int rcErrorEvent = 0;

static str RCstartThread(Receptor rc);
static void RCscenarioInternal(Receptor rc);
static void RCgeneratorInternal(Receptor rc);

static Receptor
RCnew(str nme)
{
	Receptor rc;

	rc = (Receptor) GDKzalloc(sizeof(RCrecord));
	if (rc == 0)
		return rc;
	rc->name = GDKstrdup(nme);
	if (rcAnchor)
		rcAnchor->prv = rc;
	rc->nxt = rcAnchor;
	rc->prv = NULL;
	rcAnchor = rc;
	return rc;
}

Receptor
RCfind(str nme)
{
	Receptor r;
	char buf[BUFSIZ];
	for (r = rcAnchor; r; r = r->nxt)
		if (strcmp(nme, r->name) == 0)
			return r;
	snprintf(buf, BUFSIZ, "datacell.%s", nme);
	BSKTtolower(buf);
	for (r = rcAnchor; r; r = r->nxt)
		if (strcmp(buf, r->name) == 0)
			return r;
	return NULL;
}
/*
 * The MAL interface for managing the receptor pool
 * The basket should already be defined. Their order
 * is used to interpret the messages received.
 * The standard tuple layout for MonetDB interaction is used.
 */
static str
RCreceptorStartInternal(int *ret, str *tbl, str *host, int *port, int mode, int protocol, int delay)
{
	Receptor rc;
	int idx, i, j, len;
	Column *fmt;
	BAT *b;

	if (RCfind(*tbl))
		throw(MAL, "receptor.new", "Duplicate receptor '%s'", *tbl);
	idx = BSKTlocate(*tbl);
	if (idx == 0) /* should not happen */
		throw(MAL, "receptor.new", "Basket '%s' not found", *tbl);
	for (rc = rcAnchor; rc; rc = rc->nxt)
		if (rc->port == *port)
			throw(MAL, "receptor.new", "Port '%d' already in use",rc->port);

	rc = RCnew(*tbl);
	if (rc == 0)
		throw(MAL, "receptor.new", MAL_MALLOC_FAIL);
	rc->host = GDKstrdup(*host);
	rc->port = *port;
	rc->error = NULL;
	rc->delay = delay;
	rc->lck = 0;
	rc->status = BSKTPAUSE;
	rc->scenario = 0;
	rc->sequence = 0;
	rc->modnme = 0;
	rc->fcnnme = 0;
	rc->mode = mode;
	rc->protocol = protocol;
	rc->lastseen = *timestamp_nil;

	rc->bskt = idx;
	len = BSKTmemberCount(*tbl);
	fmt = rc->table.format = GDKzalloc(sizeof(Column) * len);

	for (j = 0, i = 0; i < baskets[idx].colcount; i++) {
		b = baskets[idx].primary[j];
		if (b == NULL) {
			rc->table.nr_attrs = j;   /* ensure a consistent structure*/
			throw(MAL, "receptor.new", "Could not access descriptor");
		}
		BBPincref(b->batCacheid, TRUE);
		fmt[j].c[0] = b;
		fmt[j].name = baskets[idx].cols[i];
		fmt[j].sep = ",";
		fmt[j].seplen = 1;
		fmt[j].type = GDKstrdup(ATOMname(b->ttype));
		fmt[j].adt = (b)->ttype;
		fmt[j].tostr = &TABLETadt_toStr;
		fmt[j].frstr = &TABLETadt_frStr;
		fmt[j].extra = fmt + j;
		fmt[j].len = fmt[j].nillen =
						 ATOMlen(fmt[j].adt, ATOMnilptr(fmt[j].adt));
		fmt[j].data = GDKmalloc(fmt[j].len);
		fmt[j].nullstr = "";
		j++;
	}
	rc->table.nr_attrs = j;

#ifdef _DEBUG_RECEPTOR_
	mnstr_printf(RCout, "#Instantiate a new receptor %d fields\n", j);
#endif
	if (MT_create_thread(&rc->pid, (void (*)(void *))RCstartThread, rc, MT_THR_DETACHED) != 0)
		throw(MAL, "receptor.start", "Receptor '%s' initiation failed", rc->name);
	(void) ret;
	return MAL_SUCCEED;
}
str
RCreceptorStart(int *ret, str *tbl, str *host, int *port)
{
	return RCreceptorStartInternal(ret, tbl, host, port, BSKTPASSIVE, TCP, PAUSEDEFAULT);
}

str
RCreceptorPause(int *ret, str *nme)
{
	Receptor rc;

	rc = RCfind(*nme);
	if (rc == NULL)
		throw(MAL, "receptor.resume", "Receptor '%s' not defined",*nme);
	rc->status = BSKTPAUSE;

#ifdef _DEBUG_RECEPTOR_
	mnstr_printf(RCout, "#Pause receptor %s\n", *nme);
#endif
	(void) ret;
	return MAL_SUCCEED;
}

str
RCreceptorResume(int *ret, str *nme)
{
	Receptor rc;

	rc = RCfind(*nme);
	if (rc == NULL)
		throw(MAL, "receptor.resume", "Receptor '%s' not defined",*nme);
	rc->status = BSKTRUNNING;

#ifdef _DEBUG_RECEPTOR_
	mnstr_printf(RCout, "#Resume receptor %s\n", *nme);
#endif
	(void) ret;
	return MAL_SUCCEED;
}

str
RCpause(int *ret)
{
	Receptor rc;
	str msg = MAL_SUCCEED;
	for (rc = rcAnchor; rc && msg == MAL_SUCCEED; rc = rc->nxt)
		if (rc->status == BSKTRUNNING)
			msg = RCreceptorPause(ret, &rc->name);
	return msg;
}

str
RCresume(int *ret)
{
	Receptor rc;
	str msg = MAL_SUCCEED;
	for (rc = rcAnchor; rc && msg == MAL_SUCCEED; rc = rc->nxt)
		if (rc->status == BSKTPAUSE)
			msg = RCreceptorResume(ret, &rc->name);
	return msg;
}

str RCreceptorStop(int *ret, str *nme)
{
	Receptor rc, rb;

	rc = RCfind(*nme);
	if (rc == NULL)
		throw(MAL, "receptor.drop", "Receptor '%s' not defined", *nme);
#ifdef _DEBUG_RECEPTOR_
	mnstr_printf(RCout, "#Drop a receptor\n");
#endif
	(void) ret;
	if (rcAnchor == rc)
		rcAnchor = rc->nxt;
	rb = rc->prv;
	if (rc->nxt)
		rc->nxt->prv = rc->prv;
	if (rb)
		rb->nxt = rc->nxt;
	rc->status = BSKTINIT;
	if (rc->lck)
		BSKTunlock(&rc->lck, &rc->name);
	MT_join_thread(rc->pid);
	return MAL_SUCCEED;
}

str
RCstop(int *ret)
{
	Receptor r, o;
	for (r = rcAnchor; r; r = o) {
		o = r->nxt;
		RCreceptorStop(ret, &r->name);
	}
	return MAL_SUCCEED;
}

str
RCscenario(int *ret, str *nme, str *fname, int *seq)
{
	Receptor rc;
	rc = RCfind(*nme);
	if (rc == NULL)
		throw(MAL, "receptor.scenario", "Receptor '%s' not defined",*nme);
#ifdef _DEBUG_RECEPTOR_
	mnstr_printf(RCout, "#Define receptor scenario\n");
#endif
	(void) ret;
	rc->scenario = GDKstrdup(*fname);
	rc->sequence = *seq;
	throw(MAL, "receptor.scenario", "Scenario '%s' not yet implemented", *nme);
}

str
RCgenerator(int *ret, str *nme, str *modnme, str *fcnnme)
{
	Receptor rc;
	rc = RCfind(*nme);
	if (rc == NULL)
		throw(MAL, "receptor.generator", "Receptor '%s' not defined",*nme);
#ifdef _DEBUG_RECEPTOR_
	mnstr_printf(RCout, "#Define receptor generator\n");
#endif
	(void) ret;
	rc->modnme = GDKstrdup(*modnme);
	rc->modnme = GDKstrdup(*fcnnme);
	throw(MAL, "receptor.generator", "Receptor '%s' not yet implemented",*nme);
}

/*
 * The hard part starts here. Each receptor is turned into
 * a separate thread that reads the channel and prepares
 * the containers for the continuous queries.
 * The receptor body should continously read until the socket is closed.
 */

static void
RCreconnect(Receptor rc)
{
	do {
		rc->error = NULL;
		if (rc->mode == BSKTACTIVE)
			rc->error = socket_client_connect(&rc->newsockfd, rc->host, rc->port);
		if (rc->error) {
			mnstr_printf(RCout, "#Receptor connect fails: %s\n", rc->error);
			MT_sleep_ms(rc->delay);
		}
	} while (rc->error);
}


#define myisspace(s)  ((s) == ' ' || (s) == '\t')

static inline char *
find_quote(char *s, char quote)
{
	while (*s != quote)
		s++;
	return s;
}

static inline char *
rfind_quote(char *s, char *e, char quote)
{
	while (*e != quote && e > s)
		e--;
	return e;
}

static inline int
insert_val(Column *fmt, char *s, char *e, char quote, ptr key, str *err, int c)
{
	char bak = 0;
	const void *adt;
	char buf[BUFSIZ];

	if (quote) {
		/* string needs the quotes included */
		s = find_quote(s, quote);
		if (!s) {
			snprintf(buf, BUFSIZ, "quote '%c' expected but not found in \"%s\" from line " BUNFMT "\n", quote, s, BATcount(fmt->c[0]));
			*err = GDKstrdup(buf);
			return -1;
		}
		s++;
		e = rfind_quote(s, e, quote);
		if (s != e) {
			bak = *e;
			*e = 0;
		}
		if ((s == e && fmt->nullstr[0] == 0) ||
			(quote == fmt->nullstr[0] && e > s &&
			 strncasecmp(s, fmt->nullstr + 1, fmt->nillen) == 0 &&
			 quote == fmt->nullstr[fmt->nillen - 1])) {
			adt = fmt->nildata;
			fmt->c[0]->T->nonil = 0;
		} else
			adt = fmt->frstr(fmt, fmt->adt, s, e, quote);
		if (bak)
			*e = bak;
	} else {
		if (s != e) {
			bak = *e;
			*e = 0;
		}

		if ((s == e && fmt->nullstr[0] == 0) ||
			(e > s && strcasecmp(s, fmt->nullstr) == 0)) {
			adt = fmt->nildata;
			fmt->c[0]->T->nonil = 0;
		} else
			adt = fmt->frstr(fmt, fmt->adt, s, e, quote);
		if (bak)
			*e = bak;
	}

	if (!adt) {
		char *val;
		bak = *e;
		*e = 0;
		val = (s != e) ? GDKstrdup(s) : GDKstrdup("");
		*e = bak;

		snprintf(buf, BUFSIZ, "value '%s' while parsing '%s' from line " BUNFMT " field %d not inserted, expecting type %s\n", val, s, BATcount(fmt->c[0]), c, fmt->type);
		*err = GDKstrdup(buf);
		GDKfree(val);
		return -1;
	}
	/* key may be NULL but that's not a problem, as long as we have void */
	bunfastins(fmt->c[0], key, adt);
	return 0;
  bunins_failed:
	snprintf(buf, BUFSIZ, "while parsing '%s' from line " BUNFMT " field %d not inserted\n", s, BATcount(fmt->c[0]), c);
	*err = GDKstrdup(buf);
	return -1;
}

static char *
tablet_skip_string(char *s, char quote)
{
	while (*s) {
		if (*s == '\\' && s[1] != '\0')
			s++;
		else if (*s == quote) {
			if (s[1] == quote)
				*s++ = '\\';	/* sneakily replace "" with \" */
			else
				break;
		}
		s++;
	}
	assert(*s == quote || *s == '\0');
	if (*s)
		s++;
	else
		return NULL;
	return s;
}

static int
insert_line(Tablet *as, char *line, ptr key, BUN col1, BUN col2)
{
	Column *fmt = as->format;
	char *s, *e = 0, quote = 0, seperator = 0;
	BUN i;
	char errmsg[BUFSIZ];

	for (i = 0; i < as->nr_attrs; i++) {
		e = 0;

		/* skip leading spaces */
		if (fmt[i].ws)
			while (myisspace((int) (*line)))
				line++;
		s = line;

		/* recognize fields starting with a quote */
		if (*line && *line == fmt[i].quote && (line == s || *(line - 1) != '\\')) {
			quote = *line;
			line++;
			line = tablet_skip_string(line, quote);
			if (!line) {
				snprintf(errmsg, BUFSIZ, "End of string (%c) missing " "in %s at line " BUNFMT "\n", quote, s, BATcount(fmt->c[0]));
				as->error = GDKstrdup(errmsg);
				if (!as->tryall)
					return -1;
				BUNins(as->complaints, NULL, as->error, TRUE);
			}
		}

		/* skip until separator */
		seperator = fmt[i].sep[0];
		if (fmt[i].sep[1] == 0) {
			while (*line) {
				if (*line == seperator) {
					e = line;
					break;
				}
				line++;
			}
		} else {
			while (*line) {
				if (*line == seperator &&
					strncmp(fmt[i].sep, line, fmt[i].seplen) == 0) {
					e = line;
					break;
				}
				line++;
			}
		}
		if (!e && i == (as->nr_attrs - 1))
			e = line;
		if (e) {
			if (i >= col1 && i < col2)
				(void) insert_val(&fmt[i], s, e, quote, key, &as->error, (int) i);
			quote = 0;
			line = e + fmt[i].seplen;
			if (as->error) {
				if (!as->tryall)
					return -1;
				BUNins(as->complaints, NULL, as->error, TRUE);
			}
		} else {
			snprintf(errmsg, BUFSIZ, "missing separator '%s' line " BUNFMT " field " BUNFMT "\n", fmt->sep, BATcount(fmt->c[0]), i);
			as->error = GDKstrdup(errmsg);
			if (!as->tryall)
				return -1;
			BUNins(as->complaints, NULL, as->error, TRUE);
		}
	}
	return 0;
}

static void
RCbody(Receptor rc)
{
	char buf[MYBUFSIZ + 1];
	char tuplesINbuffer[5];
	int counter = 0;
	int cnt;
	size_t j;
	str e, he;
	str line = "\0";
	int i, k;
	ssize_t n;
	SOCKET newsockfd = rc->newsockfd;
	stream *receptor;
#ifdef _DEBUG_RECEPTOR_
	int m = 0;
#endif
	buf[MYBUFSIZ] = 0; /* ensure null terminated string */
	rc->newsockfd = 0;

	if (rc->scenario) {
		RCscenarioInternal(rc);
		return;
	}
	if (rc->modnme && rc->fcnnme) {
		RCgeneratorInternal(rc);
		return;
	}
	/* ADD YOUR FAVORITE RECEPTOR CODE HERE */

bodyRestart:
	/* create the channel the first time or when connection was lost. */
	if (rc->mode == BSKTACTIVE && rc->protocol == UDP)
		receptor = udp_rastream(rc->host, rc->port, rc->name);
	else
		receptor = socket_rastream(newsockfd, rc->name);
	if (receptor == NULL) {
		perror("Receptor: Could not open stream");
		mnstr_printf(RCout, "#stream %s.%d.%s\n", rc->host, rc->port, rc->name);
		socket_close(newsockfd);
#ifdef _DEBUG_RECEPTOR_
		mnstr_printf(RCout, "#Terminate RCbody loop\n");
#endif
		return;
	}

	/*
	 * Consume each event and store the result.
	 * If the thread is suspended we sleep for at least one second.
	 * In case of a locked basket we sleep for a millisecond.
	 */

	rc->cycles++;
	for (n = 1; n > 0;) {
		if (rc->status == BSKTPAUSE) {
#ifdef _DEBUG_RECEPTOR_
			mnstr_printf(RCout, "#pause receptor %s\n", rc->name);
#endif
			while (rc->status == BSKTPAUSE && rc->delay)
				MT_sleep_ms(rc->delay);
#ifdef _DEBUG_RECEPTOR_
			mnstr_printf(RCout, "#pause receptor %s ended\n", rc->name);
#endif
		}

		if (rc->status == BSKTSTOP) {
			mnstr_close(receptor);
			for (j = 0; j < rc->table.nr_attrs; j++) {
				GDKfree(rc->table.format[j].data);
				BBPdecref(rc->table.format[j].c[0]->batCacheid, TRUE);
				/* above will be double freed with multiple
				 * streams/threads */
			}
			shutdown(newsockfd, SHUT_RDWR);
			GDKfree(rc);
			rc = NULL;
			break;
		}

		(void) MTIMEcurrent_timestamp(&rc->lastseen);
#ifdef _DEBUG_RECEPTOR_
		mnstr_printf(RCout, "#wait for data read m: %d\n", m);
#endif

		/* actually we should switch here based on the event syntax protocol */

		/*Batch Processing
		  The Datadriver (see linear road benchmark) or the Sensor
		  tools, are connected through TCP/IP connection to the receptor
		  module and	feed the DataCell with tuples, Both tools are
		  able to send batches of tuples to the stream engine The first
		  line of each batch always contains the number of tuples that
		  the receptor is going to read (i.e.,#number) When the receptor
		  reads the first line of the incoming message, it immediately
		  LOCKS the bats (that constitute the basket) and keeps the lock
		  until the end of the reading/writting procedure When the
		  receptor reads all the tuples UNLOCKS the bats, and then the
		  Factories/Queries that are waiting for these data are able to
		  read it*/

		if ((n = mnstr_readline(receptor, buf, MYBUFSIZ)) > 0) {
			buf[n] = 0;
#ifdef _DEBUG_RECEPTOR_
			mnstr_printf(RCout, "#Receptor buf [" SSZFMT "]:%s \n", n, buf);
			m = 0;
#endif
			/* use trivial concurrency measure */
			line = buf;

			/* BATs may be replaced in the meantime */
			BSKTlock(&rc->lck, &rc->name, &rc->delay);
			for (i = 0; i < baskets[rc->bskt].colcount; i++)
				rc->table.format[i].c[0] = baskets[rc->bskt].primary[i];
			BSKTunlock(&rc->lck, &rc->name);

			cnt = 0;
			he = strchr(line, '#');
			if (he != 0) {
				strcpy(tuplesINbuffer, line + 1);
				counter = atoi(tuplesINbuffer);
				*he = 0;
			} else {
				/* we got the line already */
				goto parse;
			}

			/* this code should be optimized for block-based reads */
			while (cnt < counter) {
				if ((n = mnstr_readline(receptor, buf, MYBUFSIZ)) > 0) {
					buf[n] = 0;
#ifdef _DEBUG_RECEPTOR_
					mnstr_printf(RCout, "#Receptor buf [" SSZFMT "]:%s \n", n, buf);
#endif
parse:
					do {
						line = buf;
						e = strchr(line, '\n');
						if (e == 0) {
							/* only keep the last errorenous event for analysis */
							if (rcError)
								GDKfree(rcError);
							rcError = (char *) GDKmalloc(k = strlen(line) + 100);
							if (rcError)
								snprintf(rcError, k, "newline missing:%s", line);
							rcErrorEvent = cnt;
							cnt--;
							break;
						}
						*e = 0;
#ifdef _DEBUG_RECEPTOR_
						mnstr_printf(RCout, "#insert line :%s \n", line);
#endif
						BSKTlock(&rc->lck, &rc->name, &rc->delay);
						if (insert_line(&rc->table, line, NULL, 0, rc->table.nr_attrs) < 0) {
							if (baskets[rc->bskt].errors)
								BUNappend(baskets[rc->bskt].errors, line, TRUE);
							/* only keep the last errorenous event for analysis */
							if (rcError)
								GDKfree(rcError);
							rcError = (char *) GDKmalloc(k = strlen(line) + 100);
							if (rcError)
								snprintf(rcError, k, "parsing error:%s", line);
							rcErrorEvent = cnt;
							BSKTunlock(&rc->lck, &rc->name);
							break;
						}
						rc->received++;
						rc->pending++;
						BSKTunlock(&rc->lck, &rc->name);
						e++;
						line = e;
					} while (*e);
				}
				cnt++;
			}
			if (rc->table.error) {
				mnstr_printf(GDKerr, "%s", rc->table.error);
				rc->table.error = 0;
			}
		}
	}
	/* only when reading fails we attempt to reconnect */
	mnstr_close(receptor);
	if (rc->mode == BSKTACTIVE) {
		/* try to reconnect */
		RCreconnect(rc);
		goto bodyRestart;
	}
#ifdef _DEBUG_RECEPTOR_
	mnstr_printf(RCout, "#Terminate RCbody loop\n");
#endif
}
/*
 * A short cut is to generate the events based upon the interpretation
 * of a scenario file. Much like the one used in the sensor.
 * It is processed multiple times. The header is the delay imposed.
 * Make sure you use a complete path.
 */
void
RCscenarioInternal(Receptor rc)
{
	char buf[MYBUFSIZ + 1], *tuple;
	lng tick;
	lng previoustsmp = 0;
	FILE *fd;
	int snr;
	int newdelay = 0;

	if (rc->scenario == 0) {
		mnstr_printf(RCout, "Scenario missing\n");
		return;
	}
#ifdef _DEBUG_RECEPTOR_
	mnstr_printf(RCout, "#Execute the scenario '%s'\n", rc->scenario);
#endif

	snr = 0;
	do {
		fd = fopen(rc->scenario, "r");
		if (fd == NULL) {
			mnstr_printf(RCout, "Could not open file '%s'\n", rc->scenario);
			return;
		}

		/* read the event requests and sent when the becomes */
		while (fgets(buf, MYBUFSIZ, fd) != 0) {
			newdelay = (int) atol(buf);
			tuple = buf;

			if (newdelay > 0) {
				/* wait */
				tuple = strchr(buf, '[');
				if (tuple == 0)
					tuple = buf;
				MT_sleep_ms(newdelay);
			} else if (rc->delay > 0) {
				/* wait */
				MT_sleep_ms(rc->delay);
			}
#ifdef _DEBUG_RECEPTOR_
			mnstr_printf(RCout, "#%s", tuple);
#endif
			do {
				tick = usec();
			} while (tick == previoustsmp);

			previoustsmp = tick;

			BSKTlock(&rc->lck, &rc->name, &rc->delay);
			if (rc->status != BSKTRUNNING) {
				snr = rc->sequence;
				break;
			}
			if (insert_line(&rc->table, tuple + 1 /*ignore '[' */, (ptr) & tick, 0, rc->table.nr_attrs) < 0) {
				mnstr_printf(RCout, "failed insert_line %s\n", tuple);
				BSKTunlock(&rc->lck, &rc->name);
				break;
			}
			BSKTunlock(&rc->lck, &rc->name);
		}
		fclose(fd);
		snr++;
	} while (snr < rc->sequence);
}
/*
 * @
 * The last option is to simply associate a MAL function/factory
 * with an receptor. Its body can be used to encode
 * arbitrary complex generators. The easiest one is
 * a metronome.
 * Its implementation similar to the petrinet engine.
 */
static void
RCgeneratorInternal(Receptor rc)
{
	Symbol s;
	InstrPtr p;
	MalStkPtr glb;
	MalBlkPtr mb;
	Client cntxt = &mal_clients[0];  /* FIXME: should this be the active user? */
	int pc;

	if (rc->modnme == 0 || rc->fcnnme) {
		mnstr_printf(RCout, "Factory missing\n");
		return;
	}
	s = newFunction("user", "rcController", FACTORYsymbol);
	p = getSignature(s);
	getArg(p, 0) = newTmpVariable(mb = s->def, TYPE_void);
	/* create an execution environment */
	p = newFcnCall(mb, rc->modnme, rc->fcnnme);
	pc = getPC(mb, p);
	pushEndInstruction(mb);
	chkProgram(cntxt->fdout, cntxt->nspace, mb);
	if (mb->errors) {
		mnstr_printf(RCout, "Receptor Controller found errors\n");
		return;
	}

	newStack(glb, mb->vtop);
	memset((char *) glb, 0, stackSize(mb->vtop));
	glb->stktop = mb->vtop;
	glb->blk = mb;

#ifdef _DEBUG_RECEPTOR_
	printFunction(RCout, mb, 0, LIST_MAL_ALL);
#endif
	for (;;)
		switch (rc->status) {
		case BSKTPAUSE:
			MT_sleep_ms(1);
			break;
		case BSKTSTOP:
		case BSKTINIT:
		case BSKTERROR:
			return;
		case BSKTRUNNING:
			reenterMAL(cntxt, mb, pc, pc + 1, glb);
		}

}


/*
 * The receptor thread manages the connections. Both as a active and
 * in passive mode.  The UDP channel part is not our focus right now.
 */
str
RCstartThread(Receptor rc)
{
#ifdef _DEBUG_RECEPTOR_
	mnstr_printf(RCout, "#Receptor body %s starts at %s:%d\n", rc->name, rc->host, rc->port);
#endif

	/* Handle a server mode protocol */
#ifdef _DEBUG_RECEPTOR_
	mnstr_printf(RCout, "#Start the receptor thread, protocol=%d\n", rc->protocol);
#endif
	if (rc->mode == BSKTPASSIVE &&
		(rc->error = socket_server_connect(&rc->sockfd, rc->port))) {
		rc->status = BSKTERROR;
		mnstr_printf(RCout, "Failed to start receptor server:%s\n", rc->error);
		/* in this case there is nothing more we can do but terminate */
		return NULL;
	}
	/* the receptor should continously attempt to either connect the
	   remote site for new events or listing for the next request */
	while (rc->status != BSKTSTOP) {
		if (rc->mode == BSKTPASSIVE) {
			/* in server mode you should expect new connections */
#ifdef _DEBUG_RECEPTOR_
			mnstr_printf(RCout, "#Receptor listens\n");
#endif
			rc->error = socket_server_listen(rc->sockfd, &rc->newsockfd);
			if (rc->error) {
				mnstr_printf(RCout, "Receptor listen fails: %s\n", rc->error);
				rc->status = BSKTERROR;
			}
#ifdef _DEBUG_RECEPTOR_
			mnstr_printf(RCout, "#Receptor connection request received \n");
#endif
			if (MT_create_thread(&rc->pid, (void (*)(void *))RCbody, rc, MT_THR_DETACHED) != 0) {
				shutdown(rc->newsockfd, SHUT_RDWR);
				close(rc->newsockfd);
				GDKfree(rc);
				throw(MAL, "receptor.start", "Process '%s' creation failed",rc->name);
			}
			/* ensure the thread took rc->newsockfd */
			while (rc->newsockfd > 0)
				MT_sleep_ms(100);
		} else if (rc->mode == BSKTACTIVE) {
			/* take the initiative to connect to sensor */
			RCreconnect(rc);
			RCbody(rc);
		}
	}
	shutdown(rc->sockfd, SHUT_RDWR);
	return MAL_SUCCEED;
}

static void
dumpReceptor(Receptor rc)
{
	mnstr_printf(GDKout, "#receptor %s at %s:%d protocol=%s mode=%s status=%s delay=%d \n",
			rc->name, rc->host, rc->port, protocolname[rc->protocol], modename[rc->mode], statusname[rc->status], rc->delay);
}

str
RCdump(void)
{
	Receptor rc = rcAnchor;
	for (; rc; rc = rc->nxt)
		dumpReceptor(rc);
	if (rcError)
		mnstr_printf(GDKout, "#last error event %d:%s\n", rcErrorEvent, rcError);
	return MAL_SUCCEED;
}
/* provide a tabular view for inspection */
str
RCtable(int *nameId, int *hostId, int *portId, int *protocolId, int *modeId, int *statusId, int *seenId, int *cyclesId, int *receivedId, int *pendingId)
{
	BAT *name = NULL, *seen = NULL, *pending = NULL, *received = NULL, *cycles = NULL;
	BAT *protocol = NULL, *mode = NULL, *status = NULL, *port = NULL, *host = NULL;
	Receptor rc = rcAnchor;

	name = BATnew(TYPE_void, TYPE_str, BATTINY, TRANSIENT);
	if (name == 0)
		goto wrapup;
	BATseqbase(name, 0);
	host = BATnew(TYPE_void, TYPE_str, BATTINY, TRANSIENT);
	if (host == 0)
		goto wrapup;
	BATseqbase(host, 0);
	port = BATnew(TYPE_void, TYPE_int, BATTINY, TRANSIENT);
	if (port == 0)
		goto wrapup;
	BATseqbase(port, 0);
	protocol = BATnew(TYPE_void, TYPE_str, BATTINY, TRANSIENT);
	if (protocol == 0)
		goto wrapup;
	BATseqbase(protocol, 0);
	mode = BATnew(TYPE_void, TYPE_str, BATTINY, TRANSIENT);
	if (mode == 0)
		goto wrapup;
	BATseqbase(mode, 0);

	seen = BATnew(TYPE_void, TYPE_timestamp, BATTINY, TRANSIENT);
	if (seen == 0)
		goto wrapup;
	BATseqbase(seen, 0);
	cycles = BATnew(TYPE_void, TYPE_int, BATTINY, TRANSIENT);
	if (cycles == 0)
		goto wrapup;
	BATseqbase(cycles, 0);
	pending = BATnew(TYPE_void, TYPE_int, BATTINY, TRANSIENT);
	if (pending == 0)
		goto wrapup;
	BATseqbase(pending, 0);
	received = BATnew(TYPE_void, TYPE_int, BATTINY, TRANSIENT);
	if (received == 0)
		goto wrapup;
	BATseqbase(received, 0);
	status = BATnew(TYPE_void, TYPE_str, BATTINY, TRANSIENT);
	if (status == 0)
		goto wrapup;
	BATseqbase(status, 0);

	for (; rc; rc = rc->nxt)
		if (rc->table.format[1].c[0]) {
			BUNappend(name, rc->name, FALSE);
			BUNappend(host, rc->host, FALSE);
			BUNappend(port, &rc->port, FALSE);
			BUNappend(protocol, protocolname[rc->protocol], FALSE);
			BUNappend(mode, modename[rc->mode], FALSE);
			BUNappend(status, statusname[rc->status], FALSE);
			BUNappend(seen, &rc->lastseen, FALSE);
			BUNappend(cycles, &rc->cycles, FALSE);
			rc->pending = (int) BATcount(rc->table.format[1].c[0]);
			BUNappend(pending, &rc->pending, FALSE);
			BUNappend(received, &rc->received, FALSE);
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
	BBPkeepref(*receivedId = received->batCacheid);
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
	if (received)
		BBPreleaseref(received->batCacheid);
	throw(MAL, "datacell.baskets", MAL_MALLOC_FAIL);
}
