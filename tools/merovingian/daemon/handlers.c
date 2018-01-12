/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0.  If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 *
 * Copyright 1997 - July 2008 CWI, August 2008 - 2018 MonetDB B.V.
 */

#include "monetdb_config.h"
#include <signal.h>
#include <unistd.h> /* isatty */
#include <time.h> /* time, localtime */
#include <string.h> /* str* */
#include <sys/types.h> /* open */
#include <sys/wait.h> /* wait */
#include <sys/stat.h> /* open */
#include <fcntl.h> /* open */

#include "utils/properties.h"

#include "merovingian.h"
#include "handlers.h"

#ifndef O_CLOEXEC
#define O_CLOEXEC 0
#endif


static const char *sigint  = "SIGINT";
static const char *sigterm = "SIGTERM";
static const char *sigquit = "SIGQUIT";
static const char *sighup  = "SIGHUP";
static const char *sigabrt = "SIGABRT";
static const char *sigsegv = "SIGSEGV";
static const char *sigbus  = "SIGBUS";
static const char *sigkill = "SIGKILL";
static const char *
sigtostr(int sig)
{
	switch (sig) {
		case SIGINT:
			return(sigint);
		case SIGTERM:
			return(sigterm);
		case SIGQUIT:
			return(sigquit);
		case SIGHUP:
			return(sighup);
		case SIGABRT:
			return(sigabrt);
		case SIGSEGV:
			return(sigsegv);
#ifdef SIGBUS
		case SIGBUS:
			return(sigbus);
#endif
		case SIGKILL:
			return(sigkill);
		default:
			return(NULL);
	}
}

/**
 * Handler for SIGINT, SIGTERM and SIGQUIT.  This starts a graceful
 * shutdown of merovingian.
 */
void
handler(int sig)
{
	char buf[64];
	const char *signame = sigtostr(sig);

	strcpy(buf, "caught ");
	if (signame) {
		strcpy(buf + 7, signame);
	} else {
		strcpy(buf + 7, "some signal");
	}
	strcpy(buf + strlen(buf), ", starting shutdown sequence\n");
	if (write(1, buf, strlen(buf)) < 0)
		perror("write failed");
	_mero_keep_listening = 0;
}

/* we're not using a lock for setting, reading and clearing this flag
 * (deadlock!), but we should use atomic instructions */
static volatile int hupflag = 0;

/**
 * Handler for SIGHUP, causes a re-read of the .merovingian_properties
 * file and the logfile to be reopened.
 */
void
huphandler(int sig)
{
	(void) sig;

	hupflag = 1;
}

void reinitialize(void)
{
	int t;
	time_t now;
	struct tm *tmp;
	char mytime[20];
	char *f;
	confkeyval *kv;

	if (!hupflag)
		return;

	hupflag = 0;

	now = time(NULL);
	tmp = localtime(&now);

	/* re-read properties, we're in our dbfarm */
	readProps(_mero_props, ".");

	/* check and trim the hash-algo from the passphrase for easy use
	 * lateron */
	kv = findConfKey(_mero_props, "passphrase");
	if (kv->val != NULL) {
		char *h = kv->val + 1;
		if ((f = strchr(h, '}')) == NULL) {
			Mfprintf(stderr, "ignoring invalid passphrase: %s\n", kv->val);
			setConfVal(kv, NULL);
		} else {
			*f++ = '\0';
			if (strcmp(h, MONETDB5_PASSWDHASH) != 0) {
				Mfprintf(stderr, "ignoring passphrase with incompatible "
						"password hash: %s\n", h);
				setConfVal(kv, NULL);
			} else {
				setConfVal(kv, f);
			}
		}
	}

	/* have to make sure the logger is not logging anything */
	pthread_mutex_lock(&_mero_topdp_lock);

	strftime(mytime, sizeof(mytime), "%Y-%m-%d %H:%M:%S", tmp);

	f = getConfVal(_mero_props, "logfile");
	/* reopen (or open new) file */
	t = open(f, O_WRONLY | O_APPEND | O_CREAT | O_CLOEXEC, S_IRUSR | S_IWUSR);
	if (t == -1) {
		Mfprintf(stderr, "forced to ignore SIGHUP: unable to open "
				"'%s': %s\n", f, strerror(errno));
	} else {
#if O_CLOEXEC == 0
		fcntl(t, F_SETFD, FD_CLOEXEC);
#endif
		Mfprintf(_mero_logfile, "%s END merovingian[%lld]: "
				"caught SIGHUP, closing logfile\n",
				mytime, (long long int)_mero_topdp->next->pid);
		fflush(_mero_logfile);
		_mero_topdp->out = _mero_topdp->err = t;
		_mero_logfile = fdopen(t, "a");
		Mfprintf(_mero_logfile, "%s BEG merovingian[%lld]: "
				"reopening logfile\n",
				mytime, (long long int)_mero_topdp->next->pid);
	}

	/* logger go ahead! */
	pthread_mutex_unlock(&_mero_topdp_lock);
}

/**
 * Wait for and deal with any children that may have exited.  This
 * handler deals with terminated children, by deregistering them from
 * the internal administration (_mero_topdp) with the necessary
 * cleanup.
 */
void
childhandler(void)
{
	dpair p, q;
	pid_t pid;
	int wstatus;

	while ((pid = waitpid(-1, &wstatus, WNOHANG)) > 0) {
		pthread_mutex_lock(&_mero_topdp_lock);

		/* get the pid from the former child, and locate it in our list */
		q = _mero_topdp->next;
		p = q->next;
		while (p != NULL) {
			if (p->pid == pid) {
				/* log everything that's still in the pipes */
				logFD(p->out, "MSG", p->dbname, (long long int)p->pid, _mero_logfile, 1);
				/* remove from the list */
				q->next = p->next;
				/* close the descriptors */
				close(p->out);
				close(p->err);
				if (WIFEXITED(wstatus)) {
					Mfprintf(stdout, "database '%s' (%lld) has exited with "
							 "exit status %d\n", p->dbname,
							 (long long int)p->pid, WEXITSTATUS(wstatus));
				} else if (WIFSIGNALED(wstatus)) {
					if (WCOREDUMP(wstatus)) {
						Mfprintf(stdout, "database '%s' (%lld) has crashed "
								 "(dumped core)\n", p->dbname,
								 (long long int)p->pid);
					} else {
						const char *sigstr = sigtostr(WTERMSIG(wstatus));
						char signum[8];
						if (sigstr == NULL) {
							snprintf(signum, 8, "%d", WTERMSIG(wstatus));
							sigstr = signum;
						}
						Mfprintf(stdout, "database '%s' (%lld) was killed by signal "
								 "%s\n", p->dbname,
								 (long long int)p->pid, sigstr);
					}
				}
				if (p->dbname)
					free(p->dbname);
				free(p);
				pthread_mutex_unlock(&_mero_topdp_lock);
				break;
			}
			q = p;
			p = q->next;
		}

		pthread_mutex_unlock(&_mero_topdp_lock);
	}
}

/**
 * Last resort handler to give a message in the log.
 */
void
segvhandler(int sig) {
	struct sigaction sa;

	(void)sig;

	/* (try to) ignore any further segfaults */
	sigemptyset(&sa.sa_mask);
	sa.sa_flags = 0;
	sa.sa_handler = SIG_IGN;
	sigaction(SIGSEGV, &sa, NULL);

	if (_mero_topdp != NULL) {
		char errmsg[] = "\nSEGMENTATION FAULT OCCURRED\n"
				"\nA fatal error has occurred which prevents monetdbd from operating."
				"\nThis is likely a bug in monetdbd, please report it on http://bugs.monetdb.org/"
				"\nand include the tail of this log in your bugreport with your explanation of "
				"\nwhat you were doing, if possible.\n"
				"\nABORTING NOW, YOU HAVE TO MANUALLY KILL ALL REMAINING mserver5 PROCESSES\n";
		if (write(_mero_topdp->err, errmsg, sizeof(errmsg) - 1) >= 0)
			sync();
	}
	abort();
}

/* vim:set ts=4 sw=4 noexpandtab: */
