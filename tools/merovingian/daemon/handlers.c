/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0.  If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 *
 * Copyright 1997 - July 2008 CWI, August 2008 - 2016 MonetDB B.V.
 */

#include "monetdb_config.h"
#include <stdio.h>
#include <signal.h>
#include <unistd.h> /* isatty */
#include <time.h> /* time, localtime */
#include <string.h> /* str* */
#include <sys/types.h> /* open */
#include <sys/wait.h> /* wait */
#include <sys/stat.h> /* open */
#include <fcntl.h> /* open */
#include <errno.h>
#include <pthread.h>

#include <utils/properties.h>

#include "merovingian.h"
#include "handlers.h"


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
	const char *signame = sigtostr(sig);
	if (signame == NULL) {
		Mfprintf(stdout, "caught signal %d, starting shutdown sequence\n", sig);
	} else {
		Mfprintf(stdout, "caught %s, starting shutdown sequence\n", signame);
	}
	_mero_keep_listening = 0;
}

/**
 * Handler for SIGHUP, causes a re-read of the .merovingian_properties
 * file and the logfile to be reopened.
 */
void
huphandler(int sig)
{
	int t;
	time_t now = time(NULL);
	struct tm *tmp = localtime(&now);
	char mytime[20];
	char *f;
	confkeyval *kv;

	(void)sig;

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
	t = open(f, O_WRONLY | O_APPEND | O_CREAT, S_IRUSR | S_IWUSR);
	if (t == -1) {
		Mfprintf(stderr, "forced to ignore SIGHUP: unable to open "
				"'%s': %s\n", f, strerror(errno));
	} else {
		Mfprintf(_mero_logfile, "%s END merovingian[" LLFMT "]: "
				"caught SIGHUP, closing logfile\n",
				mytime, (long long int)_mero_topdp->next->pid);
		fflush(_mero_logfile);
		_mero_topdp->out = _mero_topdp->err = t;
		_mero_logfile = fdopen(t, "a");
		Mfprintf(_mero_logfile, "%s BEG merovingian[" LLFMT "]: "
				"reopening logfile\n",
				mytime, (long long int)_mero_topdp->next->pid);
	}

	/* logger go ahead! */
	pthread_mutex_unlock(&_mero_topdp_lock);
}

/**
 * Handles SIGCHLD signals, that is, signals that a parent receives
 * about its children.  This handler deals with terminated children, by
 * deregistering them from the internal administration (_mero_topdp)
 * with the necessary cleanup.
 */
void
childhandler(int sig, siginfo_t *si, void *unused)
{
	dpair p, q;

	(void)sig;
	(void)unused;

	/* wait for the child to get properly terminated, hopefully filling
	 * in the siginfo struct on FreeBSD */
	if (waitpid(-1, NULL, WNOHANG) <= 0) {
		/* if no child has exited, we may have already waited for it
		 * in e.g. ctl_handle_client() */
		return;
	}

	if (si->si_code != CLD_EXITED &&
			si->si_code != CLD_KILLED &&
			si->si_code != CLD_DUMPED)
	{
		/* ignore traps, stops and continues, we only want terminations
		 * of the client process */
		return;
	}

	pthread_mutex_lock(&_mero_topdp_lock);

	/* get the pid from the former child, and locate it in our list */
	q = _mero_topdp->next;
	p = q->next;
	while (p != NULL) {
		if (p->pid == si->si_pid) {
			/* log everything that's still in the pipes */
			logFD(p->out, "MSG", p->dbname, (long long int)p->pid, _mero_logfile);
			/* remove from the list */
			q->next = p->next;
			/* close the descriptors */
			close(p->out);
			close(p->err);
			if (si->si_code == CLD_EXITED) {
				Mfprintf(stdout, "database '%s' (%lld) has exited with "
						"exit status %d\n", p->dbname,
						(long long int)p->pid, si->si_status);
			} else if (si->si_code == CLD_KILLED) {
				const char *sigstr = sigtostr(si->si_status);
				char signum[8];
				if (sigstr == NULL) {
					snprintf(signum, 8, "%d", si->si_status);
					sigstr = signum;
				}
				Mfprintf(stdout, "database '%s' (%lld) was killed by signal "
						"%s\n", p->dbname,
						(long long int)p->pid, sigstr);
			} else if (si->si_code == CLD_DUMPED) {
				Mfprintf(stdout, "database '%s' (%lld) has crashed "
						"(dumped core)\n", p->dbname,
						(long long int)p->pid);
			}
			if (p->dbname)
				free(p->dbname);
			free(p);
			pthread_mutex_unlock(&_mero_topdp_lock);
			return;
		}
		q = p;
		p = q->next;
	}

	pthread_mutex_unlock(&_mero_topdp_lock);

	Mfprintf(stdout, "received SIGCHLD from unknown child with pid %lld\n",
			(long long int)si->si_pid);
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
