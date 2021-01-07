/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0.  If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 *
 * Copyright 1997 - July 2008 CWI, August 2008 - 2021 MonetDB B.V.
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


static const char *
sigtostr(int sig)
{
	switch (sig) {
#ifdef SIGABRT
	case SIGABRT:
		return "SIGABRT";		/* Aborted */
#endif
#ifdef SIGALRM
	case SIGALRM:
		return "SIGALRM";		/* Alarm clock */
#endif
#ifdef SIGBUS
	case SIGBUS:
		return "SIGBUS";		/* Bus error */
#endif
#ifdef SIGCHLD
	case SIGCHLD:
		return "SIGCHLD";		/* Child exited */
#endif
#ifdef SIGCONT
	case SIGCONT:
		return "SIGCONT";		/* Continued */
#endif
#ifdef SIGFPE
	case SIGFPE:
		return "SIGFPE";		/* Floating point exception */
#endif
#ifdef SIGHUP
	case SIGHUP:
		return "SIGHUP";		/* Hangup */
#endif
#ifdef SIGILL
	case SIGILL:
		return "SIGILL";		/* Illegal instruction */
#endif
#ifdef SIGINT
	case SIGINT:
		return "SIGINT";		/* Interrupt */
#endif
#ifdef SIGKILL
	case SIGKILL:
		return "SIGKILL";		/* Killed */
#endif
#ifdef SIGPIPE
	case SIGPIPE:
		return "SIGPIPE";		/* Broken pipe */
#endif
#ifdef SIGPOLL
	case SIGPOLL:
		return "SIGPOLL";		/* Pollable event */
#endif
#ifdef SIGPROF
	case SIGPROF:
		return "SIGPROF";		/* Profiling timer expired */
#endif
#ifdef SIGPWR
	case SIGPWR:
		return "SIGPWR";		/* Power fail */
#endif
#ifdef SIGQUIT
	case SIGQUIT:
		return "SIGQUIT";		/* Quit */
#endif
#ifdef SIGSEGV
	case SIGSEGV:
		return "SIGSEGV";		/* Segmentation fault */
#endif
#ifdef SIGSTKFLT
	case SIGSTKFLT:
		return "SIGSTKFLT";		/* Stack fault */
#endif
#ifdef SIGSTOP
	case SIGSTOP:
		return "SIGSTOP";		/* Stopped (signal) */
#endif
#ifdef SIGSYS
	case SIGSYS:
		return "SIGSYS";		/* Bad system call */
#endif
#ifdef SIGTERM
	case SIGTERM:
		return "SIGTERM";		/* Terminated */
#endif
#ifdef SIGTRAP
	case SIGTRAP:
		return "SIGTRAP";		/* Trace/breakpoint trap */
#endif
#ifdef SIGTSTP
	case SIGTSTP:
		return "SIGTSTP";		/* Stopped */
#endif
#ifdef SIGTTIN
	case SIGTTIN:
		return "SIGTTIN";		/* Stopped (tty input) */
#endif
#ifdef SIGTTOU
	case SIGTTOU:
		return "SIGTTOU";		/* Stopped (tty output) */
#endif
#ifdef SIGURG
	case SIGURG:
		return "SIGURG";		/* Urgent I/O condition */
#endif
#ifdef SIGUSR1
	case SIGUSR1:
		return "SIGUSR1";		/* User defined signal 1 */
#endif
#ifdef SIGUSR2
	case SIGUSR2:
		return "SIGUSR2";		/* User defined signal 2 */
#endif
#ifdef SIGVTALRM
	case SIGVTALRM:
		return "SIGVTALRM";		/* Virtual timer expired */
#endif
#ifdef SIGWINCH
	case SIGWINCH:
		return "SIGWINCH";		/* Window changed */
#endif
#ifdef SIGXCPU
	case SIGXCPU:
		return "SIGXCPU";		/* CPU time limit exceeded */
#endif
#ifdef SIGXFSZ
	case SIGXFSZ:
		return "SIGXFSZ";		/* File size limit exceeded */
#endif
	default:
		return NULL;
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

	if (write(1, "caught ", 7) < 0 ||
		(signame ? write(1, signame, strlen(signame)) : write(1, "some signal", 11)) < 0 ||
		write(1, ", starting shutdown sequence\n", 29) < 0)
		perror("write failed");
	_mero_keep_listening = 0;
}

/* we're not using a lock for setting, reading and clearing this flag
 * (deadlock!), but we should use atomic instructions */
static volatile sig_atomic_t hupflag = 0;

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
	 * later on */
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
		(void) fcntl(t, F_SETFD, FD_CLOEXEC);
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
				p->pid = -1;	/* indicate the process is dead */

				/* close the descriptors */
				close(p->out);
				close(p->err);
				p->out = -1;
				p->err = -1;
				if (WIFEXITED(wstatus)) {
					Mfprintf(stdout, "database '%s' (%lld) has exited with "
							 "exit status %d\n", p->dbname,
							 (long long int)p->pid, WEXITSTATUS(wstatus));
				} else if (WIFSIGNALED(wstatus)) {
					const char *sigstr = sigtostr(WTERMSIG(wstatus));
					char signum[8];
					if (sigstr == NULL) {
						snprintf(signum, 8, "%d", WTERMSIG(wstatus));
						sigstr = signum;
					}
					if (WCOREDUMP(wstatus)) {
						Mfprintf(stdout, "database '%s' (%lld) has crashed "
								 "with signal %s (dumped core)\n",
								 p->dbname, (long long int)p->pid, sigstr);
					} else {
						Mfprintf(stdout, "database '%s' (%lld) was killed "
								 "by signal %s\n",
								 p->dbname, (long long int)p->pid, sigstr);
					}
				}
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
	(void) sigemptyset(&sa.sa_mask);
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
