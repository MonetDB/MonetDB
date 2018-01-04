/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0.  If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 *
 * Copyright 1997 - July 2008 CWI, August 2008 - 2018 MonetDB B.V.
 */

#define _GNU_SOURCE		/* to get declaration of strsignal on Linux */

#include "monetdb_config.h"
#include <stdio.h>
#include <sys/types.h>
#ifdef HAVE_UNISTD_H
# include <unistd.h>
#endif
#ifdef HAVE_IO_H
# include <io.h>
#endif
#ifdef HAVE_SYS_WAIT_H
# include <sys/wait.h>
#endif
#include <signal.h>
#include <stdlib.h>
#include <ctype.h>
#include <time.h>
#ifdef HAVE_STRING_H
#include <string.h>
#endif

#define DEFAULT_TIMEOUT 0

static int timeout = DEFAULT_TIMEOUT;
static int quiet = 0;
static char *progname;

static pid_t exec_pid;
static char **exec_argv;
static int exec_timeout = 0;


static void
invocation(FILE *fp, char *prefix, char **argv)
{
	if (quiet) {
		if (fp == stderr) {
			fprintf(fp, "\n%s", prefix);
		}
	} else {
		fprintf(fp, "\n!%s: %s", progname, prefix);
	}
	while (*argv) {
		if (!quiet || fp == stderr)
			fprintf(fp, "%s ", *argv);
		argv++;
	}
	if (!quiet || fp == stderr)
		fprintf(fp, "\n");
}

static void
alarm_handler(int sig)
{
	(void) sig;
	exec_timeout = 1;
	kill(-exec_pid, SIGKILL);
}

static int
limit(char **argv)
{
	struct sigaction action;
	int status;

	exec_pid = fork();
	if (exec_pid == 0) {
		pid_t pid = getpid();

		/* Make this process the process group leader */
		setpgid(pid, pid);

		action.sa_handler = SIG_DFL;
		sigemptyset(&action.sa_mask);
		action.sa_flags = 0;
		sigaction(SIGXCPU, &action, 0);
		sigaction(SIGXFSZ, &action, 0);

		execvp(argv[0], argv);
		perror("exec");

		exit(EXIT_FAILURE);	/* could not exec binary */
	}

	/* parent */

	if (timeout) {
		/* We register the alarm handler in the parent process. If
		 * we would put the alarm in the child process, the child
		 * process could overrule it.  
		 */
		action.sa_handler = alarm_handler;
		sigemptyset(&action.sa_mask);
		action.sa_flags = 0;
		sigaction(SIGALRM, &action, 0);
		alarm(timeout);
	}

	while (waitpid(exec_pid, &status, 0) != exec_pid)
		;

	if (WIFEXITED(status)) {	/* Terminated normally */
		return WEXITSTATUS(status);
	} else if (WIFSIGNALED(status)) {	/* Got a signal */
		if (exec_timeout) {
			if (quiet) {
				char *cp[1];

				cp[0] = argv[9];	/* hardwired: the test output file */
				invocation(stderr, "!Timeout: ", cp);
			} else {
				invocation(stderr, "Timeout: ", argv);
			}
			return 1;
		} else {
			int wts = WTERMSIG(status);
			char msg[1024];

#ifdef HAVE_STRSIGNAL
			snprintf(msg, 1022, "%s (%d): ", strsignal(wts), wts);
#else
#ifdef HAVE__SYS_SIGLIST
			snprintf(msg, 1022, "%s (%d): ", _sys_siglist[wts], wts);
#else
			snprintf(msg, 1022, "signal %d: ", wts);
#endif
#endif
			invocation(stderr, msg, argv);
			return ((wts > 0) ? wts : 1);
		}
	}

	return 0;		/* shouldn't get here */
}


__declspec(noreturn) static void usage(void)
	__attribute__((__noreturn__));

static void
usage(void)
{
	fprintf(stderr, "Usage: %s\n" "\t-timeout <seconds>\n" "\t-q\n" "\t<progname> [<arguments>]\n", progname);
	exit(EXIT_FAILURE);
}

static void
parse_args(int argc, char **argv)
{
	progname = argv[0];
	argv++;
	argc--;

	while (argc && argv[0][0] == '-') {
		if (strcmp(argv[0], "-help") == 0) {
			usage();
		} else if (strcmp(argv[0], "-timeout") == 0) {
			argc--;
			argv++;
			if (argc == 0)
				usage();
			timeout = atoi(argv[0]);
		} else if (strcmp(argv[0], "-q") == 0) {
			quiet = 1;
		} else {
			usage();
		}

		argv++;
		argc--;
	}

	if (argc == 0) {
		usage();
	}

	exec_argv = argv;
}


int
main(int argc, char **argv)
{
	int x;

	parse_args(argc, argv);

	x = limit(exec_argv);

	return x;
}
