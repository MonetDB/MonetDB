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
 * Copyright August 2008-2010 MonetDB B.V.
 * All Rights Reserved.
 */

/**
 * Handler for SIGINT, SIGTERM and SIGQUIT.  This starts a graceful
 * shutdown of merovingian.
 */
static void
handler(int sig)
{
	char *signame = NULL;
	switch (sig) {
		case SIGINT:
			signame = "SIGINT";
		break;
		case SIGTERM:
			signame = "SIGTERM";
		break;
		case SIGQUIT:
			signame = "SIGQUIT";
		break;
		default:
			assert(0);
	}
	Mfprintf(stdout, "caught %s, starting shutdown sequence\n", signame);
	_mero_keep_listening = 0;
}

/**
 * Handler for SIGHUP, causes logfiles to be reopened, if not attached
 * to a terminal.
 */
static void
huphandler(int sig)
{
	(void)sig;

	if (!isatty(_mero_topdp->out) || !isatty(_mero_topdp->err)) {
		int t;
		time_t now = time(NULL);
		struct tm *tmp = localtime(&now);
		char mytime[20];

		/* have to make sure the logger is not logging anything */
		pthread_mutex_lock(&_mero_topdp_lock);

		strftime(mytime, sizeof(mytime), "%Y-%m-%d %H:%M:%S", tmp);

		if (_mero_msglogfile != NULL) {
			/* reopen original file */
			t = open(_mero_msglogfile, O_WRONLY | O_APPEND | O_CREAT,
					S_IRUSR | S_IWUSR);
			if (t == -1) {
				Mfprintf(stderr, "forced to ignore SIGHUP: unable to open "
						"'%s': %s\n", _mero_msglogfile, strerror(errno));
			} else {
				Mfprintf(_mero_streamout, "%s END merovingian[" LLFMT "]: "
						"caught SIGHUP, closing logfile\n",
						mytime, (long long int)_mero_topdp->next->pid);
				fflush(_mero_streamout);
				fclose(_mero_streamout);
				_mero_topdp->out = t;
				_mero_streamout = fdopen(_mero_topdp->out, "a");
				Mfprintf(_mero_streamout, "%s BEG merovingian[" LLFMT "]: "
						"reopening logfile\n",
						mytime, (long long int)_mero_topdp->next->pid);
			}
		}
		if (_mero_errlogfile != NULL) {
			/* reopen original file */
			if (strcmp(_mero_msglogfile, _mero_errlogfile) == 0) {
				_mero_topdp->err = _mero_topdp->out;
			} else {
				t = open(_mero_errlogfile, O_WRONLY | O_APPEND | O_CREAT,
						S_IRUSR | S_IWUSR);
				if (t == -1) {
					Mfprintf(stderr, "forced to ignore SIGHUP: "
							"unable to open '%s': %s\n",
							_mero_errlogfile, strerror(errno));
				} else {
					Mfprintf(_mero_streamerr, "%s END merovingian[" LLFMT "]: "
							"caught SIGHUP, closing logfile\n",
							mytime, (long long int)_mero_topdp->next->pid);
					fclose(_mero_streamerr);
					_mero_topdp->err = t;
					_mero_streamerr = fdopen(_mero_topdp->err, "a");
					Mfprintf(_mero_streamerr, "%s BEG merovingian[" LLFMT "]: "
							"reopening logfile\n",
							mytime, (long long int)_mero_topdp->next->pid);
				}
			}
		}

		/* logger go ahead! */
		pthread_mutex_unlock(&_mero_topdp_lock);
	} else {
		Mfprintf(stdout, "caught SIGHUP, ignoring signal "
				"(logging to terminal)");
	}
}

/**
 * Handles SIGCHLD signals, that is, signals that a parent recieves
 * about its children.  This handler deals with terminated children, by
 * deregistering them from the internal administration (_mero_topdp)
 * with the necessary cleanup.
 */
static void
childhandler(int sig, siginfo_t *si, void *unused)
{
	dpair p, q;

	(void)sig;
	(void)unused;

	/* wait for the child to get properly terminated, hopefully filling
	 * in the siginfo struct on FreeBSD */
	wait(NULL);

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
			logFD(p->out, "MSG", p->dbname, (long long int)p->pid, _mero_streamout);
			logFD(p->err, "ERR", p->dbname, (long long int)p->pid, _mero_streamerr);
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
				Mfprintf(stdout, "database '%s' (%lld) was killed by signal "
						"%d\n", p->dbname,
						(long long int)p->pid, si->si_status);
			} else if (si->si_code == CLD_DUMPED) {
				Mfprintf(stdout, "database '%s' (%lld) has crashed "
						"(dumped core)\n", p->dbname,
						(long long int)p->pid);
			}
			if (p->dbname)
				GDKfree(p->dbname);
			GDKfree(p);
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

