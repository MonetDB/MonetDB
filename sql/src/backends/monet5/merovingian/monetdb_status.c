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
 * Copyright August 2008-2009 MonetDB B.V.
 * All Rights Reserved.
 */


static void
printStatus(sabdb *stats, int mode, int twidth)
{
	sabuplog uplog;
	str e;

	if ((e = SABAOTHgetUplogInfo(&uplog, stats)) != MAL_SUCCEED) {
		fprintf(stderr, "status: internal error: %s\n", e);
		GDKfree(e);
		return;
	}

	if (mode == 1) {
		/* short one-line (default) mode */
		char *state;
		char uptime[12];
		char avg[8];
		char *crash;
		char *dbname;

		switch (stats->state) {
			case SABdbRunning:
				state = "running";
			break;
			case SABdbCrashed:
				state = "crashed";
			break;
			case SABdbInactive:
				state = "stopped";
			break;
			default:
				state = "unknown";
			break;
		}
		/* override if locked for brevity */
		if (stats->locked == 1)
			state = "locked ";

		if (uplog.lastcrash == -1) {
			crash = "-";
		} else {
			struct tm *t;
			crash = alloca(sizeof(char) * 20);
			t = localtime(&uplog.lastcrash);
			strftime(crash, 20, "%Y-%m-%d %H:%M:%S", t);
		}

		if (stats->state != SABdbRunning) {
			uptime[0] = '\0';
		} else {
			secondsToString(uptime, time(NULL) - uplog.laststart, 3);
		}

		/* cut too long database names */
		dbname = alloca(sizeof(char) * (twidth + 1));
		abbreviateString(dbname, stats->dbname, twidth);
		/* dbname | state | uptime | health */
		secondsToString(avg, uplog.avguptime, 1);
			printf("%-*s  %s %12s", twidth,
					dbname, state, uptime);
		if (uplog.startcntr)
			printf("  %3d%%, %3s  %s",
					100 - (uplog.crashcntr * 100 / uplog.startcntr),
					avg, crash);
		printf("\n");
	} else if (mode == 2) {
		/* long mode */
		char *state;
		sablist *entry;
		char up[32];
		struct tm *t;

		switch (stats->state) {
			case SABdbRunning:
				state = "running";
			break;
			case SABdbCrashed:
				state = "crashed";
			break;
			case SABdbInactive:
				state = "stopped";
			break;
			default:
				state = "unknown";
			break;
		}

		printf("%s:\n", stats->dbname);
		printf("  location: %s\n", stats->path);
		printf("  database name: %s\n", stats->dbname);
		printf("  state: %s\n", state);
		printf("  locked: %s\n", stats->locked == 1 ? "yes" : "no");
		entry = stats->scens;
		printf("  scenarios:");
		if (entry == NULL) {
			printf(" (none)");
		} else while (entry != NULL) {
			printf(" %s", entry->val);
			entry = entry->next;
		}
		printf("\n");
		entry = stats->conns;
		printf("  connections:");
		if (entry == NULL) {
			printf(" (none)");
		} else while (entry != NULL) {
			printf(" %s", entry->val);
			entry = entry->next;
		}
		printf("\n");
		printf("  start count: %d\n  stop count: %d\n  crash count: %d\n",
				uplog.startcntr, uplog.stopcntr, uplog.crashcntr);
		if (stats->state == SABdbRunning) {
			secondsToString(up, time(NULL) - uplog.laststart, 999);
			printf("  current uptime: %s\n", up);
		}
		secondsToString(up, uplog.avguptime, 999);
		printf("  average uptime: %s\n", up);
		secondsToString(up, uplog.maxuptime, 999);
		printf("  maximum uptime: %s\n", up);
		secondsToString(up, uplog.minuptime, 999);
		printf("  minimum uptime: %s\n", up);
		if (uplog.lastcrash != -1) {
			t = localtime(&uplog.lastcrash);
			strftime(up, 32, "%Y-%m-%d %H:%M:%S", t);
		} else {
			sprintf(up, "(unknown)");
		}
		printf("  last crash: %s\n", up);
		if (uplog.laststart != -1) {
			t = localtime(&uplog.laststart);
			strftime(up, 32, "%Y-%m-%d %H:%M:%S", t);
		} else {
			sprintf(up, "(unknown)");
		}
		printf("  last start: %s\n", up);
		printf("  average of crashes in the last start attempt: %d\n",
				uplog.crashavg1);
		printf("  average of crashes in the last 10 start attempts: %.2f\n",
				uplog.crashavg10);
		printf("  average of crashes in the last 30 start attempts: %.2f\n",
				uplog.crashavg30);
	} else {
		/* this shows most used properties, and is shown also for modes
		 * that are added but we don't understand (yet) */
		char buf[64];
		char min[8], avg[8], max[8];
		struct tm *t;
		/* dbname, status -- since, crash averages */

		switch (stats->state) {
			case SABdbRunning: {
				char up[32];
				t = localtime(&uplog.laststart);
				strftime(buf, 64, "up since %Y-%m-%d %H:%M:%S, ", t);
				secondsToString(up, time(NULL) - uplog.laststart, 999);
				strcat(buf, up);
			} break;
			case SABdbCrashed:
				t = localtime(&uplog.lastcrash);
				strftime(buf, 64, "crashed on %Y-%m-%d %H:%M:%S", t);
			break;
			case SABdbInactive:
				snprintf(buf, 64, "not running");
			break;
			default:
				snprintf(buf, 64, "unknown");
			break;
		}
		if (stats->locked == 1)
			strcat(buf, ", locked");
		printf("database %s, %s\n", stats->dbname, buf);
		printf("  crash average: %d.00 %.2f %.2f (over 1, 15, 30 starts) "
				"in total %d crashes\n",
				uplog.crashavg1, uplog.crashavg10, uplog.crashavg30,
				uplog.crashcntr);
		secondsToString(min, uplog.minuptime, 1);
		secondsToString(avg, uplog.avguptime, 1);
		secondsToString(max, uplog.maxuptime, 1);
		printf("  uptime stats (min/avg/max): %s/%s/%s over %d runs\n",
				min, avg, max, uplog.stopcntr);
	}
}

static void
command_status(int argc, char *argv[])
{
	int doall = 1; /* we default to showing all */
	int mode = 1;  /* 0=crash, 1=short, 2=long */
	char *state = "rscl"; /* contains states to show */
	int i;
	char *p;
	str e;
	sabdb *stats;
	sabdb *orig;
	int t;
	int dbwidth = 0;
	int twidth = TERMWIDTH;

	if (argc == 0) {
		exit(2);
	}

	/* time to collect some option flags */
	for (i = 1; i < argc; i++) {
		if (argv[i][0] == '-') {
			for (p = argv[i] + 1; *p != '\0'; p++) {
				switch (*p) {
					case 'c':
						mode = 0;
					break;
					case 'l':
						mode = 2;
					break;
					case 's':
						if (*(p + 1) != '\0') {
							state = ++p;
						} else if (i + 1 < argc && argv[i + 1][0] != '-') {
							state = argv[++i];
						} else {
							fprintf(stderr, "status: -s needs an argument\n");
							command_help(2, &argv[-1]);
							exit(1);
						}
						for (p = state; *p != '\0'; p++) {
							switch (*p) {
								case 'r': /* running (started) */
								case 's': /* stopped */
								case 'c': /* crashed */
								case 'l': /* locked */
								break;
								default:
									fprintf(stderr, "status: unknown flag for -s: -%c\n", *p);
									command_help(2, &argv[-1]);
									exit(1);
								break;
							}
						}
						p--;
					break;
					case '-':
						if (p[1] == '\0') {
							if (argc - 1 > i) 
								doall = 0;
							i = argc;
							break;
						}
					default:
						fprintf(stderr, "status: unknown option: -%c\n", *p);
						command_help(2, &argv[-1]);
						exit(1);
					break;
				}
			}
			/* make this option no longer available, for easy use
			 * lateron */
			argv[i] = NULL;
		} else {
			doall = 0;
		}
	}

	if ((e = SABAOTHgetStatus(&orig, NULL)) != MAL_SUCCEED) {
		fprintf(stderr, "status: internal error: %s\n", e);
		GDKfree(e);
		exit(2);
	}
	/* don't even look at the arguments, if we are instructed
	 * to list all known databases */
	if (doall != 1) {
		sabdb *w = NULL;
		sabdb *top = w;
		sabdb *prev;
		for (i = 1; i < argc; i++) {
			t = 0;
			if (argv[i] != NULL) {
				prev = NULL;
				for (stats = orig; stats != NULL; stats = stats->next) {
					if (glob(argv[i], stats->dbname)) {
						t = 1;
						/* move out of orig into w, such that we can't
						 * get double matches in the same output list
						 * (as side effect also avoids a double free
						 * lateron) */
						if (w == NULL) {
							top = w = stats;
						} else {
							w = w->next = stats;
						}
						if (prev == NULL) {
							orig = stats->next;
							/* little hack to revisit the now top of the
							 * list */
							w->next = orig;
							stats = w;
							continue;
						} else {
							prev->next = stats->next;
							stats = prev;
						}
					}
					prev = stats;
				}
				if (w != NULL)
					w->next = NULL;
				if (t == 0) {
					fprintf(stderr, "status: no such database: %s\n", argv[i]);
					argv[i] = NULL;
				}
			}
		}
		SABAOTHfreeStatus(&orig);
		orig = top;
	}
	/* calculate width, BUG: SABdbState selection is only done at
	 * printing */
	for (stats = orig; stats != NULL; stats = stats->next) {
		if ((t = strlen(stats->dbname)) > dbwidth)
			dbwidth = t;
	}

	if (mode == 1 && orig != NULL) {
		/* print header for short mode, state -- last crash = 54 chars */
		twidth -= 54;
		if (twidth < 6)
			twidth = 6;
		if (dbwidth < 14)
			dbwidth = 14;
		if (dbwidth < twidth)
			twidth = dbwidth;
		printf("%*sname%*s  ",
				twidth - 4 /* name */ - ((twidth - 4) / 2), "",
				(twidth - 4) / 2, "");
		printf(" state     uptime       health       last crash\n");
	}

	for (p = state; *p != '\0'; p++) {
		int curLock = 0;
		SABdbState curMode = SABdbIllegal;
		switch (*p) {
			case 'r':
				curMode = SABdbRunning;
			break;
			case 's':
				curMode = SABdbInactive;
			break;
			case 'c':
				curMode = SABdbCrashed;
			break;
			case 'l':
				curLock = 1;
			break;
		}
		stats = orig;
		while (stats != NULL) {
			if (stats->locked == curLock &&
					(curLock == 1 || 
					 (curLock == 0 && stats->state == curMode)))
				printStatus(stats, mode, twidth);
			stats = stats->next;
		}
	}

	if (orig != NULL)
		SABAOTHfreeStatus(&orig);
}

/* vim:set ts=4 sw=4 noexpandtab: */
