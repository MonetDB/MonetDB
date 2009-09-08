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

typedef enum {
	START = 0,
	STOP,
	KILL,
	SHARE
} merocom;
/*	CREATE,
	DESTROY */

static void
command_merocom(int argc, char *argv[], merocom mode)
{
	int doall = 0;
	char path[8096];
	char *res;
	int i;
	err e;
	sabdb *orig;
	sabdb *stats;
	char *type = NULL;
	char *p;
	int ret = 0;

	snprintf(path, 8095, "%s/.merovingian_control", dbfarm);
	path[8095] = '\0';

	switch (mode) {
		case START:
			type = "start";
		break;
		case STOP:
			type = "stop";
		break;
		case KILL:
			type = "kill";
		break;
		case SHARE:
			type = "share";
		break;
	}

	if (argc == 1) {
		/* print help message for this command */
		command_help(2, &argv[-1]);
		exit(1);
	} else if (argc == 0) {
		exit(2);
	}

	/* time to collect some option flags */
	for (i = 1; i < argc; i++) {
		if (argv[i][0] == '-') {
			for (p = argv[i] + 1; *p != '\0'; p++) {
				switch (*p) {
					case 'a':
						doall = 1;
					break;
					case '-':
						if (p[1] == '\0') {
							if (argc - 1 > i) 
								doall = 0;
							i = argc;
							break;
						}
					default:
						fprintf(stderr, "%s: unknown option: -%c\n", type, *p);
						command_help(2, &argv[-1]);
						exit(1);
					break;
				}
			}
			/* make this option no longer available, for easy use
			 * lateron */
			argv[i] = NULL;
		}
	}

	/* if Merovingian isn't running, there's not much we can do */
	if (mero_running == 0) {
		fprintf(stderr, "%s: cannot perform: MonetDB Database Server "
				"(merovingian) is not running\n", type);
		exit(1);
	}

	if (doall == 1) {
		/* don't even look at the arguments, because we are instructed
		 * to list all known databases */
		if ((e = SABAOTHgetStatus(&orig, NULL)) != MAL_SUCCEED) {
			fprintf(stderr, "%s: internal error: %s\n", type, e);
			GDKfree(e);
			exit(2);
		}
	} else {
		sabdb *w = NULL;
		orig = NULL;
		for (i = mode == SHARE ? 2 : 1; i < argc; i++) {
			if (argv[i] != NULL) {
				if ((e = SABAOTHgetStatus(&stats, argv[i])) != MAL_SUCCEED) {
					fprintf(stderr, "%s: internal error: %s\n", type, e);
					GDKfree(e);
					exit(2);
				}

				if (stats == NULL) {
					fprintf(stderr, "%s: no such database: %s\n", type, argv[i]);
					argv[i] = NULL;
				} else {
					if (orig == NULL) {
						orig = stats;
						w = stats;
					} else {
						w = w->next = stats;
					}
				}
			}
		}
	}
	
	stats = orig;
	while (stats != NULL) {
		if (mode == STOP || mode == KILL) {
			if (stats->state == SABdbRunning) {
				printf("%s%sing database '%s'... ", type, mode == STOP ? "p" : "", stats->dbname);
				fflush(stdout);
				control_send(&res, path, 0, stats->dbname, type);
				if (strcmp(res, "OK\n") == 0) {
					printf("done\n");
				} else {
					printf("FAILED:\n%s", res);
					ret = 1;
				}
				free(res);
			} else if (doall != 1) {
				printf("%s: database is not running: %s\n", type, stats->dbname);
			}
		} else if (mode == START) {
			if (stats->state != SABdbRunning) {
				printf("starting database '%s'... ", stats->dbname);
				fflush(stdout);
				control_send(&res, path, 0, stats->dbname, type);
				if (strcmp(res, "OK\n") == 0) {
					printf("done\n");
				} else {
					printf("FAILED:\n%s", res);
					ret = 1;
				}
				free(res);
			} else if (doall != 1 && stats->state == SABdbRunning) {
				printf("%s: database is already running: %s\n",
						type, stats->dbname);
			}
		} else if (mode == SHARE) {
			char *value = argv[1];
			char share[4069];

			/* stay quiet, we're part of monetdb set property=value */

			snprintf(share, sizeof(share), "share=%s", value);
			control_send(&res, path, 0, stats->dbname, share);
			if (strcmp(res, "OK\n") != 0) {
				printf("FAILED:\n%s", res);
				ret = 1;
			}
			free(res);
		}
		stats = stats->next;
	}

	if (orig != NULL)
		SABAOTHfreeStatus(&orig);

	exit(ret);
}

