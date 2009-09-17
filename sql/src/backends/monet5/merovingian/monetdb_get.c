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
command_get(int argc, char *argv[], confkeyval *defprops)
{
	char doall = 1;
	char *p;
	char *property = NULL;
	err e;
	int i;
	sabdb *orig, *stats;
	int twidth = TERMWIDTH;
	char *source, *value;
	confkeyval *kv, *props = getDefaultProps();

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
					case '-':
						if (p[1] == '\0') {
							if (argc - 1 > i) 
								doall = 0;
							i = argc;
							break;
						}
					default:
						fprintf(stderr, "get: unknown option: -%c\n", *p);
						command_help(2, &argv[-1]);
						exit(1);
					break;
				}
			}
			/* make this option no longer available, for easy use
			 * lateron */
			argv[i] = NULL;
		} else if (property == NULL) {
			/* first non-option is property, rest is database */
			property = argv[i];
			argv[i] = NULL;
			if (strcmp(property, "all") == 0) {
				/* die hard leak (can't use constant, strtok modifies
				 * (and hence crashes)) */
				property = GDKstrdup("name,forward,shared,nthreads,master");
			}
		} else {
			doall = 0;
		}
	}

	if (property == NULL) {
		fprintf(stderr, "get: need a property argument\n");
		command_help(2, &argv[-1]);
		exit(1);
	}

	if (doall == 1) {
		/* don't even look at the arguments, because we are instructed
		 * to list all known databases */
		if ((e = SABAOTHgetStatus(&orig, NULL)) != MAL_SUCCEED) {
			fprintf(stderr, "get: internal error: %s\n", e);
			GDKfree(e);
			exit(2);
		}
	} else {
		sabdb *w = NULL;
		orig = NULL;
		for (i = 1; i < argc; i++) {
			if (argv[i] != NULL) {
				if ((e = SABAOTHgetStatus(&stats, argv[i])) != MAL_SUCCEED) {
					fprintf(stderr, "get: internal error: %s\n", e);
					GDKfree(e);
					exit(2);
				}

				if (stats == NULL) {
					fprintf(stderr, "get: no such database: %s\n", argv[i]);
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

	/* name = 15 */
	/* prop = 8 */
	/* source = 7 */
	twidth -= 15 - 2 - 8 - 2 - 7 - 2;
	if (twidth < 6)
		twidth = 6;
	printf("     name          prop     source           value\n");
	while ((p = strtok(property, ",")) != NULL) {
		property = NULL;
		stats = orig;
		while (stats != NULL) {
			/* special virtual case */
			if (strcmp(p, "name") == 0) {
				source = "-";
				value = stats->dbname;
			} else {
				if (mero_running == 1) {
					char *buf;
					e = control_send(&buf, mero_control, -1, stats->dbname, "get");
					if (e != NULL) {
						fprintf(stderr, "get: internal error: %s\n", e);
						free(e);
						exit(2);
					}
					readPropsBuf(props, buf);
					free(buf);
				} else {
					readProps(props, stats->path);
				}
				kv = findConfKey(props, p);
				if (kv == NULL) {
					fprintf(stderr, "get: no such property: %s\n", p);
					stats = NULL;
					continue;
				}
				if (kv->val == NULL) {
					kv = findConfKey(defprops, p);
					source = "default";
					value = kv != NULL && kv->val != NULL ? kv->val : "<unknown>";
				} else {
					source = "local";
					value = kv->val;
				}
			}
			printf("%-15s  %-8s  %-7s  %s\n",
					stats->dbname, p, source, value);
			freeConfFile(props);
			stats = stats->next;
		}
	}

	if (orig != NULL)
		SABAOTHfreeStatus(&orig);
	GDKfree(props);
}

