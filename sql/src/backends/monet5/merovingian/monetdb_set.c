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
	SET = 0,
	INHERIT
} meroset;

static void
command_set(int argc, char *argv[], meroset type)
{
	char *p, *value = NULL;
	char property[24] = "";
	err e;
	int i;
	int state = 0;
	sabdb *stats;
	confkeyval *props = getDefaultProps();

	if (argc >= 1 && argc <= 2) {
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
							i = argc;
							break;
						}
					default:
						fprintf(stderr, "%s: unknown option: -%c\n",
								argv[0], *p);
						command_help(2, &argv[-1]);
						exit(1);
					break;
				}
			}
			/* make this option no longer available, for easy use
			 * lateron */
			argv[i] = NULL;
		} else if (property[0] == '\0') {
			/* first non-option is property, rest is database */
			p = argv[i];
			if (type == SET) {
				if ((p = strchr(argv[i], '=')) == NULL) {
					fprintf(stderr, "set: need property=value\n");
					command_help(2, &argv[-1]);
					exit(1);
				}
				*p = '\0';
				snprintf(property, sizeof(property), "%s", argv[i]);
				*p++ = '=';
				value = p;
				p = argv[i];
			}
			argv[i] = NULL;
		}
	}

	if (property[0] == '\0') {
		fprintf(stderr, "%s: need a property argument\n", argv[0]);
		command_help(2, &argv[0]);
		exit(1);
	}

	/* handle rename separately due to single argument constraint */
	if (strcmp(property, "name") == 0) {
		if (type == INHERIT) {
			fprintf(stderr, "inherit: cannot default to a database name\n");
			exit(1);
		}

		if (argc > 3) {
			fprintf(stderr, "%s: cannot rename multiple databases to "
					"the same name\n", argv[0]);
			exit(1);
		}

		if (mero_running == 0) {
			if ((e = db_rename(stats->dbname, value)) != NULL) {
				fprintf(stderr, "set: %s\n", e);
				free(e);
				exit(1);
			}
		} else {
			char *res;
			char *out;

			out = control_send(&res, mero_control, -1, argv[2], p);
			if (out != NULL || strcmp(res, "OK") != 0) {
				res = out == NULL ? res : out;
				fprintf(stderr, "%s: %s\n", argv[0], res);
				state |= 1;
			}
			free(res);
		}

		GDKfree(props);
		exit(state);
	}

	for (i = 1; i < argc; i++) {
		if (argv[i] == NULL)
			continue;

		if (mero_running == 0) {
			if ((e = SABAOTHgetStatus(&stats, argv[i])) != MAL_SUCCEED) {
				fprintf(stderr, "%s: internal error: %s\n", argv[0], e);
				GDKfree(e);
				exit(2);
			}

			if (stats == NULL) {
				fprintf(stderr, "%s: no such database: %s\n",
						argv[0], argv[i]);
				state |= 1;
				continue;
			}

			if ((e = setProp(stats->path, property, value)) != NULL) {
				fprintf(stderr, "%s: %s\n", argv[0], e);
				free(e);
				state |= 1;
				continue;
			}

			SABAOTHfreeStatus(&stats);
		} else {
			char *res;
			char *out;

			out = control_send(&res, mero_control, 0, argv[i], p);
			if (out != NULL || strcmp(res, "OK") != 0) {
				res = out == NULL ? res : out;
				fprintf(stderr, "%s: %s\n", argv[0], res);
				state |= 1;
			}
			free(res);
		}
	}

	GDKfree(props);
	exit(state);
}

