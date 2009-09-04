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
	char *p;
	char *property = NULL, *value = NULL;
	err e;
	int i;
	int state = 0;
	sabdb *orig, *stats, *w;
	confkeyval *kv, *props = getDefaultProps();

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
		} else if (property == NULL) {
			/* first non-option is property, rest is database */
			property = argv[i];
			argv[i] = NULL;
			if (type == SET) {
				if ((p = strchr(property, '=')) == NULL) {
					fprintf(stderr, "set: need property=value\n");
					command_help(2, &argv[-1]);
					exit(1);
				}
				*p++ = '\0';
				value = p;
			}
		}
	}

	if (property == NULL) {
		fprintf(stderr, "%s: need a property argument\n", argv[0]);
		command_help(2, &argv[0]);
		exit(1);
	}

	if (strcmp(property, "shared") == 0) {
		/* mess around with first argument (property) to become value
		 * only, such that can see if it had an argument or not lateron */
		if (type == INHERIT) {
			argv[1] = property;
			property[0] = '\0';
		} else {
			argv[1] = value;
			/* check if tag matches [A-Za-z0-9./]+ */
			while (*value != '\0') {
				if (!(
						(*value >= 'A' && *value <= 'Z') ||
						(*value >= 'a' && *value <= 'z') ||
						(*value >= '0' && *value <= '9') ||
						(*value == '.' || *value == '/')
				   ))
				{
					fprintf(stderr, "set: invalid character '%c' at %d "
							"in tag name '%s'\n",
							*value, (int)(value - argv[1]), argv[1]);
					exit(1);
				}
				value++;
			}
		}
		return(command_merocom(argc, &argv[0], SHARE));
	}

	w = NULL;
	orig = NULL;
	for (i = 1; i < argc; i++) {
		if (argv[i] != NULL) {
			if ((e = SABAOTHgetStatus(&stats, argv[i])) != MAL_SUCCEED) {
				fprintf(stderr, "%s: internal error: %s\n", argv[0], e);
				GDKfree(e);
				exit(2);
			}

			if (stats == NULL) {
				fprintf(stderr, "%s: no such database: %s\n",
						argv[0], argv[i]);
				argv[i] = NULL;
			} else {
				if (stats->state == SABdbRunning) {
					fprintf(stderr, "%s: database '%s' is still running, "
							"stop database first\n", argv[0], stats->dbname);
					SABAOTHfreeStatus(&stats);
					state |= 1;
					continue;
				}
				if (orig == NULL) {
					orig = stats;
					w = stats;
				} else {
					w = w->next = stats;
				}
			}
		}
	}

	for (stats = orig; stats != NULL; stats = stats->next) {
		if (strcmp(property, "name") == 0) {
			char new[512];

			/* special virtual case */
			if (type == INHERIT) {
				fprintf(stderr, "inherit: cannot default to a database name\n");
				state |= 1;
				continue;
			}

			/* check if dbname matches [A-Za-z0-9-_]+ */
			for (i = 0; value[i] != '\0'; i++) {
				if (
						!(value[i] >= 'A' && value[i] <= 'Z') &&
						!(value[i] >= 'a' && value[i] <= 'z') &&
						!(value[i] >= '0' && value[i] <= '9') &&
						!(value[i] == '-') &&
						!(value[i] == '_')
				   )
				{
					fprintf(stderr, "set: invalid character '%c' at %d "
							"in database name '%s'\n",
							value[i], i, value);
					value[0] = '\0';
					state |= 1;
					break;
				}
			}
			if (value[0] == '\0')
				continue;

			/* construct path to new database */
			snprintf(new, 512, "%s", stats->path);
			p = strrchr(new, '/');
			if (p == NULL) {
				fprintf(stderr, "set: non-absolute database path? '%s'\n",
						stats->path);
				state |= 1;
				continue;
			}
			snprintf(p + 1, 512 - (p + 1 - new), "%s", value);

			/* Renaming is as simple as changing the directory name.
			 * Since the logdir is relative to it, we don't need to
			 * bother about that either. */
			if (rename(stats->path, new) != 0) {
				fprintf(stderr, "%s: failed to rename database from "
						"'%s' to '%s': %s\n", argv[0], stats->path, new,
						strerror(errno));
				state |= 1;
			}
		} else {
			char *err;
			readProps(props, stats->path);
			kv = findConfKey(props, property);
			if (kv == NULL) {
				fprintf(stderr, "%s: no such property: %s\n", argv[0], property);
				state |= 1;
				continue;
			}
			if ((err = setConfVal(kv, type == SET ? value : NULL)) != NULL) {
				fprintf(stderr, "%s: %s\n", argv[0], err);
				GDKfree(err);
				state |= 1;
				continue;
			}

			if (strcmp(kv->key, "forward") == 0 && kv->val != NULL) {
				if (strcmp(kv->val, "proxy") != 0 &&
						strcmp(kv->val, "redirect") != 0)
				{
					fprintf(stderr, "%s: expected 'proxy' or 'redirect' for "
							"property 'forward', got: %s\n", argv[0], kv->val);
					state |= 1;
					continue;
				}
			}

			writeProps(props, stats->path);
			freeConfFile(props);
		}
	}

	if (orig != NULL)
		SABAOTHfreeStatus(&orig);
	GDKfree(props);
	exit(state);
}

