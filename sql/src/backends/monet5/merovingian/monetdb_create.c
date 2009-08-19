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

static char seedChars[] = {'a', 'b', 'c', 'd', 'e', 'f', 'g', 'h', 'i', 'j',
	'k', 'l', 'm', 'n', 'o', 'p', 'q', 'r', 's', 't', 'u', 'v', 'w', 'x',
	'y', 'z', 'A', 'B', 'C', 'D', 'E', 'F', 'G', 'H', 'I', 'J', 'K', 'L',
	'M', 'N', 'O', 'P', 'Q', 'R', 'S', 'T', 'U', 'V', 'W', 'X', 'Y', 'Z',
	'1', '2', '3', '4', '5', '6', '7', '8', '9', '0'};

static void
command_create(int argc, char *argv[])
{
	int i;
	int maintenance = 0;  /* create locked database */
	int state = 0;        /* return status */
	int hadwork = 0;      /* if we actually did something */

	if (argc == 1) {
		/* print help message for this command */
		command_help(2, &argv[-1]);
		exit(1);
	}
	
	/* walk through the arguments and hunt for "options" */
	for (i = 1; i < argc; i++) {
		if (strcmp(argv[i], "--") == 0) {
			argv[i][0] = '\0';
			break;
		}
		if (argv[i][0] == '-') {
			if (argv[i][1] == 'l') {
				maintenance = 1;
				argv[i][0] = '\0';
			} else {
				fprintf(stderr, "create: unknown option: %s\n", argv[i]);
				command_help(argc + 1, &argv[-1]);
				exit(1);
			}
		}
	}

	/* do for each listed database */
	for (i = 1; i < argc; i++) {
		sabdb *stats;
		err e;
		char *dbname = argv[i];
		int c;

		/* check if dbname matches [A-Za-z0-9-_]+ */
		for (c = 0; dbname[c] != '\0'; c++) {
			if (
					!(dbname[c] >= 'A' && dbname[c] <= 'Z') &&
					!(dbname[c] >= 'a' && dbname[c] <= 'z') &&
					!(dbname[c] >= '0' && dbname[c] <= '9') &&
					!(dbname[c] == '-') &&
					!(dbname[c] == '_')
			   )
			{
				fprintf(stderr, "create: invalid character '%c' at %d "
						"in database name '%s'\n",
						dbname[c], c, dbname);
				dbname[0] = '\0';
				state |= 1;
				/* avoid usage message */
				hadwork = 1;
				break;
			}
		}
		if (dbname[0] == '\0')
			continue;

		/* the argument is the database to create, see what Sabaoth can
		 * tell us about it */
		if ((e = SABAOTHgetStatus(&stats, dbname)) != MAL_SUCCEED) {
			fprintf(stderr, "create: internal error: %s\n", e);
			GDKfree(e);
			exit(2);
		}

		if (stats == NULL) { /* sabaoth doesn't know, green light for us! */
			char path[8096];
			FILE *f;
			int size;
			char buf[48];

			snprintf(path, 8095, "%s/%s", dbfarm, dbname);
			path[8095] = '\0';
			if (mkdir(path, 0755) == -1) {
				fprintf(stderr, "create: unable to create %s: %s\n",
						argv[1], strerror(errno));

				SABAOTHfreeStatus(&stats);
				state |= 1;
				continue;
			}
			/* if we should put this database in maintenance, make sure
			 * no race condition ever can happen, by putting it into
			 * maintenance before it even exists for Merovingian */
			if (maintenance == 1) {
				snprintf(path, 8095, "%s/%s/.maintenance", dbfarm, dbname);
				fclose(fopen(path, "w"));
			}
			/* avoid GDK from making fugly complaints */
			snprintf(path, 8095, "%s/%s/.gdk_lock", dbfarm, dbname);
			f = fopen(path, "w");
			/* to all insanity, .gdk_lock is "valid" if it contains a
			 * ':', which it does by pure coincidence of a time having a
			 * ':' in there twice... */
			if (fwrite("bla:", 1, 4, f) < 4)
				fprintf(stderr, "create: failure in writing lock file\n");
			fclose(f);
			/* generate a vault key */
			size = rand();
			size = (size % (36 - 20)) + 20;
			for (c = 0; c < size; c++) {
				buf[c] = seedChars[rand() % 62];
			}
			for ( ; c < 48; c++) {
				buf[c] = '\0';
			}
			snprintf(path, 8095, "%s/%s/.vaultkey", dbfarm, dbname);
			f = fopen(path, "w");
			if (fwrite(buf, 1, 48, f) < 48)
				fprintf(stderr, "create: failure in writing vaultkey file\n");
			fclose(f);

			/* without an .uplog file, Merovingian won't work, this
			 * needs to be last to avoid race conditions */
			snprintf(path, 8095, "%s/%s/.uplog", dbfarm, dbname);
			fclose(fopen(path, "w"));

			printf("successfully created database '%s'%s\n", dbname,
					(maintenance == 1 ? " in maintenance mode" : ""));
		} else {
			fprintf(stderr, "create: database '%s' already exists\n", dbname);

			SABAOTHfreeStatus(&stats);
			state |= 1;
		}

		hadwork = 1;
	}

	if (hadwork == 0) {
		command_help(2, &argv[-1]);
		state |= 1;
	}
	exit(state);
}

