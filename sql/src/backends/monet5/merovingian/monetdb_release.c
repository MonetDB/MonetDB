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
command_release(int argc, char *argv[])
{
	int i;
	int state = 0;    /* return status */
	int hadwork = 0;  /* did we do anything useful? */

	if (argc == 1) {
		/* print help message for this command */
		command_help(argc + 1, &argv[-1]);
		exit(1);
	}

	/* do for each listed database */
	for (i = 1; i < argc; i++) {
		sabdb *stats;
		err e;
		char *dbname = argv[i];

		/* the argument is the database to take under maintenance, see
		 * what Sabaoth can tell us about it */
		if ((e = SABAOTHgetStatus(&stats, dbname)) != MAL_SUCCEED) {
			fprintf(stderr, "release: internal error: %s\n", e);
			GDKfree(e);
			exit(2);
		}

		if (stats != NULL) {
			char path[8096];

			if (stats->locked != 1) {
				fprintf(stderr, "release: database '%s' is not "
						"under maintenance\n", dbname);
				SABAOTHfreeStatus(&stats);
				hadwork = 1;
				state |= 1;
				continue;
			}

			/* put this database in maintenance mode */
			snprintf(path, 8095, "%s/.maintenance", stats->path);
			if (unlink(path) != 0) {
				fprintf(stderr, "failed to take database '%s' out of "
						" maintenance mode: %s\n", dbname, strerror(errno));
				SABAOTHfreeStatus(&stats);
				hadwork = 1;
				state |= 1;
				continue;
			}
			printf("database '%s' has been taken out of maintenance mode\n",
					dbname);
			SABAOTHfreeStatus(&stats);
		} else {
			fprintf(stderr, "release: no such database: %s\n", dbname);
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

