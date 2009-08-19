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

/* recursive helper function to delete a directory */
static err
deletedir(char *dir)
{
	DIR *d;
	struct dirent *e;
	struct stat s;
	str buf = alloca(sizeof(char) * (PATHLENGTH + 1));
	str data = alloca(sizeof(char) * 8096);
	str path = alloca(sizeof(char) * (PATHLENGTH + 1));

	buf[PATHLENGTH] = '\0';
	d = opendir(dir);
	if (d == NULL) {
		/* silently return if we cannot find the directory; it's
		 * probably already deleted */
		if (errno == ENOENT)
			return(NO_ERR);
		snprintf(data, 8095, "unable to open dir %s: %s",
				dir, strerror(errno));
		return(GDKstrdup(data));
	}
	while ((e = readdir(d)) != NULL) {
		snprintf(path, PATHLENGTH, "%s/%s", dir, e->d_name);
		if (stat(path, &s) == -1) {
			snprintf(data, 8095, "unable to stat file %s: %s",
					path, strerror(errno));
			closedir(d);
			return(GDKstrdup(data));
		}

		if (S_ISREG(s.st_mode) || S_ISLNK(s.st_mode)) {
			if (unlink(path) == -1) {
				snprintf(data, 8095, "unable to unlink file %s: %s",
						path, strerror(errno));
				closedir(d);
				return(GDKstrdup(data));
			}
		} else if (S_ISDIR(s.st_mode)) {
			err er;
			/* recurse, ignore . and .. */
			if (strcmp(e->d_name, ".") != 0 &&
					strcmp(e->d_name, "..") != 0 &&
					(er = deletedir(path)) != NO_ERR)
			{
				closedir(d);
				return(er);
			}
		} else {
			/* fifos, block, char devices etc, we don't do */
			snprintf(data, 8095, "not a regular file: %s", path);
			closedir(d);
			return(GDKstrdup(data));
		}
	}
	closedir(d);
	if (rmdir(dir) == -1) {
		snprintf(data, 8095, "unable to remove directory %s: %s",
				dir, strerror(errno));
		return(GDKstrdup(data));
	}

	return(NO_ERR);
}

static void
command_destroy(int argc, char *argv[])
{
	int i;
	int force = 0;    /* ask for confirmation */
	int state = 0;    /* return status */
	int hadwork = 0;  /* did we do anything useful? */

	if (argc == 1) {
		/* print help message for this command */
		command_help(argc + 1, &argv[-1]);
		exit(1);
	}

	/* walk through the arguments and hunt for "options" */
	for (i = 1; i < argc; i++) {
		if (strcmp(argv[i], "--") == 0) {
			argv[i][0] = '\0';
			break;
		}
		if (argv[i][0] == '-') {
			if (argv[i][1] == 'f') {
				force = 1;
				argv[i][0] = '\0';
			} else {
				fprintf(stderr, "create: unknown option: %s\n", argv[i]);
				command_help(argc + 1, &argv[-1]);
				exit(1);
			}
		}
	}

	if (force == 0) {
		char answ;
		printf("you are about to remove database%s ", argc > 2 ? "s" : "");
		for (i = 1; i < argc; i++)
			printf("%s'%s'", i > 1 ? ", " : "", argv[i]);
		printf("\nALL data in %s will be lost, are you sure? [y/N] ",
				argc > 2 ? "these databases" : "this database");
		if (scanf("%c", &answ) >= 1 &&
				(answ == 'y' || answ == 'Y'))
		{
			/* do it! */
		} else {
			printf("aborted\n");
			exit(0);
		}
	}

	/* do for each listed database */
	for (i = 1; i < argc; i++) {
		sabdb *stats;
		err e;
		char *dbname = argv[i];

		if (dbname[0] == '\0')
			continue;

		/* the argument is the database to destroy, see what Sabaoth can
		 * tell us about it */
		if ((e = SABAOTHgetStatus(&stats, dbname)) != MAL_SUCCEED) {
			fprintf(stderr, "destroy: internal error: %s\n", e);
			GDKfree(e);
			exit(2);
		}

		if (stats != NULL) {
			err e;

			if (stats->state == SABdbRunning) {
				fprintf(stderr, "destroy: database '%s' is still running, "
						"stop database first\n", dbname);
				SABAOTHfreeStatus(&stats);
				state |= 1;
				hadwork = 1;
				continue;
			}

			/* annoyingly we have to delete file by file, and
			 * directories recursively... */
			if ((e = deletedir(stats->path)) != NULL) {
				fprintf(stderr, "destroy: failed to destroy '%s': %s\n",
						argv[1], e);
				GDKfree(e);
				SABAOTHfreeStatus(&stats);
				state |= 1;
				hadwork = 1;
				continue;
			}
			SABAOTHfreeStatus(&stats);
			printf("successfully destroyed database '%s'\n", dbname);
		} else {
			fprintf(stderr, "destroy: no such database: %s\n", dbname);
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

