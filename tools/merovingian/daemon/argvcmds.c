/*
 * The contents of this file are subject to the MonetDB Public License
 * Version 1.1 (the "License"); you may not use this file except in
 * compliance with the License. You may obtain a copy of the License at
 * http://www.monetdb.org/Legal/MonetDBLicense
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
 * Copyright August 2008-2011 MonetDB B.V.
 * All Rights Reserved.
 */

#include "monetdb_config.h"
#include <stdio.h>
#include <unistd.h> /* chdir */
#include <string.h> /* strerror */
#include <errno.h>
#include <sys/types.h>
#include <sys/stat.h>
#include <signal.h> /* kill */

#include <mutils.h> /* MT_lockf */
#include <utils/utils.h>
#include <utils/properties.h>
#include <utils/control.h>

#include "merovingian.h"
#include "argvcmds.h"

int
command_help(int argc, char *argv[])
{
	int exitcode = 0;

	if (argc < 2) {
		printf("usage: monetdbd [ command [ command-options ] ]\n");
		printf("  where command is one of:\n");
		printf("    create, start, stop, get, set, version or help\n");
		printf("  use the help command to get help for a particular command\n");
		printf("  For backwards compatability, when monetdbd is ran without\n");
		printf("  options, it will start the daemon in the default dbfarm\n");
		printf("  location (%s).\n", LOCALSTATEDIR "/monetdb5/dbfarm");
	} else if (strcmp(argv[1], "create") == 0) {
		printf("usage: monetdbd create <dbfarm>\n");
		printf("  Initialises a new dbfarm for a MonetDB Server.  dbfarm\n");
		printf("  must be a path in the filesystem where a directory can be\n");
		printf("  created, or a directory that is writable that already exists.\n");
	} else if (strcmp(argv[1], "start") == 0) {
		printf("usage: monetdbd start [-n] [dbfarm]\n");
		printf("  Starts the monetdbd deamon.  When no dbfarm given, it starts\n");
		printf("  in the default dbfarm (%s).\n", LOCALSTATEDIR "/monetdb5/dbfarm");
		printf("  When -n is given, monetdbd will not fork into the background.");
	} else if (strcmp(argv[1], "stop") == 0) {
		printf("usage: monetdbd stop [dbfarm]\n");
		printf("  Stops a running monetdbd deamon for the given dbfarm, or\n");
		printf("  when none given, the default one (%s).\n", LOCALSTATEDIR "/monetdb5/dbfarm");
	} else if (strcmp(argv[1], "set") == 0) {
		printf("usage: monetdbd set property=value [dbfarm]\n");
		printf("  Sets property to value for the given dbfarm, or when\n");
		printf("  absent, the default (%s).\n", LOCALSTATEDIR "/monetdb5/dbfarm");
		printf("  For a list of properties, use `monetdbd get all`\n");
	} else if (strcmp(argv[1], "get") == 0) {
		printf("usage: monetdbd get <\"all\" | property,...> [dbfarm]\n");
		printf("  Gets value for property for the given dbfarm, or\n");
		printf("  retrieves all properties for the given dbfarm\n");
	} else {
		printf("help: unknown command: %s\n", argv[1]);
		exitcode = 1;
	}

	return(exitcode);
}

int
command_version(void)
{
	printf("MonetDB Database Server v%s (%s)\n",
			MERO_VERSION, MONETDB_RELEASE);
	return(0);
}

int
command_create(int argc, char *argv[])
{
	char path[2048];
	char buf[48];
	char *p;
	char *dbfarm;
	struct stat sb;
	confkeyval phrase[2];

	if (argc != 2) {
		command_help(2, &argv[-1]);
		return(1);
	}

	dbfarm = argv[1];

	/* check if dbfarm actually exists */
	if (stat(dbfarm, &sb) == -1) {
		strncpy(path, dbfarm, sizeof(path) - 1);
		path[sizeof(path) - 1] = '\0';
		p = path;
		/* try to create the dbfarm */
		while ((p = strchr(p + 1, '/')) != NULL) {
			*p = '\0';
			if (stat(path, &sb) == -1 && mkdir(path, 0755)) {
				fprintf(stderr, "unable to create directory '%s': %s\n",
						path, strerror(errno));
				return(1);
			}
			*p = '/';
		}
		if (mkdir(path, 0755)) {
			fprintf(stderr, "unable to create directory '%s': %s\n",
					path, strerror(errno));
			return(1);
		}
	}

	phrase[0].key = NULL;
	if (readProps(phrase, dbfarm) == 0) {
		fprintf(stderr, "directory is already initialised: %s\n", dbfarm);
		return(1);
	}

	generateSalt(buf, sizeof(buf));
	phrase[0].key = "passphrase";
	phrase[0].val = buf;
	phrase[1].key = NULL;
	if (writeProps(phrase, dbfarm) != 0) {
		fprintf(stderr, "unable to create file in directory '%s': %s\n",
				dbfarm, strerror(errno));
		return(1);
	}

	return(0);
}

int
command_get(confkeyval *ckv, int argc, char *argv[])
{
	char *p;
	char *dbfarm = LOCALSTATEDIR "/monetdb5/dbfarm";
	char *property = NULL;
	char *value;
	char buf[512];
	char vbuf[512];
	confkeyval *kv;
	int meropid = -1;

	if (argc < 2 || argc > 3) {
		command_help(2, &argv[-1]);
		return(1);
	}

	if (argc == 3)
		dbfarm = argv[2];

	/* read the merovingian properties from the dbfarm */
	if (readProps(ckv, dbfarm) != 0) {
		fprintf(stderr, "unable to read properties from %s: %s\n",
				dbfarm, strerror(errno));
		return(1);
	}

	property = argv[1];
	if (strcmp(property, "all") == 0) {
		size_t off = 0;
		kv = ckv;
		property = vbuf;
		/* hardwired read-only properties */
		off += snprintf(property, sizeof(vbuf),
				"hostname,dbfarm,status,mserver");
		while (kv->key != NULL) {
			off += snprintf(property + off, sizeof(vbuf) - off,
					",%s", kv->key);
			kv++;
		}
		/* deduced read-only properties */
		off += snprintf(property + off, sizeof(vbuf) - off,
				",mapisock,controlsock");
	}

	/* chdir to dbfarm so we can open relative files (like pidfile) */
	if (chdir(dbfarm) < 0) {
		fprintf(stderr, "could not move to dbfarm '%s': %s\n",
				dbfarm, strerror(errno));
		return(1);
	}

	if (strstr(property, "status") != NULL ||
			strstr(property, "mserver") != NULL)
	{
		/* check if there is a merovingian serving this dbfarm */
		int ret;
		if ((ret = MT_lockf(".merovingian_lock", F_TLOCK, 4, 1)) == -1) {
			/* locking failed, merovingian is running */
			FILE *pf;
			char *pfile = getConfVal(ckv, "pidfile");

			if (pfile != NULL && (pf = fopen(pfile, "r")) != NULL &&
					fgets(buf, sizeof(buf), pf) != NULL)
			{
				meropid = atoi(buf);
			}
		} else {
			if (ret >= 0)
				close(ret); /* release a possible lock */
			meropid = 0;
		}
	}

	printf("   property            value\n");
	while ((p = strtok(property, ",")) != NULL) {
		property = NULL;
		if (strcmp(p, "dbfarm") == 0) {
			value = dbfarm;
		} else if (strcmp(p, "mserver") == 0) {
			if (meropid == 0) {
				value = _mero_mserver;
			} else {
				value = "binary in use cannot be determined "
					"for a running monetdbd";
			}
		} else if (strcmp(p, "hostname") == 0) {
			value = _mero_hostname;
		} else if (strcmp(p, "mapisock") == 0) {
			kv = findConfKey(ckv, "sockdir");
			value = kv->val;
			kv = findConfKey(ckv, "port");
			snprintf(buf, sizeof(buf), "%s/" MERO_SOCK "%d",
					value, kv->ival);
			value = buf;
		} else if (strcmp(p, "controlsock") == 0) {
			kv = findConfKey(ckv, "sockdir");
			value = kv->val;
			kv = findConfKey(ckv, "controlport");
			snprintf(buf, sizeof(buf), "%s/" CONTROL_SOCK "%d",
					value, kv->ival);
			value = buf;
		} else if (strcmp(p, "status") == 0) {
			if (meropid > 0) {
				char *res;
				confkeyval cport[] = {
					{"controlport",  NULL, -1,     INT},
					{"port",         NULL, -1,     INT},
					{ NULL,          NULL,  0, INVALID}
				};

				/* re-read, this time with empty defaults, so we can see
				 * what's available (forward/backwards compatability */
				if (readProps(cport, ".") != 0) {
					fprintf(stderr, "unable to read properties from %s: %s\n",
							dbfarm, strerror(errno));
					return(1);
				}

				/* try to retrieve running merovingian version */
				kv = findConfKey(ckv, "sockdir");
				value = kv->val;
				kv = findConfKey(cport, "controlport");
				if (kv->ival == -1)
					kv = findConfKey(cport, "port"); /* forward compat */
				freeConfFile(cport);
				snprintf(buf, sizeof(buf), "%s/" CONTROL_SOCK "%d",
						value, kv->ival);
				value = control_send(&res, buf, -1, "", "version", 0, NULL);
				if (value != NULL) {
					free(value);
					value = NULL;
				} else {
					if (strncmp(res, "OK\n", 3) != 0) {
						free(res);
					} else {
						value = res + 3;
					}
				}

				snprintf(buf, sizeof(buf),
						"monetdbd[%d] %s is serving this dbfarm",
						meropid, value == NULL ? "(unknown version)" : value);
				if (value != NULL)
					free(res);
				value = buf;
			} else if (meropid < 0) {
				value = "a monetdbd is serving this dbfarm, "
					"but a pidfile was not found/is corrupt";
			} else {
				value = "no monetdbd is serving this dbfarm";
			}
		} else {
			kv = findConfKey(ckv, p);
			if (kv == NULL) {
				fprintf(stderr, "get: no such property: %s\n", p);
				continue;
			}
			if (kv->val == NULL) {
				value = "<unknown>";
			} else {
				value = kv->val;
			}
		}
		printf("%-15s  %s\n", p, value);
	}

	return(0);
}

int
command_set(confkeyval *ckv, int argc, char *argv[])
{
	char *p = NULL;
	char *property;
	char *dbfarm = LOCALSTATEDIR "/monetdb5/dbfarm";
	confkeyval *kv;
	FILE *pfile = NULL;
	char buf[8];
	pid_t meropid;

	if (argc < 2 || argc > 3) {
		command_help(2, &argv[-1]);
		return(1);
	}

	if (argc == 3)
		dbfarm = argv[2];

	/* read the merovingian properties from the dbfarm */
	if (readProps(ckv, dbfarm) != 0) {
		fprintf(stderr, "unable to read properties from %s: %s\n",
				dbfarm, strerror(errno));
		return(1);
	}

	property = argv[1];

	if ((p = strchr(property, '=')) == NULL) {
		fprintf(stderr, "set: need property=value\n");
		command_help(2, &argv[-1]);
		return(1);
	}
	*p++ = '\0';

	/* handle pseudo properties returned by get */
	if (strcmp(property, "hostname") == 0 ||
			strcmp(property, "dbfarm") == 0 ||
			strcmp(property, "mserver") == 0)
	{
		fprintf(stderr, "set: %s is read-only for monetdbd\n", property);
		return(1);
	}
	if (strcmp(property, "mapisock") == 0 ||
			strcmp(property, "controlsock") == 0)
	{
		fprintf(stderr, "set: mapisock and controlsock are deduced from "
				"sockdir, port and controlport, change those instead\n");
		return(1);
	}

	if ((kv = findConfKey(ckv, property)) == NULL) {
		fprintf(stderr, "set: no such property: %s\n", property);
		return(1);
	}
	/* special trick to make it easy to use a different port with one
	 * command */
	if (strcmp(property, "port") == 0) {
		int oport = kv->ival;
		char *e;
		if ((e = setConfVal(kv, p)) != NULL) {
			fprintf(stderr, "set: failed to set property port: %s\n", e);
			free(e);
			return(1);
		}
		kv = findConfKey(ckv, "discoveryport");
		if (kv != NULL && kv->ival == oport && (e = setConfVal(kv, p)) != NULL) {
			fprintf(stderr, "set: failed to set property discoveryport: %s\n", e);
			free(e);
			return(1);
		}
		kv = findConfKey(ckv, "controlport");
		if (kv != NULL && kv->ival == oport + 1) {
			oport = atoi(p);
			snprintf(buf, sizeof(buf), "%d", oport + 1);
			property = "controlport";
			p = buf;
		}
	}
	if ((p = setConfVal(kv, p)) != NULL) {
		fprintf(stderr, "set: failed to set property %s: %s\n", property, p);
		free(p);
		return(1);
	}
	if (writeProps(ckv, dbfarm) != 0) {
		fprintf(stderr, "set: failed to write properties: %s\n",
				strerror(errno));
		return(1);
	}

	property = getConfVal(ckv, "pidfile");

	/* chdir to dbfarm so we can open relative files (like pidfile) */
	if (chdir(dbfarm) < 0) {
		fprintf(stderr, "could not move to dbfarm '%s': %s\n",
				dbfarm, strerror(errno));
		return(1);
	}

	if ((pfile = fopen(property, "r")) != NULL &&
			fgets(buf, sizeof(buf), pfile) != NULL)
	{
		meropid = atoi(buf);
		if (meropid != 0) {
			if (kill(meropid, SIGHUP) == -1) {
				fprintf(stderr, "sending SIGHUP to monetdbd[%d] failed: %s\n",
						(int)meropid, strerror(errno));
				return(1);
			}
		}
	}
	if (pfile != NULL)
		fclose(pfile);

	return(0);
}

int
command_stop(confkeyval *ckv, int argc, char *argv[])
{
	char *dbfarm = LOCALSTATEDIR "/monetdb5/dbfarm";
	char *pidfilename = NULL;
	FILE *pfile = NULL;
	char buf[8];
	pid_t daemon;

	if (argc > 2) {
		command_help(2, &argv[-1]);
		return(1);
	}

	if (argc == 2)
		dbfarm = argv[1];

	/* read the merovingian properties from the dbfarm */
	if (readProps(ckv, dbfarm) != 0) {
		fprintf(stderr, "unable to read properties from %s: %s\n",
				dbfarm, strerror(errno));
		return(1);
	}

	pidfilename = getConfVal(ckv, "pidfile");

	/* chdir to dbfarm so we can open relative files (like pidfile) */
	if (chdir(dbfarm) < 0) {
		fprintf(stderr, "could not move to dbfarm '%s': %s\n",
				dbfarm, strerror(errno));
		return(1);
	}

	if ((pfile = fopen(pidfilename, "r")) == NULL) {
		fprintf(stderr, "unable to open %s: %s\n",
				pidfilename, strerror(errno));
		return(1);
	}

	if (fgets(buf, sizeof(buf), pfile) == NULL) {
		fprintf(stderr, "unable to read from %s: %s\n",
				pidfilename, strerror(errno));
		return(1);
	}
	fclose(pfile);

	daemon = atoi(buf);
	if (daemon == 0) {
		fprintf(stderr, "invalid pid in pidfile %s: %s\n",
				pidfilename, buf);
		return(1);
	}

	if (kill(daemon, SIGTERM) == -1) {
		fprintf(stderr, "unable to shut down monetdbd[%d]: %s\n",
				(int)daemon, strerror(errno));
		return(1);
	}

	return(0);
}
