/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0.  If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 *
 * Copyright 1997 - July 2008 CWI, August 2008 - 2016 MonetDB B.V.
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
#include <mcrypt.h> /* mcrypt_BackendSum */
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
		printf("usage: monetdbd [ command [ command-options ] ] <dbfarm>\n");
		printf("  where command is one of:\n");
		printf("    create, start, stop, get, set, version or help\n");
		printf("  use the help command to get help for a particular command\n");
		printf("  The dbfarm to operate on must always be given to\n");
		printf("  monetdbd explicitly.\n");
	} else if (strcmp(argv[1], "create") == 0) {
		printf("usage: monetdbd create <dbfarm>\n");
		printf("  Initialises a new dbfarm for a MonetDB Server.  dbfarm\n");
		printf("  must be a path in the filesystem where a directory can be\n");
		printf("  created, or a directory that is writable that already exists.\n");
	} else if (strcmp(argv[1], "start") == 0) {
		printf("usage: monetdbd start [-n] <dbfarm>\n");
		printf("  Starts the monetdbd deamon for the given dbfarm.\n");
		printf("  When -n is given, monetdbd will not fork into the background.\n");
	} else if (strcmp(argv[1], "stop") == 0) {
		printf("usage: monetdbd stop <dbfarm>\n");
		printf("  Stops a running monetdbd deamon for the given dbfarm.\n");
	} else if (strcmp(argv[1], "set") == 0) {
		printf("usage: monetdbd set property=value <dbfarm>\n");
		printf("  Sets property to value for the given dbfarm.\n");
		printf("  For a list of properties, use `monetdbd get all`\n");
	} else if (strcmp(argv[1], "get") == 0) {
		printf("usage: monetdbd get <\"all\" | property,...> <dbfarm>\n");
		printf("  Gets value for property for the given dbfarm, or\n");
		printf("  retrieves all properties.\n");
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
	char *p;
	char *dbfarm;
	confkeyval phrase[2];

	if (argc != 2) {
		command_help(2, &argv[-1]);
		return(1);
	}

	dbfarm = argv[1];

	/* check if dbfarm actually exists */
	strncpy(path, dbfarm, sizeof(path) - 1);
	path[sizeof(path) - 1] = '\0';
	p = path;
	while ((p = strchr(p + 1, '/')) != NULL) {
		*p = '\0';
		if (mkdir(path, 0755) == -1 && errno != EEXIST) {
			fprintf(stderr,
				"unable to create directory '%s': %s\n",
				path, strerror(errno));
			return(1);
		}
		*p = '/';
	}
	if (mkdir(dbfarm, 0755) == -1 && errno != EEXIST) {
		fprintf(stderr, "unable to create directory '%s': %s\n",
			dbfarm, strerror(errno));
		return(1);
	}

	phrase[0].key = NULL;
	if (readProps(phrase, dbfarm) == 0) {
		fprintf(stderr, "directory is already initialised: %s\n", dbfarm);
		return(1);
	}

	phrase[0].key = "control";
	phrase[0].val = "false";
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
	char *dbfarm;
	char *property = NULL;
	char *value;
	char buf[512];
	char vbuf[512];
	confkeyval *kv;
	int meropid = -1;

	if (argc != 3) {
		command_help(2, &argv[-1]);
		return(1);
	}

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

			if (pfile != NULL && (pf = fopen(pfile, "r")) != NULL) {
				if (fgets(buf, sizeof(buf), pf) != NULL) {
					meropid = atoi(buf);
				}
				fclose(pf);
			}
		} else {
			if (ret >= 0) {
				/* release a possible lock */
				MT_lockf(".merovingian_lock", F_ULOCK, 4, 1);
				close(ret);
			}
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
				value = "unknown (monetdbd not running)";
			} else {
				char *res;
				/* get binpath from running merovingian */
				kv = findConfKey(ckv, "sockdir");
				value = kv->val;
				kv = findConfKey(ckv, "port");
				snprintf(buf, sizeof(buf), "%s/" CONTROL_SOCK "%d",
						value, kv->ival);
				value = control_send(&res, buf, -1, "", "mserver", 1, NULL);
				if (value != NULL) {
					free(value);
					value = "unknown (failed to connect to monetdbd)";
				} else {
					if (strncmp(res, "OK\n", 3) != 0) {
						free(res);
						value = "unknown (unsupported monetdbd)";
					} else {
						snprintf(buf, sizeof(buf), "%s", res + 3);
						value = buf;
						free(res);
					}
				}
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
			kv = findConfKey(ckv, "port");
			snprintf(buf, sizeof(buf), "%s/" CONTROL_SOCK "%d",
					value, kv->ival);
			value = buf;
		} else if (strcmp(p, "status") == 0) {
			if (meropid > 0) {
				char *res;
				confkeyval cport[] = {
					{"controlport",  NULL, -1,     INT},
					{ NULL,          NULL,  0, INVALID}
				};

				/* re-read, this time with empty defaults, so we can see
				 * what's available (backwards compatability) */
				if (readProps(cport, ".") != 0) {
					fprintf(stderr, "unable to read properties from %s: %s\n",
							dbfarm, strerror(errno));
					return(1);
				}

				/* try to retrieve running merovingian version */
				kv = findConfKey(ckv, "sockdir");
				value = kv->val;
				kv = findConfKey(cport, "controlport"); /* backwards compat */
				if (kv->ival == -1)
					kv = findConfKey(ckv, "port");
				snprintf(buf, sizeof(buf), "%s/" CONTROL_SOCK "%d",
						value, kv->ival);
				freeConfFile(cport);
				value = control_send(&res, buf, -1, "", "version", 1, NULL);
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
		} else if (strcmp(p, "logfile") == 0 || strcmp(p, "pidfile") == 0) {
			kv = findConfKey(ckv, p);
			if (kv->val != NULL && kv->val[0] != '/') {
				snprintf(buf, sizeof(buf), "%s/%s", dbfarm, kv->val);
				value = buf;
			} else {
				value = kv->val;
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
	char h[256];
	char *property;
	char *dbfarm;
	confkeyval *kv;
	FILE *pfile = NULL;
	char buf[8];
	pid_t meropid;

	if (argc != 3) {
		command_help(2, &argv[-1]);
		return(1);
	}

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
				"sockdir and port, change those instead\n");
		return(1);
	}

	if ((kv = findConfKey(ckv, property)) == NULL) {
		fprintf(stderr, "set: no such property: %s\n", property);
		return(1);
	}
	if (strcmp(property, "passphrase") == 0) {
		char dohash = 1;
		/* allow to either set a hash ({X}xxx), or convert the given
		 * string to its hash */
		if (*p == '{') {
			char *q;
			if ((q = strchr(p + 1, '}')) != NULL) {
				*q = '\0';
				if (strcmp(p + 1, MONETDB5_PASSWDHASH) != 0) {
					fprintf(stderr, "set: passphrase hash '%s' incompatible, "
							"expected '%s'\n",
							h, MONETDB5_PASSWDHASH);
					return(1);
				}
				*q = '}';
				dohash = 0;
			}
		}
		if (dohash == 1) {
			p = mcrypt_BackendSum(p, strlen(p));
			snprintf(h, sizeof(h), "{%s}%s", MONETDB5_PASSWDHASH, p);
			free(p);
			p = h;
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

	if ((pfile = fopen(property, "r")) != NULL) {
		if (fgets(buf, sizeof(buf), pfile) != NULL &&
			(meropid = atoi(buf)) != 0 &&
			kill(meropid, SIGHUP) == -1)
		{
			fprintf(stderr, "sending SIGHUP to monetdbd[%d] failed: %s\n",
					(int)meropid, strerror(errno));
			fclose(pfile);
			return(1);
		}
		fclose(pfile);
	}

	return(0);
}

int
command_stop(confkeyval *ckv, int argc, char *argv[])
{
	char *dbfarm;
	char *pidfilename = NULL;
	FILE *pfile = NULL;
	char buf[8];
	pid_t daemon;
	struct timeval tv;
	int i;

	if (argc != 2) {
		command_help(2, &argv[-1]);
		return(1);
	}

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
		fclose(pfile);
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

	/* wait up to 5 seconds for monetdbd to actually stop */
	for (i = 0; i < 10; i++) {
		tv.tv_sec = 0;
		tv.tv_usec = 500;
		select(0, NULL, NULL, NULL, &tv);
		if (kill(daemon, 0) == -1)
			break;
	}

	return(0);
}
