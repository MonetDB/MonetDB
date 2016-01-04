/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0.  If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 *
 * Copyright 1997 - July 2008 CWI, August 2008 - 2016 MonetDB B.V.
 */

#ifndef _SEEN_SABAOTH_H
#define _SEEN_SABAOTH_H 1

typedef struct Ssablist {
	char *val;               /* list value */
	struct Ssablist* next;   /* pointer to the next available value*/
} sablist;

/* only append to this enum, as its numbers are used in
 * serialise/deserialise */
typedef enum {
	SABdbIllegal = 0,
	SABdbRunning,
	SABdbCrashed,
	SABdbInactive,
	SABdbStarting
} SABdbState;

typedef struct Ssabdb {
	char *dbname;            /* database name */
	char *path;              /* path to this database */
	int locked;              /* whether this database is under maintenance */
	SABdbState state;        /* current database state */
	sablist* scens;          /* scenarios available for this database */
	sablist* conns;          /* connections available for this database */
	struct Ssabuplog *uplog; /* sabuplog struct for this database */
	char *uri;               /* URI to connect to this database */
	struct Ssabdb* next;     /* next database */
} sabdb;

typedef struct Ssabuplog {
	int startcntr;     /* the number of start attempts */
	int stopcntr;      /* the number of successful stop attempts */
	int crashcntr;     /* startcntr - stopcntr (for convenience) */
	time_t avguptime;  /* number of seconds up when not crashing */
	time_t maxuptime;  /* longest uptime when not crashing */
	time_t minuptime;  /* shortest uptime when not crashing */
	time_t lastcrash;  /* time of last crash, -1 if none */
	time_t laststart;  /* time of last start */
	time_t laststop;   /* time of last stop, -1 if running */
	int crashavg1;     /* if there was a crash in the last start attempt */
	double crashavg10; /* average of crashes in the last 10 start attempts */
	double crashavg30; /* average of crashes in the last 30 start attempts */
} sabuplog;

#ifdef WIN32
#if !defined(LIBMAL) && !defined(LIBATOMS) && !defined(LIBKERNEL) && !defined(LIBMAL) && !defined(LIBOPTIMIZER) && !defined(LIBSCHEDULER) && !defined(LIBMONETDB5) && !defined(LIBMSABAOTH)
#define msab_export extern __declspec(dllimport)
#else
#define msab_export extern __declspec(dllexport)
#endif
#else
#define msab_export extern
#endif

msab_export void msab_dbpathinit(const char *dbpath);
msab_export void msab_dbfarminit(const char *dbfarm);
msab_export char *msab_getDBfarm(char **ret);
msab_export char *msab_getDBname(char **ret);
msab_export char *msab_marchScenario(const char *lang);
msab_export char *msab_retreatScenario(const char *lang);
msab_export char *msab_marchConnection(const char *host, const int port);
msab_export char *msab_wildRetreat(void);
msab_export char *msab_registerStarting(void);
msab_export char *msab_registerStarted(void);
msab_export char *msab_registerStop(void);
msab_export char *msab_getMyStatus(sabdb** ret);
msab_export char *msab_getStatus(sabdb** ret, char *dbname);
msab_export char *msab_freeStatus(sabdb** ret);
msab_export char *msab_getUplogInfo(sabuplog *ret, const sabdb *db);
msab_export char *msab_serialise(char **ret, const sabdb *db);
msab_export char *msab_deserialise(sabdb **ret, char *sabdb);

#endif
