/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0.  If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 *
 * Copyright 1997 - July 2008 CWI, August 2008 - 2017 MonetDB B.V.
 */

/*
 * (author) Fabian Groffen
 *  Cluster support
 * The cluster facilitation currently only deals with (de-)registering of
 * services offered by the local server to other servers.
 * The name of this module is inspired by the Armada setting of ancient
 * times and origanisational structures.  Sabaoth, stands for ``Lord of
 * Hosts'' in an army setting as found in the Bible's New Testament.  This
 * module allows an army of Mservers to be aware of each other on a local
 * machine and redirect to each other when necessary.
 */
#include "monetdb_config.h"
#include "mal_sabaoth.h"
#include <stdio.h> /* fseek, rewind */
#include <unistd.h>	/* unlink and friends */
#include <sys/types.h>
#ifdef HAVE_DIRENT_H
#include <dirent.h> /* readdir, DIR */
#endif
#include <sys/stat.h>
#include <errno.h>
#include <string.h> /* for getting error messages */
#include <assert.h>

#if defined(_MSC_VER) && _MSC_VER >= 1400
#define close _close
#endif

#define excFromMem(TPE, WHRE, X)						\
	do {												\
		str _me = createException(TPE, WHRE, "%s", X);	\
		free(X);										\
		return(_me);									\
	} while (0)

/**
 * Writes the given language to the scenarios file.  If the file doesn't
 * exist, it is created.  Multiple invocations of this function for the
 * same language are ignored.
 */
str SABAOTHmarchScenario(str *lang) {
	str err = msab_marchScenario(*lang);
	if (err != NULL)
		excFromMem(MAL, "sabaoth.marchscenario", err);
	return(MAL_SUCCEED);
}

/**
 * Removes the given language from the scenarios file.  If the scenarios
 * file is empty (before or) after removing the language, the file is
 * removed.
 */
str SABAOTHretreatScenario(str *lang) {
	str err = msab_retreatScenario(*lang);
	if (err != NULL)
		excFromMem(MAL, "sabaoth.retreatscenario", err);
	return(MAL_SUCCEED);
}

/**
 * Writes an URI to the connection file based on the given arguments.
 * If the file doesn't exist, it is created.  Multiple invocations of
 * this function for the same arguments are NOT ignored.  If port is set
 * to <= 0, this function treats the host argument as UNIX domain
 * socket, in which case host must start with a '/'.
 */
str SABAOTHmarchConnection(str *host, int *port) {
	str err = msab_marchConnection(*host, *port);
	if (err != NULL)
		excFromMem(MAL, "sabaoth.marchconnection", err);
	return(MAL_SUCCEED);
}

/**
 * Returns the connection string for the current database, or nil when
 * there is none.  If there are multiple connections defined, only the
 * first is returned.
 */
str SABAOTHgetLocalConnection(str *ret) {
	char data[8096];
	sabdb *stats = NULL;
	str err;

	err = msab_getMyStatus(&stats);
	if (err != NULL)
		excFromMem(MAL, "sabaoth.getlocalconnection", err);

	if (stats == NULL || stats->conns == NULL || stats->conns->val == NULL) {
		*ret = GDKstrdup(str_nil);
	} else {
		if (stats->conns->val[15] == '/') {
			snprintf(data, sizeof(data), "%s?database=%s",
					stats->conns->val, stats->dbname);
		} else {
			snprintf(data, sizeof(data), "%s%s",
					stats->conns->val, stats->dbname);
		}
		*ret = GDKstrdup(data);
	}

	if (stats != NULL)
		SABAOTHfreeStatus(&stats);
	if(*ret == NULL)
		throw(MAL,"sabaoth.getlocalconnection", MAL_MALLOC_FAIL);
	return(MAL_SUCCEED);
}

/**
 * Returns the status as NULL terminated sabdb struct list for the
 * current database.  Since the current database should always exist,
 * this function never returns NULL.
 */
str SABAOTHgetMyStatus(sabdb** ret) {
	str err = msab_getMyStatus(ret);
	if (err != NULL)
		excFromMem(MAL, "sabaoth.getmystatus", err);
	return(MAL_SUCCEED);
}

/**
 * Frees up the sabdb structure returned by getStatus.
 */
str SABAOTHfreeStatus(sabdb** ret) {
	str err = msab_freeStatus(ret);
	if (err != NULL)
		excFromMem(MAL, "sabaoth.freestatus", err);
	return(MAL_SUCCEED);
}
