/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0.  If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 *
 * Copyright 1997 - July 2008 CWI, August 2008 - 2017 MonetDB B.V.
 */

/*
 * @f logger
 * @a N. J. Nes
 * @v 2.0
 * @+ The Transaction Logger
 * In the philosophy of MonetDB, transaction management overhead should only
 * be paid when necessary. Transaction management is for this purpose
 * implemented as a separate module and applications are required to
 * obey the transaction policy, e.g. obtaining/releasing locks.
 *
 * This module is designed to support efficient logging of the SQL database.
 * Once loaded, the SQL compiler will insert the proper calls at
 * transaction commit to include the changes in the log file.
 *
 * The logger uses a directory to store its log files. One master log file
 * stores information about the version of the logger and the transaction
 * log files. This file is a simple ascii file with the following format:
 *  @code{6DIGIT-VERSION\n[log file number \n]*]*}
 * The transaction log files have a binary format, which stores fixed size
 * logformat headers (flag,nr,bid), where the flag is the type of update logged.
 * The nr field indicates how many changes there were (in case of inserts/deletes).
 * The bid stores the bid identifier.
 *
 * The key decision to be made by the user is the location of the log file.
 * Ideally, it should be stored in fail-safe environment, or at least
 * the log and databases should be on separate disk columns.
 *
 * This file system may reside on the same hardware as the database server
 * and therefore the writes are done to the same disk, but could also
 * reside on another system and then the changes are flushed through the network.
 * The logger works under the assumption that it is called to safeguard
 * updates on the database when it has an exclusive lock on
 * the latest version. This lock should be guaranteed by the calling
 * transaction manager first.
 *
 * Finding the updates applied to a BAT is relatively easy, because each
 * BAT contains a delta structure. On commit these changes are
 * written to the log file and the delta management is reset. Since each
 * commit is written to the same log file, the beginning and end are
 * marked by a log identifier.
 *
 * A server restart should only (re)process blocks which are completely
 * written to disk. A log replay therefore ends in a commit or abort on
 * the changed bats. Once all logs have been read, the changes to
 * the bats are made persistent, i.e. a bbp sub-commit is done.
 *
 * @+ Module Definition
 */
/*
 * @+ Implementation Code
 */
#include "monetdb_config.h"
#include "gdk.h"
#include "gdk_logger.h"
#include "mal.h"
#include "mal_exception.h"

/* the wrappers */
mal_export str logger_create_wrap( logger *L, int *debug, str *fn, str *dirname, int *version);

str
logger_create_wrap( logger *L, int *debug, str *fn, str *dirname, int *version)
{
	logger *l = logger_create(*debug, *fn, *dirname, *version, NULL, NULL, 0);

	if (l) {
		*(logger**)L = l;
		return MAL_SUCCEED;
	}
	throw(MAL, "logger.create", OPERATION_FAILED "database %s version %d" ,
		*dirname, *version);
}

mal_export str logger_destroy_wrap(void *ret, logger *L ) ;

str
logger_destroy_wrap(void *ret, logger *L )
{
	logger *l = *(logger**)L;
	(void) ret;
	if (l) {
		logger_destroy(l);
		return MAL_SUCCEED;
	}
	throw(MAL, "logger.destroy", OPERATION_FAILED);
}
