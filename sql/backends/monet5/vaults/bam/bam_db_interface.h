/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0.  If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 *
 * Copyright 1997 - July 2008 CWI, August 2008 - 2017 MonetDB B.V.
 */

/*
 * (author) R Cijvat
 * The code in this file handles all communication that is done with
 * the running database.
 */


#ifndef _BAM_DB_INTERFACE_H
#define _BAM_DB_INTERFACE_H

#include "sql_execute.h"
#include "sql_mvc.h"

#include "bam_wrapper.h"


/* macro to run SQL and incorporate logging; sql should be (char **) */
#ifdef BAM_DEBUG
#define RUN_SQL(cntxt, sql, descr, msg) { \
	char *sql_log = prepare_for_log(*sql, FALSE); \
	if(sql_log != NULL) { \
		TO_LOG("%s\n", sql_log); \
		GDKfree(sql_log); \
	} \
	msg = SQLstatementIntern(cntxt, sql, descr, TRUE, FALSE, NULL); \
}
#else
#define RUN_SQL(cntxt, sql, descr, msg) { \
	msg = SQLstatementIntern(cntxt, sql, descr, TRUE, FALSE, NULL); \
}
#endif


/* SQL code for creating header tables; Put in the header to enable
 * other files to use these SQL queries as arguments for making calls
 * to create_table_if_not_exists function */
str bind_bam_schema(mvc * m, sql_schema ** ret);
str bind_table(mvc * m, sql_schema * s,
			   str tablename, sql_table ** ret);
str next_file_id(mvc * m, sql_table * files, lng * next_file_id);
str create_alignment_storage_0(Client cntxt, str descr, bam_wrapper * bw);
str create_alignment_storage_1(Client cntxt, str descr, bam_wrapper * bw);
str copy_into_db(Client cntxt, bam_wrapper * bw);
str drop_file(Client cntxt, str descr, lng file_id, sht dbschema);

#endif
