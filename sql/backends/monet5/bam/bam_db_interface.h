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
 * Copyright August 2008-2014 MonetDB B.V.
 * All Rights Reserved.
 */

/*
 * (author) R Cijvat
 * The code in this file handles all communication that is done with
 * the running database.
 */


#ifndef _BAM_DB_INTERFACE_H
#define _BAM_DB_INTERFACE_H

#include "sql_scenario.h"
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
	msg = SQLstatementIntern(cntxt, sql, descr, TRUE, FALSE); \
}
#else
#define RUN_SQL(cntxt, sql, descr, msg) { \
	msg = SQLstatementIntern(cntxt, sql, descr, TRUE, FALSE); \
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
