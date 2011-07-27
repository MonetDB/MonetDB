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

#ifndef BPMSTORAGE_H
#define BPMSTORAGE_H

#include "sql_storage.h"
#include "bat/bat_utils.h"
#include "bat/bat_logger.h"
#include "bat/bat_storage.h"

#define BPM_DEFAULT 	8

#define BPM_SPLIT 	(1024*1024)

typedef struct sql_bpm {
	char *name;
	oid   pid;
	sht   type;	/* bat tail type only kept in memory */
	sht   nr;	/* total */
	sht   created;	/* from created on the parts are newly added  */
	sht   sz;
	sql_delta *parts;
} sql_bpm;

typedef struct sql_dbpm {
	char *name;
	oid   pid;
	sht   nr;	/* total */
	sht   created;	/* from created on the parts are newly added */
	sht   sz;
	sql_dbat *parts;
} sql_dbpm;

#endif /*BPMSTORAGE_H */

