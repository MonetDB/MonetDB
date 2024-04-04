/*
 * SPDX-License-Identifier: MPL-2.0
 *
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0.  If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 *
 * Copyright 2024 MonetDB Foundation;
 * Copyright August 2008 - 2023 MonetDB B.V.;
 * Copyright 1997 - July 2008 CWI.
 */

#ifndef _MAPI_QUERYTYPE_H_INCLUDED
#define _MAPI_QUERYTYPE_H_INCLUDED 1

/* this definition is a straight copy from sql/include/sql_query.h */
typedef enum {
	Q_PARSE = 0,
	Q_TABLE = 1,
	Q_UPDATE = 2,
	Q_SCHEMA = 3,
	Q_TRANS = 4,
	Q_PREPARE = 5,
	Q_BLOCK = 6
} mapi_query_t;

#endif /* _MAPI_QUERYTYPE_H_INCLUDED */
