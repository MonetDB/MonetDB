/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0.  If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 *
 * Copyright 1997 - July 2008 CWI, August 2008 - 2021 MonetDB B.V.
 */

#include "test_helper.h"

#define _CONCAT(A, B)   A##B
#define CONCAT(A, B)    _CONCAT(A, B)
#define EXPAND(A) A

#define GET_TPE_ENUM(TPE) CONCAT(monetdb_, TPE)

#define TPE TEST_TPE_ID(int8_t)
#define CHECK_COLUMN_FUNC check_column_bool
#define TPE_ENUM monetdbe_bool
#define MONETDB_COLUMN_TPE monetdbe_column_bool
#include "test_helper_template.h"

#define TPE TEST_TPE_ID(int8_t)
#define CHECK_COLUMN_FUNC check_column_int8_t
#define TPE_ENUM monetdbe_int8_t
#define MONETDB_COLUMN_TPE monetdbe_column_int8_t
#include "test_helper_template.h"

#define TPE TEST_TPE_ID(int16_t)
#define CHECK_COLUMN_FUNC check_column_int16_t
#define TPE_ENUM monetdbe_int16_t
#define MONETDB_COLUMN_TPE monetdbe_column_int16_t
#include "test_helper_template.h"

#define TPE TEST_TPE_ID(int32_t)
#define CHECK_COLUMN_FUNC check_column_int32_t
#define TPE_ENUM monetdbe_int32_t
#define MONETDB_COLUMN_TPE monetdbe_column_int32_t
#include "test_helper_template.h"

#define TPE TEST_TPE_ID(int64_t)
#define CHECK_COLUMN_FUNC check_column_int64_t
#define TPE_ENUM monetdbe_int64_t
#define MONETDB_COLUMN_TPE monetdbe_column_int64_t
#include "test_helper_template.h"

#ifdef HAVE_HGE
#define TPE TEST_TPE_ID(int128_t)
#define CHECK_COLUMN_FUNC check_column_int128_t
#define TPE_ENUM monetdbe_int128_t
#define MONETDB_COLUMN_TPE monetdbe_column_int128_t
#include "test_helper_template.h"
#endif

#define TPE TEST_TPE_ID(size_t)
#define CHECK_COLUMN_FUNC check_column_size_t
#define TPE_ENUM monetdbe_size_t
#define MONETDB_COLUMN_TPE monetdbe_column_size_t
#include "test_helper_template.h"

#define TPE TEST_TPE_ID(float)
#define CHECK_COLUMN_FUNC check_column_float
#define TPE_ENUM monetdbe_float
#define MONETDB_COLUMN_TPE monetdbe_column_float
#include "test_helper_template.h"

#define TPE TEST_TPE_ID(double)
#define CHECK_COLUMN_FUNC check_column_double
#define TPE_ENUM monetdbe_double
#define MONETDB_COLUMN_TPE monetdbe_column_double
#include "test_helper_template.h"

#define TPE TEST_TPE_ID(str)
#define CHECK_COLUMN_FUNC check_column_str
#define TPE_ENUM monetdbe_str
#define MONETDB_COLUMN_TPE monetdbe_column_str
#define EQUALS(A,B) (strcmp(A, B)==0)
#include "test_helper_template.h"

#define TPE TEST_TPE_ID(blob)
#define CHECK_COLUMN_FUNC check_column_blob
#define TPE_ENUM monetdbe_blob
#define EQUALS(A,B) ((A).size != (B).size?false:(memcmp((A).data, (B).data, (A).size)) == 0)
#define MONETDB_COLUMN_TPE monetdbe_column_blob
#include "test_helper_template.h"

#define TPE TEST_TPE_ID(date)
#define CHECK_COLUMN_FUNC check_column_date
#define TPE_ENUM monetdbe_date
#define EQUALS_DATE(A,B) (((A).day == (B).day) && ((A).month == (B).month) && ((A).year == (B).year))
#define EQUALS EQUALS_DATE
#define MONETDB_COLUMN_TPE monetdbe_column_date
#include "test_helper_template.h"

#define TPE TEST_TPE_ID(time)
#define CHECK_COLUMN_FUNC check_column_time
#define TPE_ENUM monetdbe_time
#define EQUALS_TIME(A,B) (((A).ms == (B).ms) && ((A).seconds == (B).seconds) && ((A).minutes == (B).minutes) && ((A).hours == (B).hours))
#define EQUALS EQUALS_TIME
#define MONETDB_COLUMN_TPE monetdbe_column_time
#include "test_helper_template.h"

#define TPE TEST_TPE_ID(timestamp)
#define CHECK_COLUMN_FUNC check_column_timestamp
#define TPE_ENUM monetdbe_timestamp
#define EQUALS(A,B) (EQUALS_DATE(A.date, B.date) && EQUALS_TIME(A.time, B.time))
#define MONETDB_COLUMN_TPE monetdbe_column_timestamp
#include "test_helper_template.h"
