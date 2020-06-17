#include "test_helper.h"

#define _CONCAT(A, B)   A##B
#define CONCAT(A, B)    _CONCAT(A, B)
#define EXPAND(A) A

#define GET_TPE_ENUM(TPE) CONCAT(monetdb_, TPE)

#define TPE int8_t
#define CHECK_COLUMN_FUNC check_column_bool
#define TPE_ENUM monetdbe_bool
#define MONETDB_COLUMN_TPE monetdbe_column_bool
#include "test_helper_template.h"

#define TPE int8_t
#define CHECK_COLUMN_FUNC check_column_int8_t
#define TPE_ENUM monetdbe_int8_t
#define MONETDB_COLUMN_TPE monetdbe_column_int8_t
#include "test_helper_template.h"

#define TPE int16_t
#define CHECK_COLUMN_FUNC check_column_int16_t
#define TPE_ENUM monetdbe_int16_t
#define MONETDB_COLUMN_TPE monetdbe_column_int16_t
#include "test_helper_template.h"

#define TPE int32_t
#define CHECK_COLUMN_FUNC check_column_int32_t
#define TPE_ENUM monetdbe_int32_t
#define MONETDB_COLUMN_TPE monetdbe_column_int32_t
#include "test_helper_template.h"

#define TPE int64_t
#define CHECK_COLUMN_FUNC check_column_int64_t
#define TPE_ENUM monetdbe_int64_t
#define MONETDB_COLUMN_TPE monetdbe_column_int64_t
#include "test_helper_template.h"

#if HAVE_HGE
#define TPE __int128
#define CHECK_COLUMN_FUNC check_column_int128_t
#define TPE_ENUM monetdbe_int128_t
#define MONETDB_COLUMN_TPE monetdbe_column_int128_t
#include "test_helper_template.h"
#endif

#define TPE size_t
#define CHECK_COLUMN_FUNC check_column_size_t
#define TPE_ENUM monetdbe_size_t
#define MONETDB_COLUMN_TPE monetdbe_column_size_t
#include "test_helper_template.h"

#define TPE float
#define CHECK_COLUMN_FUNC check_column_float
#define TPE_ENUM monetdbe_float
#define MONETDB_COLUMN_TPE monetdbe_column_float
#include "test_helper_template.h"

#define TPE double
#define CHECK_COLUMN_FUNC check_column_double
#define TPE_ENUM monetdbe_double
#define MONETDB_COLUMN_TPE monetdbe_column_double
#include "test_helper_template.h"

#define TPE char*
#define CHECK_COLUMN_FUNC check_column_str
#define TPE_ENUM monetdbe_str
#define MONETDB_COLUMN_TPE monetdbe_column_str
#define EQUALS(A,B) (strcmp(A, B)==0)
#include "test_helper_template.h"

#define TPE monetdbe_data_blob
#define CHECK_COLUMN_FUNC check_column_blob
#define TPE_ENUM monetdbe_blob
#define EQUALS(A,B) ((A).size != (B).size?false:(memcmp((A).data, (B).data, (A).size)) == 0)
#define MONETDB_COLUMN_TPE monetdbe_column_blob
#include "test_helper_template.h"

#define TPE monetdbe_data_date
#define CHECK_COLUMN_FUNC check_column_date
#define TPE_ENUM monetdbe_date
#define EQUALS_DATE(A,B) (((A).day == (B).day) && ((A).month == (B).month) && ((A).year == (B).year))
#define EQUALS EQUALS_DATE
#define MONETDB_COLUMN_TPE monetdbe_column_date
#include "test_helper_template.h"

#define TPE monetdbe_data_time
#define CHECK_COLUMN_FUNC check_column_time
#define TPE_ENUM monetdbe_time
#define EQUALS_TIME(A,B) (((A).ms == (B).ms) && ((A).seconds == (B).seconds) && ((A).minutes == (B).minutes) && ((A).hours == (B).hours))
#define EQUALS EQUALS_TIME
#define MONETDB_COLUMN_TPE monetdbe_column_time
#include "test_helper_template.h"

#define TPE monetdbe_data_timestamp
#define CHECK_COLUMN_FUNC check_column_timestamp
#define TPE_ENUM monetdbe_timestamp
#define EQUALS(A,B) (EQUALS_DATE(A.date, B.date) && EQUALS_TIME(A.time, B.time))
#define MONETDB_COLUMN_TPE monetdbe_column_timestamp
#include "test_helper_template.h"
