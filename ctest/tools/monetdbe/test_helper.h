/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0.  If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 *
 * Copyright 1997 - July 2008 CWI, August 2008 - 2021 MonetDB B.V.
 */

#include "monetdbe.h"

#define TEST_TPE(TPE) struct {TPE data; char _is_null;}

#define TEST_TPE_ID(TPE) TEST_##TPE

#define TYPE_DEFTEST_TPE(TPE, ACTUAL_TPE) typedef TEST_TPE(ACTUAL_TPE) TEST_TPE_ID(TPE)


TYPE_DEFTEST_TPE(bool, int8_t);
TYPE_DEFTEST_TPE(int8_t, int8_t);
TYPE_DEFTEST_TPE(int16_t, int16_t);
TYPE_DEFTEST_TPE(int32_t, int32_t);
TYPE_DEFTEST_TPE(int64_t, int64_t);
#ifdef HAVE_HGE
TYPE_DEFTEST_TPE(int128_t, __int128);
#endif
TYPE_DEFTEST_TPE(size_t, size_t);
TYPE_DEFTEST_TPE(float, float);
TYPE_DEFTEST_TPE(double, double);
TYPE_DEFTEST_TPE(str, char*);
TYPE_DEFTEST_TPE(blob, monetdbe_data_blob);
TYPE_DEFTEST_TPE(date, monetdbe_data_date);
TYPE_DEFTEST_TPE(time, monetdbe_data_time);
TYPE_DEFTEST_TPE(timestamp, monetdbe_data_timestamp);

#define GET_ACTUAL_TPE(TPE_ID) ACTUAL_TPE_monetdb_##TPE_ID

#define Null  ._is_null = 1

extern bool check_column_bool      (monetdbe_result* result, size_t column_index, size_t expected_nr_column_entries, TEST_TPE_ID(int8_t) expected_column[]);
extern bool check_column_int8_t    (monetdbe_result* result, size_t column_index, size_t expected_nr_column_entries, TEST_TPE_ID(int8_t)* expected_column);
extern bool check_column_int16_t   (monetdbe_result* result, size_t column_index, size_t expected_nr_column_entries, TEST_TPE_ID(int16_t)* expected_column);
extern bool check_column_int32_t   (monetdbe_result* result, size_t column_index, size_t expected_nr_column_entries, TEST_TPE_ID(int32_t)* expected_column);
extern bool check_column_int64_t   (monetdbe_result* result, size_t column_index, size_t expected_nr_column_entries, TEST_TPE_ID(int64_t)* expected_column);
#ifdef HAVE_HGE
extern bool check_column_int128_t  (monetdbe_result* result, size_t column_index, size_t expected_nr_column_entries, TEST_TPE_ID(int128_t)* expected_column);
#endif
extern bool check_column_size_t    (monetdbe_result* result, size_t column_index, size_t expected_nr_column_entries, TEST_TPE_ID(size_t)* expected_column);
extern bool check_column_float     (monetdbe_result* result, size_t column_index, size_t expected_nr_column_entries, TEST_TPE_ID(float)* expected_column);
extern bool check_column_double    (monetdbe_result* result, size_t column_index, size_t expected_nr_column_entries, TEST_TPE_ID(double)* expected_column);
extern bool check_column_str       (monetdbe_result* result, size_t column_index, size_t expected_nr_column_entries, TEST_TPE_ID(str)* expected_column);
extern bool check_column_blob      (monetdbe_result* result, size_t column_index, size_t expected_nr_column_entries, TEST_TPE_ID(blob)* expected_column);
extern bool check_column_date      (monetdbe_result* result, size_t column_index, size_t expected_nr_column_entries, TEST_TPE_ID(date)* expected_column);
extern bool check_column_time      (monetdbe_result* result, size_t column_index, size_t expected_nr_column_entries, TEST_TPE_ID(time)* expected_column);
extern bool check_column_timestamp (monetdbe_result* result, size_t column_index, size_t expected_nr_column_entries, TEST_TPE_ID(timestamp)* expected_column);

#ifdef HAVE_HGE
#define BIGGEST_INTEGER_TPE int128_t
#define check_column_BIGGEST_INTEGER_TPE check_column_int128_t
#else
#define BIGGEST_INTEGER_TPE int64_t
#define check_column_BIGGEST_INTEGER_TPE check_column_int64_t
#endif


#define _CONCAT(A, B) A##B
#define CONCAT(A, B) _CONCAT(A, B)

#define EVAL(...)  EVAL1(EVAL1(EVAL1(__VA_ARGS__)))
#define EVAL1(...) EVAL2(EVAL2(EVAL2(__VA_ARGS__)))
#define EVAL2(...) EVAL3(EVAL3(EVAL3(__VA_ARGS__)))
#define EVAL3(...) EVAL4(EVAL4(EVAL4(__VA_ARGS__)))
#define EVAL4(...) EVAL5(EVAL5(EVAL5(__VA_ARGS__)))
#define EVAL5(...) __VA_ARGS__

#define EMPTY()

#define DETECTCALL(...) ,
#define _THIRD(_1, _2, _3, ...) _3

#define SECOND(...) SECOND_I(__VA_ARGS__,,)
#define SECOND_I(A,B,...) B
#define SHIFT_IN_ZERO , 0
#define ONE_OR_MANY_UTILITY(...) ONE_OR_MANY_UTILITY_I(__VA_ARGS__, SHIFT_IN_ZERO, X)
#define ONE_OR_MANY_UTILITY_I(A,B,...) B
#define ONE_OR_MANY(...) SECOND(ONE_OR_MANY_UTILITY(__VA_ARGS__), 1)

#define _STRUCT0(V) _THIRD EMPTY() (DETECTCALL V, STRUCT EMPTY() V, V,)
#define _STRUCT1(V, ...) _THIRD EMPTY() (DETECTCALL V, STRUCT EMPTY() V, V,), _STRUCT EMPTY() (__VA_ARGS__)
#define STRUCT(...) {_STRUCT EMPTY() (__VA_ARGS__)}
#define _STRUCT(...)  CONCAT( _STRUCT,ONE_OR_MANY(__VA_ARGS__))EMPTY()(__VA_ARGS__)

#define _COLUMN0(V) ELEMENT(V)
#define _COLUMN1(V, ...) ELEMENT(V), _COLUMN EMPTY() (__VA_ARGS__)
#define _COLUMN(...)  CONCAT( _COLUMN,ONE_OR_MANY(__VA_ARGS__))EMPTY()(__VA_ARGS__)
#define COLUMN(...) {EVAL(_COLUMN (__VA_ARGS__))}

#define ELEMENT(V) { _THIRD EMPTY() (DETECTCALL V, STRUCT EMPTY() V, V) }

#define CHECK_COLUMN(result, column_index, TPE, ...) \
check_column_##TPE ( \
    result, \
    column_index, \
    sizeof((TEST_TPE_ID(TPE)[]) COLUMN(__VA_ARGS__)) /sizeof(((TEST_TPE_ID(TPE)[]) COLUMN(__VA_ARGS__))[0]), \
    (TEST_TPE_ID(TPE)[]) COLUMN(__VA_ARGS__))
