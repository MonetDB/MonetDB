
#include "monetdbe.h"

monetdbe_export bool check_column_bool      (monetdbe_result* result, size_t column_index, size_t expected_nr_column_entries, int8_t* expected_column);
monetdbe_export bool check_column_int8_t    (monetdbe_result* result, size_t column_index, size_t expected_nr_column_entries, int8_t* expected_column);
monetdbe_export bool check_column_int16_t   (monetdbe_result* result, size_t column_index, size_t expected_nr_column_entries, int16_t* expected_column);
monetdbe_export bool check_column_int32_t   (monetdbe_result* result, size_t column_index, size_t expected_nr_column_entries, int32_t* expected_column);
monetdbe_export bool check_column_int64_t   (monetdbe_result* result, size_t column_index, size_t expected_nr_column_entries, int64_t* expected_column);
#if HAVE_HGE
monetdbe_export bool check_column_int128_t  (monetdbe_result* result, size_t column_index, size_t expected_nr_column_entries, __int128* expected_column);
#endif
monetdbe_export bool check_column_size_t    (monetdbe_result* result, size_t column_index, size_t expected_nr_column_entries, size_t* expected_column);
monetdbe_export bool check_column_float     (monetdbe_result* result, size_t column_index, size_t expected_nr_column_entries, float* expected_column);
monetdbe_export bool check_column_double    (monetdbe_result* result, size_t column_index, size_t expected_nr_column_entries, double* expected_column);
monetdbe_export bool check_column_str       (monetdbe_result* result, size_t column_index, size_t expected_nr_column_entries, char ** expected_column);
monetdbe_export bool check_column_blob      (monetdbe_result* result, size_t column_index, size_t expected_nr_column_entries, monetdbe_data_blob* expected_column);
monetdbe_export bool check_column_date      (monetdbe_result* result, size_t column_index, size_t expected_nr_column_entries, monetdbe_data_date* expected_column);
monetdbe_export bool check_column_time      (monetdbe_result* result, size_t column_index, size_t expected_nr_column_entries, monetdbe_data_time* expected_column);
monetdbe_export bool check_column_timestamp (monetdbe_result* result, size_t column_index, size_t expected_nr_column_entries, monetdbe_data_timestamp* expected_column);

#ifndef str
#define str char*
#endif

#define CHECK_COLUMN(conn, result, column_index, TPE, ...) \
check_column_##TPE ( \
    result, \
    column_index, \
    sizeof((TPE[]) __VA_ARGS__) /sizeof(((TPE[]) __VA_ARGS__)[0]), \
    (TPE[]) __VA_ARGS__)
