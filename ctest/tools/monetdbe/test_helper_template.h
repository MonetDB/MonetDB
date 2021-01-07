/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0.  If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 *
 * Copyright 1997 - July 2008 CWI, August 2008 - 2021 MonetDB B.V.
 */

#include <stddef.h>

#ifndef EQUALS
#define EQUALS(A,B) ((A) == (B))
#endif

bool CHECK_COLUMN_FUNC (
    monetdbe_result* result,
    size_t column_index,
    size_t expected_nr_column_entries,
    TPE* expected_column)
{
    monetdbe_column* rcol;
    char* err = monetdbe_result_fetch(result, &rcol, column_index);
    if (err != NULL) {

        printf("Error while fetching column result values from column %ld: %s\n", column_index, err);

        return false;
    }

    const monetdbe_types expected_type = TPE_ENUM;

    if (rcol->type != expected_type) {
        printf("Actual column and expected column differ in type.\n");
        return false;
    }

    MONETDB_COLUMN_TPE* col_x = (MONETDB_COLUMN_TPE*) rcol;

    if (col_x->count > expected_nr_column_entries) {
        printf("Actual column is bigger then expected column\n");
        return false;
    }

    if (col_x->count < expected_nr_column_entries) {
        printf("Actual column is smaller then expected column\n");
        return false;
    }

    for (size_t i = 0; i < expected_nr_column_entries; i++) {
        if (! ((col_x->is_null(&col_x->data[i]) && expected_column[i]._is_null) || EQUALS(col_x->data[i], expected_column[i].data))) {
            printf("Mismatch between expected and actual column values: values differ at index %ld\n", i);
            return false;
        }
    }

    return true;
}

#undef EQUALS
#undef MONETDB_COLUMN_TPE
#undef TPE_ENUM
#undef CHECK_COLUMN_FUNC
#undef TPE
