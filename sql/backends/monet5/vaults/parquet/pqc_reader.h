/*
 * SPDX-License-Identifier: MPL-2.0
 *
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0.  If a copy of the MPL was not distributed with this
 * file, You can obtain one at https://mozilla.org/MPL/2.0/.
 *
 * For copyright information, see the file debian/copyright.
 */

#ifndef _PQC_READER_H_
#define _PQC_READER_H_

#include "pqc_filemetadata.h"

typedef struct pqc_reader_t pqc_reader_t;
pqc_export pqc_reader_t *pqc_reader( pqc_reader_t *p, pqc_file *pq, int nr_workers, pqc_filemetadata *fmd, int colnr, int64_t nrows, const void *nil);
pqc_export void pqc_reader_destroy( pqc_reader_t *r);

pqc_export int64_t pqc_mark_chunk( pqc_reader_t *r, int nr_workers, int wnr, uint64_t nrows);
pqc_export int64_t pqc_read_chunk( pqc_reader_t *r, int wnr, void *d, void *vd, uint64_t nrows, size_t *ssize, int *dict);

pqc_export const char *pqc_get_error( pqc_reader_t *r);

#endif /* _PQC_READER_H_ */
