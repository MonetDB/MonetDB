/*
 * SPDX-License-Identifier: MPL-2.0
 *
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0.  If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 *
 * For copyright information, see the file debian/copyright.
 */

#ifndef _PQC_THRIFT_H_
#define _PQC_THRIFT_H_

/**
 * Thrift compact protocol
 * Take special note of the T_STOP type which is used specifically to mark
 * the end of a sequence of fields.
 */
typedef enum {
  T_STOP           = 0x00,
  T_BOOLEAN_TRUE   = 0x01,
  T_BOOLEAN_FALSE  = 0x02,
  T_BYTE           = 0x03,
  T_I16            = 0x04,
  T_I32            = 0x05,
  T_I64            = 0x06,
  T_DOUBLE         = 0x07,
  T_BINARY         = 0x08,
  T_LIST           = 0x09,
  T_SET            = 0x0A,
  T_MAP            = 0x0B,
  T_STRUCT         = 0x0C
} ThriftCompactType;

#include <sys/types.h>

/* these will be private within the library */

/* get windows defa */
#define pqc_export extern

pqc_export uint32_t get_uint32(unsigned char *c);
pqc_export uint32_t get_le_uint32(unsigned char *c);

pqc_export int pqc_get_zint32( char *in, uint32_t *v); /* returns number of read bytes, or error -1 */
pqc_export int pqc_get_zint64( char *in, uint64_t *v); /* returns number of read bytes, or error -1 */

pqc_export int pqc_get_int32( char *in, uint32_t *v); /* returns number of read bytes, or error -1 */
pqc_export int pqc_get_int64( char *in, uint64_t *v); /* returns number of read bytes, or error -1 */

pqc_export int pqc_put_int32( char *in, uint32_t v); /* returns number of read bytes, or error -1 */
pqc_export int pqc_put_int64( char *in, uint64_t v); /* returns number of read bytes, or error -1 */

pqc_export int pqc_get_field( char *in, int *fieldid, int *type);
pqc_export int pqc_get_list( char *in, int *size, int *type);
pqc_export int pqc_get_string( char *in, char **s, int *len); /* need way to buffer 0 */

#endif /*_PQC_THRIFT_H_*/
