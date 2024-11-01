
#ifndef _PQC_THRIFT_H_
#define _PQC_THRIFT_H_

/**
 * Thrift compact protocol
 * Take special note of the T_END type which is used specifically to mark
 * the end of a sequence of fields.
 */
typedef enum {
  CT_STOP           = 0x00,
  CT_BOOLEAN_TRUE   = 0x01,
  CT_BOOLEAN_FALSE  = 0x02,
  CT_BYTE           = 0x03,
  CT_I16            = 0x04,
  CT_I32            = 0x05,
  CT_I64            = 0x06,
  CT_DOUBLE         = 0x07,
  CT_BINARY         = 0x08,
  CT_LIST           = 0x09,
  CT_SET            = 0x0A,
  CT_MAP            = 0x0B,
  CT_STRUCT         = 0x0C
} ThriftCompactType;

#include <sys/types.h>

/* these will be private within the library */

/* get windows defa */
#define pqc_export extern

pqc_export u_int32_t get_uint32(unsigned char *c);
pqc_export u_int32_t get_le_uint32(unsigned char *c);

pqc_export int pqc_get_zint32( char *in, u_int32_t *v); /* returns number of read bytes, or error -1 */
pqc_export int pqc_get_zint64( char *in, u_int64_t *v); /* returns number of read bytes, or error -1 */

pqc_export int pqc_get_int32( char *in, u_int32_t *v); /* returns number of read bytes, or error -1 */
pqc_export int pqc_get_int64( char *in, u_int64_t *v); /* returns number of read bytes, or error -1 */

pqc_export int pqc_put_int32( char *in, u_int32_t v); /* returns number of read bytes, or error -1 */
pqc_export int pqc_put_int64( char *in, u_int64_t v); /* returns number of read bytes, or error -1 */

pqc_export int pqc_get_field( char *in, int *fieldid, int *type);
pqc_export int pqc_get_list( char *in, int *size, int *type);
pqc_export int pqc_get_string( char *in, char **s, int *len); /* need way to buffer 0 */

#endif /*_PQC_THRIFT_H_*/
