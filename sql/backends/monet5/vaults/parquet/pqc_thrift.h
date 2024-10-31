
#ifndef _PQC_THRIFT_H_
#define _PQC_THRIFT_H_

/**
 * Enumerated definition of the types that the Thrift protocol supports.
 * Take special note of the T_END type which is used specifically to mark
 * the end of a sequence of fields.
 */
enum TType {
  T_STOP       = 0,
  T_VOID       = 1,
  T_BOOL       = 2,
  T_BYTE       = 3,
  T_I08        = 3,
  T_I16        = 6,
  T_I32        = 8,
  T_U64        = 9,
  T_I64        = 10,
  T_DOUBLE     = 4,
  T_STRING     = 11,
  T_UTF7       = 11,
  T_STRUCT     = 12,
  T_MAP        = 13,
  T_SET        = 14,
  T_LIST       = 15,
  T_UUID       = 16,
};

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
