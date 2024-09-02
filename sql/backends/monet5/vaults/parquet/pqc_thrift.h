
#ifndef _PQC_THRIFT_H_
#define _PQC_THRIFT_H_

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
