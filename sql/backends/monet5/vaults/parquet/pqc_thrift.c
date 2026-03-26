#include "monetdb_config.h"
//#include <sys/types.h>
#include "pqc_thrift.h"
#include <stdbool.h>

#define int64_t uint64_t
#define int32_t uint32_t

uint32_t
get_uint32(unsigned char *c)
{
	uint32_t r = 0;
	r = (uint32_t)c[0] | ((uint32_t)c[1]<<8) | ((uint32_t)c[2]<<16) | ((uint32_t)c[3]<<24);
	return r;
}

uint32_t
get_le_uint32(unsigned char *c)
{
	uint32_t r = 0;
	r = (uint32_t)c[3] | ((uint32_t)c[2]<<8) | ((uint32_t)c[1]<<16) | ((uint32_t)c[0]<<24);
	return r;
}

static uint64_t
i64_to_zigzag (const int64_t l)
{
  return (((uint64_t)l) << 1) ^ (l >> 63);
}

static uint32_t
i32_to_zigzag (const int32_t n)
{
  return (((uint32_t)n) << 1) ^ (n >> 31);
}

static int64_t
zigzag_to_i64 (uint64_t n)
{
  return (n >> 1) ^ (uint64_t) (-(int64_t) (n & 1));
}

static int32_t
zigzag_to_i32 (uint32_t n)
{
  return (n >> 1) ^ (uint32_t) (-(int32_t) (n & 1));
}

int
pqc_get_int32( char *in, uint32_t *V)
{
	uint32_t v = 0;
	int nr = 0;
	int shift = 0;

	while(true) {
		int byte = in[nr++];
		v |= (uint32_t)(byte & 0x7f) << shift;
		shift += 7;
		if (!(byte & 0x80)) {
      			*V = v;
      			return nr;
    		}
		if (nr >= 5)
			return -1;
	}
	return 0;
}

int
pqc_get_zint32( char *in, uint32_t *V)
{
	int err = pqc_get_int32(in, V);
	if (err >= 0)
      		*V = zigzag_to_i32(*V);
	return err;
}

int
pqc_get_int64( char *in, uint64_t *V)
{
	uint64_t v = 0;
	int nr = 0;
	int shift = 0;

	while(true) {
		int byte = in[nr++];
		v |= (uint64_t)(byte & 0x7f) << shift;
		shift += 7;
		if (!(byte & 0x80)) {
      			*V = v;
      			return nr;
    		}
		if (nr >= 10)
			return -1;
	}
	return 0;
}

int
pqc_get_zint64( char *in, uint64_t *V)
{
	int err = pqc_get_int64(in, V);
	if (err >= 0)
      		*V = zigzag_to_i64(*V);
	return err;
}

int
pqc_put_int32( char *out, uint32_t V)
{
	int nr = 0;
      	uint32_t v = i32_to_zigzag(V);

	while (true) {
    		if ((v & ~0x7F) == 0) {
      			out[nr++] = (char)v;
      			break;
    		} else {
      			out[nr++] = (char)((v & 0x7F) | 0x80);
      			v >>= 7;
    		}
  	}
	return nr;
}

int
pqc_put_int64( char *out, uint64_t V)
{
	int nr = 0;
      	uint64_t v = i64_to_zigzag(V);

	while (true) {
    		if ((v & ~0x7FL) == 0) {
      			out[nr++] = (char)v;
      			break;
    		} else {
      			out[nr++] = (char)((v & 0x7F) | 0x80);
      			v >>= 7;
    		}
  	}
	return nr;
}

#define T_STOP 0

#include <stdio.h>

int
pqc_get_field( char *in, int *fieldid, int *type)
{
	unsigned char byte = in[0];
	int pos = 1;
	// printf("Read byte: 0x%02X\n", byte);
	if ((byte & 0x0f) == T_STOP) {
		*type = 0;
		*fieldid = 0;
		return pos;
	}
	if (!(byte >> 4)) {
		pos += pqc_get_int32( in+pos, (uint32_t*)fieldid);
	} else {
		*fieldid += (byte>>4);
	}
	*type = (byte & 0x0f); /* todo handle booleans, ttype */
	return pos;
}

int
pqc_get_list( char *in, int *size, int *type)
{
	unsigned char byte = in[0];
	int pos = 1;
	int sz = (byte >> 4);
	if ((sz & 0x0f) == 0x0f) {
		pos += pqc_get_int32( in+pos, (uint32_t*)&sz);
	}
	*size = sz;
	*type = (byte & 0xf);
	return pos;
}

int
pqc_get_string( char *in, char **s, int *len)
{
	int pos = 0;
	pos += pqc_get_int32(in+pos, (uint32_t*)len);
	*s = in+pos;
	return pos+*len;
}
