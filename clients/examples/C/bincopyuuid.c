#include "bincopydata.h"

typedef union {
	uint8_t u[16];
	uint32_t w[4];
} my_uuid;

static void
random_uuid(struct rng *rng, my_uuid *u)
{
	u->w[0] = rng_next(rng);
	u->w[1] = rng_next(rng);
	u->w[2] = rng_next(rng);
	u->w[3] = rng_next(rng);

	// See https://en.wikipedia.org/wiki/Universally_unique_identifier
	//
	// _0_1_2_3 _4_5 _6_7 _8_9
	// 123e4567-e89b-12d3-a456-426614174000
    // xxxxxxxx-xxxx-Mxxx-Nxxx-xxxxxxxxxxxx
	//
	// M: uuid version, located in byte 6
	// N: uuid variant, located in byte 8

	// Force variant 1: most common, fully big-endian encoding.
	// Variant 1 means 0b10xx.
	u->u[8] &= 0x3F; // now it's 0b00xx,
	u->u[8] |= 0x80; // now it's 0b10xx.

	// Force version 4: randomly generated
	u->u[6] &= 0x0F;
	u->u[6] |= 0x40;
}

void
gen_bin_uuids(FILE *f, bool byteswap, long nrecs)
{
	struct rng rng = my_favorite_rng();
	my_uuid uu = { .u = { 0 }};
	for (long i = 0; i < nrecs; i++) {
		random_uuid(&rng, &uu);
		if (i % 100 == 99)
			uu = (my_uuid) { .u = { 0 }};
		fwrite(&uu, sizeof(uu), 1, f);
	}
	(void)byteswap;
}

void
gen_text_uuids(FILE *f, bool byteswap, long nrecs)
{
	struct rng rng = my_favorite_rng();
	my_uuid uu = { .u = { 0 }};
	for (long i = 0; i < nrecs; i++) {
		random_uuid(&rng, &uu);
		if (i % 100 == 99)
			uu = (my_uuid) { .u = { 0 }};
		fprintf(
			f,
			"%02x%02x%02x%02x-%02x%02x-%02x%02x-%02x%02x-%02x%02x%02x%02x%02x%02x",
			uu.u[0], uu.u[1], uu.u[2], uu.u[3],
			uu.u[4], uu.u[5],
			uu.u[6], uu.u[7],
			uu.u[8], uu.u[9],
			uu.u[10], uu.u[11], uu.u[12], uu.u[13], uu.u[14], uu.u[15]
			);
		fputc('\0', f);
	}
	(void)byteswap;
}

