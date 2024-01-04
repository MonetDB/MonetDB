/*
 * SPDX-License-Identifier: MPL-2.0
 *
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0.  If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 *
 * Copyright 2024 MonetDB Foundation;
 * Copyright August 2008 - 2023 MonetDB B.V.;
 * Copyright 1997 - July 2008 CWI.
 */

#include "bincopydata.h"

#ifdef _MSC_VER
#include <io.h>
#include <fcntl.h>
#endif

static char *exe_name = "<to_be_filled_in>";

static void
gen_tinyints(FILE *f, bool byteswap, long nrecs, char *arg)
{
	(void)arg;
	for (long i = 0; i < nrecs; i++) {
		uint8_t v = (uint8_t)i;
		(void)byteswap;
		fwrite(&v, sizeof(v), 1, f);
	}
}

static void
gen_smallints(FILE *f, bool byteswap, long nrecs, char *arg)
{
	(void)arg;
	for (long i = 0; i < nrecs; i++) {
		uint16_t v = (uint16_t)i;
		if (byteswap) {
			copy_binary_convert16(&v);
		}
		fwrite(&v, sizeof(v), 1, f);
	}
}

static void
gen_bigints(FILE *f, bool byteswap, long nrecs, char *arg)
{
	(void)arg;
	for (long i = 0; i < nrecs; i++) {
		uint64_t v = (uint64_t)i;
		if (byteswap) {
			copy_binary_convert64(&v);
		}
		fwrite(&v, sizeof(v), 1, f);
	}
}

#ifdef HAVE_HGE
static void
gen_hugeints(FILE *f, bool byteswap, long nrecs, char *arg)
{
	(void)arg;
	for (long i = 0; i < nrecs; i++) {
		uhge v = (uhge)i;
		if (byteswap) {
			copy_binary_convert128(&v);
		}
		fwrite(&v, sizeof(v), 1, f);
	}
}
#endif

static void
gen_ints(FILE *f, bool byteswap, long nrecs, char *arg)
{
	(void)arg;
	assert((uintmax_t)nrecs <= (uintmax_t) UINT32_MAX);
	uint32_t n = (uint32_t) nrecs;
	for (uint32_t i = 0; i < n; i++) {
		uint32_t v = i;
		if (byteswap) {
			copy_binary_convert32(&v);
		}
		fwrite(&v, sizeof(v), 1, f);
	}
}

static void
gen_more_ints(FILE *f, bool byteswap, long nrecs, char *arg)
{
	(void)arg;
	assert((uintmax_t)nrecs <= (uintmax_t) UINT32_MAX);
	uint32_t n = (uint32_t) nrecs;
	for (uint32_t i = 0; i < n; i++) {
		uint32_t v = i + 1;
		if (byteswap) {
			copy_binary_convert32(&v);
		}
		fwrite(&v, sizeof(v), 1, f);
	}
}

static void
gen_null_ints(FILE *f, bool byteswap, long nrecs, char *arg)
{
	(void)arg;
	assert((uintmax_t)nrecs <= (uintmax_t) UINT32_MAX);
	uint32_t n = (uint32_t) nrecs;
	uint32_t nil = 0x80000000;
	for (uint32_t i = 0; i < n; i++) {
		uint32_t v = i % 2 == 0 ? nil : i;
		if (byteswap) {
			copy_binary_convert32(&v);
		}
		fwrite(&v, sizeof(v), 1, f);
	}
}

static void
gen_bools(FILE *f, bool byteswap, long nrecs, char *arg)
{
	(void)arg;
	for (long i = 0; i < nrecs; i++) {
		char b = i % 2;
		(void)byteswap;
		fwrite(&b, sizeof(b), 1, f);
	}
}

static void
gen_floats(FILE *f, bool byteswap, long nrecs, char *arg)
{
	(void)arg;
	// Assume for now that the raw bits are portable enough

	for (long i = 0; i < nrecs; i++) {
		float fl = (float)i;
		fl += 0.5;
		if (byteswap)
			copy_binary_convert32(&fl);
		fwrite(&fl, sizeof(fl), 1, f);
	}
}

static void
gen_doubles(FILE *f, bool byteswap, long nrecs, char *arg)
{
	(void)arg;
	// Assume for now that the raw bits are portable enough

	for (long i = 0; i < nrecs; i++) {
		double fl = (double)i;
		fl += 0.5;
		if (byteswap)
			copy_binary_convert64(&fl);
		fwrite(&fl, sizeof(fl), 1, f);
	}
}

static void
gen_strings(FILE *f, bool byteswap, long nrecs, char *arg)
{
	(void)arg;
	(void)byteswap;
	for (long i = 0; i < nrecs; i++) {
		fprintf(f, "int%ld", i);
		fputc(0, f);
	}
}

static void
gen_large_strings(FILE *f, bool byteswap, long nrecs, char *arg)
{
	(void)arg;
	size_t n = 280000;
	char *buf = malloc(n);
	memset(buf, 'a', n);
	for (long i = 0; i < nrecs; i++) {
		fprintf(f, "int%06ld", i);
		if (i % 10000 == 0)
			fwrite(buf, n, 1, f);
		fputc(0, f);
	}
	free(buf);
	(void)byteswap;
}

static void
gen_broken_strings(FILE *f, bool byteswap, long nrecs, char *arg)
{
	(void)arg;
	// "bröken"
	char utf8[] =   {0x62, 0x72,   0xc3, 0xb6,   0x6b, 0x65, 0x6e, 0x00};
	char latin1[] = {0x62, 0x72,   0xf6,         0x6b, 0x65, 0x6e, 0x00};

	(void)byteswap;
	for (long i = 0; i < nrecs; i++) {
		if (i == 123456)
			fwrite(latin1, sizeof(latin1), 1, f);
		else
			fwrite(utf8, sizeof(utf8), 1, f);
	}
}

static void
gen_newline_strings(FILE *f, bool byteswap, long nrecs, char *arg)
{
	(void)arg;
	(void)byteswap;
	for (long i = 0; i < nrecs; i++) {
		fprintf(f, "RN\r\nR\r%ld", i);
		fputc(0, f);
	}
}

static void
gen_null_strings(FILE *f, bool byteswap, long nrecs, char *arg)
{
	(void)arg;
	(void)byteswap;
	for (long i = 0; i < nrecs; i++) {
		if (i % 2 == 0)
			fputc(0x80, f);
		else
			fputs("banana", f);
		fputc(0, f);
	}
}

static void
gen_null_blobs(FILE *f, bool byteswap, long nrecs, char *arg)
{
	(void)arg;
	uint8_t *buffer = malloc(nrecs);
	for (long i = 0; i < nrecs; i++) {
		buffer[i] = 0xD0 + 3 - (i % 3);
	}

	for (long i = 0; i < nrecs; i++) {
		uint64_t header;
		size_t n;
		if (i % 3 == 2) {
			// null
			n = 0;
			header = (uint64_t)-1;
		} else {
			n = (i % 1000);
			header = n;
		}
		if (byteswap)
			copy_binary_convert64(&header);
		assert(sizeof(header) == 8);
		fwrite(&header, sizeof(header), 1, f);
		if (n > 0)
			fwrite(buffer, 1, n, f);
	}
	free(buffer);
}

static void
gen_json(FILE *f, bool byteswap, long nrecs, char *arg)
{
	(void)arg;
	(void)byteswap;
	for (long i = 0; i < nrecs; i++) {
		if (i % 100 == 99) {
			fputc('\x80', f);
		} else {
			fprintf(f, "{\"id\":%ld,\"msg\":\"int%ld\"}", i, i);
		}
		fputc('\0', f);
	}
}

#define FUNCNAME gen_decimal_tinyints
#define STYP int8_t
#define UTYP uint8_t
// #define CONVERT
#include "bincopydecimal_impl.h"

#define FUNCNAME gen_decimal_smallints
#define STYP int16_t
#define UTYP uint16_t
#define CONVERT copy_binary_convert16
#include "bincopydecimal_impl.h"

#define FUNCNAME gen_decimal_ints
#define STYP int32_t
#define UTYP uint32_t
#define CONVERT copy_binary_convert32
#include "bincopydecimal_impl.h"


#define FUNCNAME gen_decimal_bigints
#define STYP int64_t
#define UTYP uint64_t
#define CONVERT copy_binary_convert64
#include "bincopydecimal_impl.h"

#ifdef HAVE_HGE
	#define FUNCNAME gen_decimal_hugeints
	#define STYP hge
	#define UTYP uhge
	#define CONVERT copy_binary_convert128
	#include "bincopydecimal_impl.h"
#endif

typedef void (*generator_t)(FILE *f, bool byteswap, long nrecs, char *argument);

static struct gen {
	char *name;
	generator_t gen;
	bool arg_allowed;
} generators[] = {
	{ "ints", gen_ints },
	{ "more_ints", gen_more_ints },
	{ "null_ints", gen_null_ints },
	{ "bools", gen_bools },
	{ "floats", gen_floats },
	{ "doubles", gen_doubles },
	{ "tinyints", gen_tinyints },
	{ "smallints", gen_smallints },
	{ "bigints", gen_bigints },
#ifdef HAVE_HGE
	{ "hugeints", gen_hugeints },
#endif
	//
	{ "dec_tinyints", gen_decimal_tinyints, .arg_allowed=true },
	{ "dec_smallints", gen_decimal_smallints, .arg_allowed=true },
	{ "dec_ints", gen_decimal_ints, .arg_allowed=true },
	{ "dec_bigints", gen_decimal_bigints, .arg_allowed=true },
#ifdef HAVE_HGE
	{ "dec_hugeints", gen_decimal_hugeints, .arg_allowed=true },
#endif
	//
	{ "strings", gen_strings },
	{ "large_strings", gen_large_strings },
	{ "broken_strings", gen_broken_strings },
	{ "newline_strings", gen_newline_strings },
	{ "null_strings", gen_null_strings },
	{ "null_blobs", gen_null_blobs },
	//
	{ "timestamps", gen_timestamps },
	{ "timestamp_times", gen_timestamp_times },
	{ "timestamp_dates", gen_timestamp_dates },
	{ "timestamp_ms", gen_timestamp_ms },
	{ "timestamp_seconds", gen_timestamp_seconds },
	{ "timestamp_minutes", gen_timestamp_minutes },
	{ "timestamp_hours", gen_timestamp_hours },
	{ "timestamp_days", gen_timestamp_days },
	{ "timestamp_months", gen_timestamp_months },
	{ "timestamp_years", gen_timestamp_years },

	{ "json_objects", gen_json },

	{ "binary_uuids", gen_bin_uuids },
	{ "text_uuids", gen_text_uuids },

	{ NULL, NULL },
};

/* Format the message and write it to stderr. Then exit with the given status.
 * If status is 1, include USAGE in the message.
 * Otherwise, if errno is set, include the error message.
 */
void
croak(int status, const char *ctx, ...)
{
	va_list ap;

	fprintf(stderr, "Error: ");
	if (ctx != NULL) {
		fprintf(stderr, " ");
		va_start(ap, ctx);
		vfprintf(stderr, ctx, ap);
		va_end(ap);
	}
	fprintf(stderr, "\n");
	if (errno) {
		fprintf(stderr, "Possibly due to: %s\n", strerror(errno));
	} else if (status == 1) {
		fprintf(stderr, "USAGE: %s TYPE NRECS DESTFILE\n", exe_name);
		fprintf(stderr, "TYPE:\n");
		for (struct gen *g = generators; g->name != NULL; g++) {
			fprintf(stderr, "  - %s\n", g->name);
		}
	}
	exit(status);
}

static generator_t
pick_generator(const char *full_name, char **arg)
{
	char *name = strdup(full_name);
	*arg = NULL;

	char *sep = strchr(name, '!');
	if (sep != NULL) {
		*arg = strdup(sep + 1);
		*sep = '\0';
	}

	generator_t gen = NULL;
	for (struct gen *g = generators; g->name; g++) {
		if (strcmp(g->name, name) == 0) {
			if (*arg && !g->arg_allowed)
				croak(2, "Generator '%s' does not take arguments", name);
			gen = g->gen;
			break;
		}
	}

	free(name);

	return gen;
}


int
main(int argc, char *argv[])
{
	exe_name = argv[0];
	generator_t gen;
	char *gen_arg = NULL;
	long nrecs;
	FILE *dest;
	bool byteswap = false;
	bool i_am_little_endian =
#ifdef WORDS_BIGENDIAN
		false
#else
		true
#endif
	;
	bool i_am_big_endian = !i_am_little_endian;

	char **args = &argv[1];
	char **args_end = &argv[argc];

	while (args < args_end && **args == '-') {
		char *arg = *args++;
		if (strcmp(arg, "--native-endian") == 0)
			byteswap = false;
		else if (strcmp(arg, "--big-endian") == 0)
			byteswap = i_am_little_endian;
		else if (strcmp(arg, "--little-endian") == 0)
			byteswap = i_am_big_endian;
		else
			croak(1, "Unexpected argument: %s", arg);
	}

	if (args_end - args != 3)
		croak(1, "Unexpected number of arguments");

	gen = pick_generator(args[0], &gen_arg);
	if (gen == NULL)
		croak(1, "Unknown TYPE: %s", args[0]);

	char *end;
	nrecs = strtol(args[1], &end, 10);
	if (*end != '\0')
		croak(1, "NRECS must be an integer, not '%s'", args[1]);

	char *destfilename = args[2];
	if (strcmp(destfilename, "-") == 0) {
#ifdef _MSC_VER
		_setmode(1, O_BINARY);
#endif
		dest = stdout;
	} else {
		dest = fopen(destfilename, "wb");
	}

	if (dest == NULL)
		croak(2, "Cannot open '%s' for writing", args[2]);

	gen(dest, byteswap, nrecs, gen_arg);

	fclose(dest);
	free(gen_arg);

	return 0;
}
