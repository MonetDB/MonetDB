#include "bincopydata.h"

static char *exe_name = "<to_be_filled_in>";

static struct gen {
	char *name;
	void (*gen)(FILE *f, long nrecs);
} generators[];

_Noreturn static void croak(int status, const char *msg, ...)
	__attribute__((__format__(__printf__, 2, 3)));

/* Format the message and write it to stderr. Then exit with the given status.
 * If status is 1, include USAGE in the message.
 * Otherwise, if errno is set, include the error message.
 */
static void
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


static void
gen_tinyints(FILE *f, long nrecs)
{
	for (long i = 0; i < nrecs; i++) {
		uint8_t v = (uint8_t)i;
		fwrite(&v, sizeof(v), 1, f);
	}
}

static void
gen_smallints(FILE *f, long nrecs)
{
	for (long i = 0; i < nrecs; i++) {
		uint16_t v = (uint16_t)i;
		fwrite(&v, sizeof(v), 1, f);
	}
}

static void
gen_bigints(FILE *f, long nrecs)
{
	for (long i = 0; i < nrecs; i++) {
		uint64_t v = (uint64_t)i;
		fwrite(&v, sizeof(v), 1, f);
	}
}

static void
gen_ints(FILE *f, long nrecs)
{
	assert((uintmax_t)nrecs <= (uintmax_t) UINT32_MAX);
	uint32_t n = (uint32_t) nrecs;
	for (uint32_t i = 0; i < n; i++) {
		fwrite(&i, sizeof(i), 1, f);
	}
}

static void
gen_more_ints(FILE *f, long nrecs)
{
	assert((uintmax_t)nrecs <= (uintmax_t) UINT32_MAX);
	uint32_t n = (uint32_t) nrecs;
	for (uint32_t i = 0; i < n; i++) {
		uint32_t j = i + 1;
		fwrite(&j, sizeof(i), 1, f);
	}
}

static void
gen_null_ints(FILE *f, long nrecs)
{
	assert((uintmax_t)nrecs <= (uintmax_t) UINT32_MAX);
	uint32_t n = (uint32_t) nrecs;
	uint32_t nil = 0x80000000;
	for (uint32_t i = 0; i < n; i++) {
		uint32_t j = i % 2 == 0 ? nil : i;
		fwrite(&j, sizeof(i), 1, f);
	}
}

static void
gen_bools(FILE *f, long nrecs)
{
	for (long i = 0; i < nrecs; i++) {
		char b = i % 2;
		fwrite(&b, sizeof(b), 1, f);
	}
}

static void
gen_floats(FILE *f, long nrecs)
{
	// Assume for now that the raw bits are portable enough

	for (long i = 0; i < nrecs; i++) {
		float fl = (float)i;
		fl += 0.5;
		fwrite(&fl, sizeof(fl), 1, f);
	}
}

static void
gen_doubles(FILE *f, long nrecs)
{
	// Assume for now that the raw bits are portable enough

	for (long i = 0; i < nrecs; i++) {
		double fl = (double)i;
		fl += 0.5;
		fwrite(&fl, sizeof(fl), 1, f);
	}
}

static void
gen_strings(FILE *f, long nrecs)
{
	for (long i = 0; i < nrecs; i++) {
		fprintf(f, "int%ld", i);
		fputc(0, f);
	}
}

static void
gen_large_strings(FILE *f, long nrecs)
{
	size_t n = 280000;
	char *buf = malloc(n);
	memset(buf, 'a', n);
	for (long i = 0; i < nrecs; i++) {
		fprintf(f, "int%06ld", i);
		if (i % 10000 == 0)
			fwrite(buf, n, 1, f);
		fputc(0, f);
	}
}

static void
gen_broken_strings(FILE *f, long nrecs)
{
	// "bröken"
	char utf8[] =   {0x62, 0x72,   0xc3, 0xb6,   0x6b, 0x65, 0x6e, 0x00};
	char latin1[] = {0x62, 0x72,   0xf6,         0x6b, 0x65, 0x6e, 0x00};

	for (long i = 0; i < nrecs; i++) {
		if (i == 123456)
			fwrite(latin1, sizeof(latin1), 1, f);
		else
			fwrite(latin1, sizeof(utf8), 1, f);
	}
}

static void
gen_newline_strings(FILE *f, long nrecs)
{
	for (long i = 0; i < nrecs; i++) {
		fprintf(f, "rn\r\nr\r%ld", i);
		fputc(0, f);
	}
}

static void
gen_null_strings(FILE *f, long nrecs)
{
	for (long i = 0; i < nrecs; i++) {
		if (i % 2 == 0)
			fputc(0x80, f);
		else
			fputs("banana", f);
		fputc(0, f);
	}
}

static struct gen generators[] = {
	{ "ints", gen_ints },
	{ "more_ints", gen_more_ints },
	{ "null_ints", gen_null_ints },
	{ "bools", gen_bools },
	{ "floats", gen_floats },
	{ "doubles", gen_doubles },
	{ "tinyints", gen_tinyints },
	{ "smallints", gen_smallints },
	{ "bigints", gen_bigints },
	//
	{ "strings", gen_strings },
	{ "large_strings", gen_large_strings },
	{ "broken_strings", gen_broken_strings },
	{ "newline_strings", gen_newline_strings },
	{ "null_strings", gen_null_strings },
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

	{ NULL, NULL },
};

int
main(int argc, char *argv[])
{
	exe_name = argv[0];
	void (*gen)(FILE*, long);
	long nrecs;
	FILE *dest;

	if (argc != 4)
		croak(1, "Unexpected number of arguments: %d", argc - 1);

	gen = NULL;
	for (struct gen *g = generators; g->name; g++) {
		if (strcmp(g->name, argv[1]) == 0) {
			gen = g->gen;
			break;
		}
	}
	if (gen == NULL)
		croak(1, "Unknown TYPE: %s", argv[1]);

	char *end;
	nrecs = strtol(argv[2], &end, 10);
	if (*end != '\0')
		croak(1, "NRECS must be an integer, not '%s'", argv[2]);

	dest = fopen(argv[3], "wb");
	if (dest == NULL)
		croak(2, "Cannot open '%s' for writing", argv[3]);

	gen(dest, nrecs);

	fclose(dest);

	return 0;
}
