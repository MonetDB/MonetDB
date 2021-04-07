#include "monetdb_config.h"
#include "stream.h"

#include <errno.h>
#include <fcntl.h>
#include <stdarg.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>

#if defined(_MSC_VER) && _MSC_VER >= 1400
#define fdopen _fdopen
#endif


const char *USAGE =
	"Usage:\n"
	"    streamcat read    [-o FILE] FILE R_OPENER [R_WRAPPER..]\n"
	"    streamcat write   [-i FILE] FILE W_OPENER [W_WRAPPER..]\n"
	"    streamcat bstream -i (FILE | -)\n"
	"    streamcat bstream -o (FILE | -) [-a ADDITIONAL_TEXT] FILE...\n"
	"Options:\n"
	"    -o FILE             use FILE instead of stdout\n"
	"    -i FILE             use FILE instead of stdin\n"
	"With R_OPENER:\n"
	"    - rstream           stream = open_rstream(filename)\n"
	"    - rastream          stream = open_rastream(filename)\n"
	"    - urlstream         stream = open_urlstream(filename)\n"
	""
	"With W_OPENER:\n"
	"    - wstream           stream = open_wstream(filename)\n"
	"    - wastream          stream = open_wastream(filename)\n"
	"    - blocksize:N       Copy in blocks of this size\n"
	"    - flush:{data,all}  Flush after each block\n"
	"With R_WRAPPER:\n"
	"    - iconv:enc         stream = iconv_rstream(stream, enc)\n"
	"    - blocksize:N       Copy in blocks of this size\n"
	"    - compr             Autodecompress\n"
	"With W_WRAPPER:\n"
	"    - iconv:enc         stream = iconv_wstream(stream, enc)\n"
	"    - blocksize:N       Copy out blocks of this size\n"
	;


static int cmd_read(char *argv[]);
static int cmd_write(char *argv[]);
static int cmd_bstream(char *argv[]);

typedef stream *(*opener_fun)(char *filename);

static stream *opener_rstream(char *filename);
static stream *opener_rastream(char *filename);
static stream *opener_urlstream(char *url);

static stream *opener_wstream(char *filename);
static stream *opener_wastream(char *filename);

static stream *wrapper_read_iconv(stream *s, char *enc);

static stream *wrapper_write_iconv(stream *s, char *enc);

static stream *wrapper_autocompression(stream *s, char *level);

static void copy_stream_to_file(stream *in, FILE *out, size_t bufsize);

static void copy_file_to_stream(FILE *in, stream *out, size_t bufsize, bool do_flush, mnstr_flush_level flush_level);

_Noreturn static void croak(int status, const char *msg, ...)
	__attribute__((__format__(__printf__, 2, 3)));

/* Format the message and write it to stderr. Then exit with the given status.
 * If status is 1, include USAGE in the message.
 */
static void
croak(int status, const char *msg, ...)
{
	va_list ap;

	va_start(ap, msg);
	if (msg != NULL) {
		fprintf(stderr, "Error: ");
		vfprintf(stderr, msg, ap);
		fprintf(stderr, "\n");
	}
	va_end(ap);
	if (status == 1)
		fprintf(stderr, "%s", USAGE);
	exit(status);
}


int
main(int argc, char *argv[])
{
	if (mnstr_init() < 0)
		croak(2, "Could not initialize streams library");

	if (argc < 2)
		croak(1, NULL);

	if (strcmp(argv[1], "read") == 0)
		return cmd_read(argv+1);
	else if (strcmp(argv[1], "write") == 0)
		return cmd_write(argv+1);
	else if (strcmp(argv[1], "bstream") == 0)
		return cmd_bstream(argv+1);
	else
		croak(1, "Unknown subcommand '%s'", argv[1]);
}


int cmd_read(char *argv[])
{
	char **arg = &argv[1];
	char *filename = NULL;
	char *opener_name = NULL;
	opener_fun opener;
	size_t bufsize = 1024;

	stream *s = NULL;
	FILE *out = NULL;

	if (*arg != NULL && arg[0][0] == '-') {
		char *a = *arg++;
		if (a[1] == 'o') {
			if (*arg == NULL)
				croak(1, "-o requires parameter");
			char *name = *arg++;
			out = fopen(name, "wb");
			if (out == NULL)
				croak(2, "could not open %s", name);
		} else {
			croak(1, "unknown option '%s'", a);
		}
	}

	filename = *arg++;
	if (filename == NULL)
		croak(1, "Missing filename");
	else if (filename[0] == '-')
		croak(1, "Unexpected option: %s", filename);

	opener_name = *arg++;
	if (opener_name == NULL)
		croak(1, "Missing opener");
	else if (opener_name[0] == '-')
		croak(1, "Unexpected option: %s", opener_name);
	else if (strcmp(opener_name, "rstream") == 0)
		opener = opener_rstream;
	else if (strcmp(opener_name, "rastream") == 0)
		opener = opener_rastream;
	else if (strcmp(opener_name, "urlstream") == 0)
		opener = opener_urlstream;
	else
		croak(1, "Unknown opener '%s'", opener_name);

	s = opener(filename);
	if (s == NULL || mnstr_errnr(s)) {
		char *msg = mnstr_error(s);
		croak(2, "Opener %s failed: %s", opener_name, msg ? msg : "<no error message>");
	}

	for (; *arg != NULL; arg++) {
		if (arg[0][0] == '-')
			croak(1, "Unexpected option: %s", *arg);

		char *wrapper_name = *arg;
		char *parms = strchr(wrapper_name, ':');
		stream *(*wrapper)(stream *s, char *parm) = NULL;

		if (parms != NULL) {
			*parms = '\0';
			parms += 1;
		}
		if (strcmp(wrapper_name, "iconv") == 0) {
			if (parms == NULL)
				croak(1, "iconv wrapper needs a parameter");
			wrapper = wrapper_read_iconv;
		} else if (strcmp(wrapper_name, "blocksize") == 0) {
			if (parms == NULL)
				croak(1, "blocksize needs a parameter");
			char *end;
			long size = strtol(parms, &end, 10);
			if (*end != '\0' || size <= 0)
				croak(1, "invalid blocksize: %ld", size);
			bufsize = size;
		} else if (strcmp(wrapper_name, "compr") == 0) {
			wrapper = wrapper_autocompression;
		} else {
			croak(1, "Unknown wrapper: %s", wrapper_name);
		}
		if (wrapper != NULL)
			s = wrapper(s, parms);
		if (s == NULL || mnstr_errnr(s)) {
			char *msg = mnstr_error(s);
			croak(2, "Opener %s failed: %s", opener_name, msg ? msg : "<no error message>");
		}
	}

	if (out == NULL) {
		fflush(stdout);
		out = stdout;
#ifdef _MSC_VER
		_setmode(_fileno(out), O_BINARY);
#endif
	}

	copy_stream_to_file(s, out, bufsize);
	close_stream(s);
	if (out != stdout)
		fclose(out);

	return 0;
}


int cmd_write(char *argv[])
{
	char **arg = &argv[1];
	char *filename = NULL;
	char *opener_name = NULL;
	opener_fun opener;
	size_t bufsize = 1024;
	bool do_flush = false;
	mnstr_flush_level flush_level = MNSTR_FLUSH_DATA;

	FILE *in = NULL;
	stream *s = NULL;

	if (*arg != NULL && arg[0][0] == '-') {
		char *a = *arg++;
		if (a[1] == 'i') {
			if (*arg == NULL)
				croak(1, "-i requires parameter");
			char *name = *arg++;
			in = fopen(name, "rb");
			if (in == NULL)
				croak(2, "could not open %s", name);
		} else {
			croak(1, "unknown option '%s'", a);
		}
	}

	filename = *arg++;
	if (filename == NULL)
		croak(1, "Missing filename");
	else if (filename[0] == '-')
		croak(1, "Unexpected option: %s", filename);

	opener_name = *arg++;
	if (opener_name == NULL)
		croak(1, "Missing opener");
	else if (opener_name[0] == '-')
		croak(1, "Unexpected option: %s", opener_name);
	else if (strcmp(opener_name, "wstream") == 0)
		opener = opener_wstream;
	else if (strcmp(opener_name, "wastream") == 0)
		opener = opener_wastream;
	else
		croak(1, "Unknown opener '%s'", opener_name);

	s = opener(filename);
	if (s == NULL || mnstr_errnr(s)) {
		char *msg = mnstr_error(NULL);
		croak(2, "Opener %s failed: %s", opener_name, msg ? msg : "");
	}

	for (; *arg != NULL; arg++) {
		if (arg[0][0] == '-')
			croak(1, "Unexpected option: %s", *arg);

		char *wrapper_name = *arg;
		char *parms = strchr(wrapper_name, ':');
		stream *(*wrapper)(stream *s, char *parm) = NULL;

		if (parms != NULL) {
			*parms = '\0';
			parms += 1;
		}
		if (strcmp(wrapper_name, "iconv") == 0) {
			if (parms == NULL)
				croak(1, "iconv wrapper needs a parameter");
			wrapper = wrapper_write_iconv;
		} else if (strcmp(wrapper_name, "blocksize") == 0) {
			if (parms == NULL)
				croak(1, "blocksize needs a parameter");
			char *end;
			long size = strtol(parms, &end, 10);
			if (*end != '\0' || size <= 0)
				croak(1, "invalid blocksize: %ld", size);
			bufsize = size;
		} else if (strcmp(wrapper_name, "flush") == 0) {
			if (parms == NULL)
				parms = "";
			if (strcmp(parms, "data") == 0)
				flush_level = MNSTR_FLUSH_DATA;
			else if (strcmp(parms, "all") == 0)
				flush_level = MNSTR_FLUSH_ALL;
			else
				croak(1, "flush wrapper parameter is either \"data\" or \"all\"");
			do_flush = true;
		} else {
			croak(1, "Unknown wrapper: %s", wrapper_name);
		}
		if (wrapper != NULL)
			s = wrapper(s, parms);
		if (s == NULL || mnstr_errnr(s)) {
			char *msg = mnstr_error(s);
			croak(2, "Opener %s failed: %s", opener_name, msg ? msg : "<no error message>");
		}
	}

	if (in == NULL) {
		in = stdin;
#ifdef _MSC_VER
		_setmode(_fileno(in), O_BINARY);
#endif
	}

	copy_file_to_stream(in, s, bufsize, do_flush, flush_level);
	close_stream(s);

	if (in != stdin)
		fclose(in);

	return 0;
}


static void copy_stream_to_file(stream *in, FILE *out, size_t bufsize)
{
	char *buffer;
	ssize_t nread;
	size_t nwritten;
	uint64_t total = 0;
	long iterations = -1;
	ssize_t short_read = 0;

	buffer = malloc(bufsize);

	while (1) {
		iterations += 1;
		nread = mnstr_read(in, buffer, 1, bufsize);
		if (nread < 0)
			croak(2, "Error reading from stream after %" PRIu64 " bytes: %s", total, mnstr_error(in));
		if (nread == 0) {
			// eof
			break;
		}

		if (short_read != 0)
			// A short read MUST be followed by either error or eof.
			croak(2, "Short read (%zd/%zu) after %ld iterations not followed by EOF or error", short_read, bufsize, iterations - 1);
		short_read = (size_t)nread < bufsize ? nread : 0;

		errno = 0;
		nwritten = fwrite(buffer, 1, nread, out);
		if (nwritten != (size_t)nread)
			croak(2, "Write error after %" PRIu64 " bytes: %s", total + (uint64_t)nwritten, strerror(errno));
		total += nwritten;
	}

	free(buffer);

	fflush(out);
	// DO NOT fclose(out)!  It's an alias for stdout.
}


static void copy_file_to_stream(FILE *in, stream *out, size_t bufsize, bool do_flush, mnstr_flush_level flush_level)
{
	char *buffer;
	size_t nread;
	ssize_t nwritten;
	int64_t total = 0;

	buffer = malloc(bufsize);

	while (1) {
		errno = 0;
		nread = fread(buffer, 1, bufsize, in);
		if (nread == 0) {
			if (errno != 0)
				croak(2, "Error reading from stream after %" PRId64 " bytes: %s", total, strerror(errno));
			else
				break;
		}
		nwritten = mnstr_write(out, buffer, 1, nread);
		if (nwritten < 0)
			croak(2, "Write error after %" PRId64 " bytes: %s", total, mnstr_error(out));
		if ((size_t)nwritten != nread)
			croak(2, "Partial write (%lu/%lu bytes) after %" PRId64 " bytes: %s",
				(unsigned long)nwritten,  (unsigned long)nread,
				total + (int64_t)nwritten, mnstr_error(out));
		total += (int64_t)nwritten;
		if (do_flush)
			if (mnstr_flush(out, flush_level) != 0)
				croak(2, "Flush failed after %" PRId64 " bytes: %s", total, mnstr_error(out));
	}

	free(buffer);

	// DO NOT fclose(in)!  It's often an alias for stdin.
	// And if it isn't, who cares, we're about to shutdown anyway
}


static stream *
opener_rstream(char *filename)
{
	stream *s = open_rstream(filename);
	if (!mnstr_isbinary(s))
		croak(2, "open_rastream returned binary stream");
	return s;
}


static stream *
opener_rastream(char *filename)
{
	stream *s = open_rastream(filename);
	if (mnstr_isbinary(s))
		croak(2, "open_rastream returned binary stream");
	return s;
}

static stream *
opener_urlstream(char *url)
{
	stream *s = open_urlstream(url);
	return s;
}

static stream *
opener_wstream(char *filename)
{
	stream *s = open_wstream(filename);
	return s;
}


static stream *
opener_wastream(char *filename)
{
	stream *s = open_wastream(filename);
	return s;
}


static stream *
wrapper_read_iconv(stream *s, char *enc)
{
	return iconv_rstream(s, enc, "wrapper_read_iconv");
}


static stream *
wrapper_write_iconv(stream *s, char *enc)
{
	return iconv_wstream(s, enc, "wrapper_write_iconv");
}

static stream *
wrapper_autocompression(stream *s, char *level_parm)
{
	int level = 0;
	if (level_parm) {
		char *end;
		level = strtol(level_parm, &end, 10);
		if (*end != '\0' || level < 0)
			croak(1, "invalid compression level: %s", level_parm);
	}
	return compressed_stream(s, level);
}

static
int cmd_bstream(char *argv[])
{
	char **arg = &argv[1];
	char *inout_filename = NULL;
	bool dump = false;
	char *additional = NULL;
	stream *s = NULL;
	stream *bs = NULL;

	// Parse the flags
	while (*arg && arg[0][0] == '-') {
		switch (arg[0][1]) {
			case 'a':
				if (!*++arg || arg[0][0] == '-')
					croak(1, "-a requires argument");
				additional = *arg++;
				break;
			case 'i':
				if (!*++arg)
					croak(1, "-i requires argument");
				inout_filename = *arg++;
				dump = true;
				break;
			case 'o':
				if (!*++arg)
					croak(1, "-o requires argument");
				inout_filename = *arg++;
				dump = false;
				break;
			default:
				croak(1, "unknown opion: %s", *arg);
				break;
		}
	}
	if (!inout_filename)
		croak(1, "either -i or -o is required");
	if (dump && additional)
		croak(1, "-a is only applicable in combination with -o");

	// Open the stream
	if (dump) {
		FILE *f;
		if (strcmp(inout_filename, "-") == 0) {
			f = stdin;
#ifdef _MSC_VER
			_setmode(_fileno(f), O_BINARY);
#endif
		} else {
			f = fopen(inout_filename, "rb");  // notice! binary input file
		}
		if (!f)
			croak(2, "can't open file '%s': %s", inout_filename, strerror(errno));
		s = file_rstream(f, true, inout_filename);
		if (!s)
			croak(2, "can't convert FILE '%s' to stream: %s", inout_filename, mnstr_peek_error(NULL));
	} else {
		FILE *f;
		if (strcmp(inout_filename, "-") == 0) {
			f = stdout;
		} else {
			f = fopen(inout_filename, "w"); // notice! ascii output file
		}
		if (!f)
			croak(2, "can't open file '%s': %s", inout_filename, strerror(errno));
		s = file_wstream(f, true, inout_filename);
		if (!s)
			croak(2, "can't convert FILE '%s' to stream: %s", inout_filename, mnstr_peek_error(NULL));
	}

	// Construct the bstream
	bs = block_stream(s);
	if (!bs)
		croak(2, "can't construct bstream: %s", mnstr_peek_error(NULL));

	if (dump) {
		// char buf[8192];
		// int size;
		// while (!bs->eof);
	} else {
		while (*arg) {
			char *filename = *arg++;
			FILE *f = fopen(filename, "r");
			if (!f)
				croak(2, "could not open '%s': %s", filename, strerror(errno));
			copy_file_to_stream(f, bs, 42, false, MNSTR_FLUSH_DATA);
			fclose(f);
			mnstr_flush(bs, MNSTR_FLUSH_DATA);
		}
	}

	mnstr_destroy(bs);
	if (additional) {
		mnstr_printf(s, "%s", additional);
	}
	mnstr_close(s);

	return 0;
}
