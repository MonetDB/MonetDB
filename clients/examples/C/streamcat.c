#include <monetdb_config.h>
#include <stream.h>

#include <errno.h>
#include <stdarg.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>


const char *USAGE =
	"Usage:\n"
	"    streamcat read  FILENAME R_OPENER\n"
	"    streamcat write FILENAME W_OPENER\n"
	"With R_OPENER:\n"
	"    - rstream           stream = open_rstream(filename)\n"
	"    - rastream          stream = open_rastream(filename)\n"
	"With W_OPENER:\n"
	"    - wstream           stream = open_wstream(filename)\n"
	"    - wastream          stream = open_wastream(filename)\n"
	;


static int cmd_read(char *argv[]);
static int cmd_write(char *argv[]);
typedef stream *(*opener_fun)(char *filename);

static stream *opener_rstream(char *filename);
static stream *opener_rastream(char *filename);

static stream *opener_wstream(char *filename);
static stream *opener_wastream(char *filename);

static void copy_to_stdout(stream *in);

static void copy_from_stdin(stream *out);

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
	if (argc < 2)
		croak(1, NULL);

	if (strcmp(argv[1], "read") == 0)
		return cmd_read(argv+1);
	else if (strcmp(argv[1], "write") == 0)
		return cmd_write(argv+1);
	else
		croak(1, "Unknown subcommand '%s'", argv[1]);
}


int cmd_read(char *argv[])
{
	char **arg = &argv[1];
	char *filename = NULL;
	char *opener_name = NULL;
	opener_fun opener;

	stream *s = NULL;

	filename = *arg++;
	if (filename == NULL)
		croak(1, "Missing filename");

	opener_name = *arg++;
	if (opener_name == NULL)
		croak(1, "Missing opener");
	else if (strcmp(opener_name, "rstream") == 0)
		opener = opener_rstream;
	else if (strcmp(opener_name, "rastream") == 0)
		opener = opener_rastream;
	else
		croak(1, "Unknown opener '%s'", opener_name);

	s = opener(filename);

	copy_to_stdout(s);
	mnstr_close(s);

	return 0;
}


int cmd_write(char *argv[])
{
	char **arg = &argv[1];
	char *filename = NULL;
	char *opener_name = NULL;
	opener_fun opener;

	stream *s = NULL;

	filename = *arg++;
	if (filename == NULL)
		croak(1, "Missing filename");

	opener_name = *arg++;
	if (opener_name == NULL)
		croak(1, "Missing opener");
	else if (strcmp(opener_name, "wstream") == 0)
		opener = opener_wstream;
	else if (strcmp(opener_name, "wastream") == 0)
		opener = opener_wastream;
	else
		croak(1, "Unknown opener '%s'", opener_name);

	s = opener(filename);

	copy_from_stdin(s);
	mnstr_close(s);

	return 0;
}


static void copy_to_stdout(stream *in)
{
	FILE *out;
	char buf[1024];
	ssize_t nread;
	size_t nwritten;
	unsigned long total = 0;

	// Try to get binary stdout on Windows
	fflush(stdout);
	out = fdopen(fileno(stdout), "wb");

	while (1) {
		nread = mnstr_read(in, buf, 1, sizeof(buf));
		if (nread < 0)
			croak(2, "Error reading from stream after %lu bytes: %s", total, mnstr_error(in));
		if (nread == 0) {
			// eof
			break;
		}
		errno = 0;
		nwritten = fwrite(buf, 1, nread, out);
		if (nwritten != (size_t)nread)
			croak(2, "Write error after %lu bytes: %s", total + nwritten, strerror(errno));
		total += nwritten;
	}

	fflush(out);
	// DO NOT fclose(out)!  It's an alias for stdout.
}


static void copy_from_stdin(stream *out)
{
	FILE *in;
	char buf[1024];
	size_t nread;
	ssize_t nwritten;
	unsigned long total = 0;

	// We can't flush stdin but it hasn't been used yet so with any
	// luck, no input is buffered yet
	in = fdopen(fileno(stdin), "rb");

	while (1) {
		errno = 0;
		nread = fread(buf, 1, sizeof(buf), in);
		if (nread == 0) {
			if (errno != 0)
				croak(2, "Error reading from stream after %lu bytes: %s", total, strerror(errno));
			else
				break;
		}
		nwritten = mnstr_write(out, buf, 1, nread);
		if ((size_t)nwritten != nread)
			croak(2, "Write error after %lu bytes: %s", total + nwritten, mnstr_error(out));
		total += nwritten;
	}

	// DO NOT fclose(in)!  It's an alias for stdin.
}


static stream *
opener_rstream(char *filename)
{
	stream *s = open_rstream(filename);
	if (s == NULL)
		croak(2, "Error opening file '%s': %s", filename, strerror(errno));
	return s;
}


static stream *
opener_rastream(char *filename)
{
	stream *s = open_rastream(filename);
	if (s == NULL)
		croak(2, "Error opening file '%s': %s", filename, strerror(errno));
	return s;
}

static stream *
opener_wstream(char *filename)
{
	stream *s = open_wstream(filename);
	if (s == NULL)
		croak(2, "Error opening file '%s': %s", filename, strerror(errno));
	return s;
}


static stream *
opener_wastream(char *filename)
{
	stream *s = open_wastream(filename);
	if (s == NULL)
		croak(2, "Error opening file '%s': %s", filename, strerror(errno));
	return s;
}