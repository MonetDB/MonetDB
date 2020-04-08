#include <monetdb_config.h>
#include <stream.h>

#include <errno.h>
#include <stdarg.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>


const char *USAGE =
	"Usage:\n"
	"    streamcat read FILENAME OPENER\n"
	"With OPENER:\n"
	"    - rstream           stream = open_rstream(filename)\n"
	"    - rastream          stream = open_rastream(filename)\n"
	;


static int cmd_read(char *argv[]);
typedef stream *(*opener_fun)(char *filename);
static stream *opener_rstream(char *filename);
static stream *opener_rastream(char *filename);

static void copyout(stream *in);

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

	copyout(s);

	return 0;
}


static void copyout(stream *in)
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
		nwritten = fwrite(buf, 1, nread, out);
		if (nwritten != (size_t)nread)
			croak(2, "Write error after %lu bytes: %s", total + nwritten, strerror(errno));
		total += nwritten;
	}

	fflush(out);
	// DO NOT CLOSE out!  It's an alias for stdout.
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