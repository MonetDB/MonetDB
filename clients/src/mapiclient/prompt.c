#include "clients_config.h"
#include <monet_options.h>
#include "Mapi.h"
#include <unistd.h>
#include <stdlib.h>
#include <errno.h>
#include <string.h>
#ifdef HAVE_TERMIOS_H
#include <termios.h>
#endif
#include "mprompt.h"

#ifdef _MSC_VER
#define fileno _fileno
#endif

char *
simple_prompt(const char *prompt, int maxlen, int echo, const char *def)
{
	int length = 0;
	char *destination = NULL;
	FILE *termin = NULL, *termout = NULL;

#ifdef HAVE_TERMIOS_H
	struct termios t_orig, t;
#else
	(void) echo;
#endif

	destination = (char *) malloc(maxlen + 2);
	if (!destination)
		return NULL;

	termin = fopen("/dev/tty", "r");
	termout = fopen("/dev/tty", "w");

	if (termin == NULL || termout == NULL) {
		if (termin)
			fclose(termin);
		if (termout)
			fclose(termout);
		termin = stdin;
		termout = stderr;
	}

#ifdef HAVE_TERMIOS_H
	if (!echo) {
		tcgetattr(fileno(termin), &t);
		t_orig = t;
		t.c_lflag &= ~ECHO;
		tcsetattr(fileno(termin), TCSAFLUSH, &t);
	}
#endif
	if (prompt) {
		if (def)
			fprintf(termout, "%s(%s):", prompt, def);
		else
			fprintf(termout, "%s:", prompt);
		fflush(termout);
	}
	if (fgets(destination, maxlen, termin) == NULL)
		destination[0] = '\0';

	length = (int) strlen(destination);
	if (length > 0 && destination[length - 1] != '\n') {
		char buf[128];
		int buflen;

		do {
			if (fgets(buf, sizeof(buf), termin) == NULL)
				break;
			buflen = (int) strlen(buf);
		} while (buflen > 0 && buf[buflen - 1] != '\n');
	}

	if (length > 0 && destination[length - 1] == '\n')
		destination[length - 1] = '\0';
#ifdef HAVE_TERMIOS_H
	if (!echo) {
		tcsetattr(fileno(termin), TCSAFLUSH, &t_orig);
		fputs("\n", termout);
		fflush(termout);
	}
	if (termin != stdin)
		fclose(termin);
	if (termout != stdout)
		fclose(termout);
#endif
	if (destination[0] == 0 && def)
		strcpy(destination, def);
	return destination;
}
