
#include <mem.h>
#include <stdio.h>
#include <string.h>

#ifdef HAVE_TERMIOS_H
#include <termios.h>
#endif

#include <stdlib.h>	/* for malloc() on Darwin */
#ifdef HAVE_MALLOC_H
#include <malloc.h>
#endif

char *
simple_prompt(const char *prompt, int maxlen, int echo)
{
        int        length = 0;
        char       *destination = NULL;
        FILE       *termin = NULL, *termout = NULL;

#ifdef HAVE_TERMIOS_H
        struct termios t_orig, t;
#else
	(void) echo; /* Stefan: unused!? */
#endif

        destination = (char *) malloc(maxlen + 2);
        if (!destination)
                return NULL;

	termin = fopen("/dev/tty", "r");
	termout = fopen("/dev/tty", "w");

	if (termin == NULL || termout == NULL){
		termin = stdin;
		termout = stderr;
	}

        if (prompt)
        {
                fputs( prompt, termout);
                fflush(termout);
        }

#ifdef HAVE_TERMIOS_H
        if (!echo)
        {
                tcgetattr(fileno(termin), &t);
                t_orig = t;
                t.c_lflag &= ~ECHO;
                tcsetattr(fileno(termin), TCSAFLUSH, &t);
        }
#endif

        if (fgets(destination, maxlen, termin) == NULL)
                destination[0] = '\0';

        length = strlen(destination);
        if (length > 0 && destination[length - 1] != '\n')
        {
                char	buf[128];
                int	buflen;

                do
                {
                        if (fgets(buf, sizeof(buf), termin) == NULL)
                                break;
                        buflen = strlen(buf);
                } while (buflen > 0 && buf[buflen - 1] != '\n');
        }

        if (length > 0 && destination[length - 1] == '\n')
                destination[length - 1] = '\0';

#ifdef HAVE_TERMIOS_H
        if (!echo)
        {
                tcsetattr(fileno(termin), TCSAFLUSH, &t_orig);
                fputs("\n", termout);
                fflush(termout);
        }
	if (termin != stdin)
		fclose(termin);

	if (termout != stdout)
		fclose(termout);
#endif

        return destination;
}


/*
main()
{
	char *user = simple_prompt("Username:", 100, 1);
	char *passwd = simple_prompt("Password:", 100, 0);
	printf("user %s\npassword secret\n", user);

	free(user);
	free(passwd);
}
*/
