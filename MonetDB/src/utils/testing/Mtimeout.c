#ifdef HAVE_CONFIG_H
#include <config.h>
#endif
#include <stdio.h>
#include <sys/types.h>
#ifdef HAVE_UNISTD_H
# include <unistd.h>
#endif
#ifdef HAVE_SYS_WAIT_H
# include <sys/wait.h>
#endif
#include <signal.h>
#include <stdlib.h>
#include <ctype.h>

#define DEFAULT_TIMEOUT 0

static int timeout = DEFAULT_TIMEOUT;
static char *progname;

static pid_t exec_pid;
static char **exec_argv;
static int exec_timeout = 0;


static void
invocation(FILE *fp, char *prefix, char **argv)
{
    fprintf(fp, "\n! %s", prefix);
    while(*argv) {
	fprintf(fp, "%s ", *argv);
	argv++;
    }
    fprintf(fp, "\n");
}

static void
alarm_handler(int sig)
{
    exec_timeout = 1;
#if defined(LINUX)
    kill(-exec_pid, SIGINT);
    sleep(11);
#endif
    kill(-exec_pid, SIGKILL);
}

static int
limit(char **argv)
{
    struct sigaction action;
    int status;

    exec_pid = fork();
    if (exec_pid == 0){
        pid_t pid = getpid();

	/* Make this process the process group leader */
	setpgid(pid, pid);

	action.sa_handler = SIG_DFL;
	sigemptyset(&action.sa_mask);
	action.sa_flags = 0;
	sigaction(SIGXCPU, &action, 0);
	sigaction(SIGXFSZ, &action, 0);

	execvp(argv[0], argv);
	perror("exec");
	
	exit(EXIT_FAILURE); /* could not exec binary */
    } else {
	if (timeout) {
	    /* We register the alarm handler in the parent process. If
             * we would put the alarm in the child process, the child
             * process could overrule it.  
	     */
	    action.sa_handler = alarm_handler;
	    sigemptyset(&action.sa_mask);
	    action.sa_flags = 0;
	    sigaction(SIGALRM, &action, 0);
	    alarm(timeout);	
	}

	while(waitpid(exec_pid, &status, 0) != exec_pid)
	    ;

	if (WIFEXITED(status)) { /* Terminated normally */
	    return WEXITSTATUS(status);
	} else if (WIFSIGNALED(status)) { /* Got a signal */
	    switch(WTERMSIG(status)) {
	    case SIGXCPU:
		invocation(stderr, "Out of CPU bounds: ", argv);
		return SIGXCPU;
	    case SIGXFSZ:
		invocation(stderr, "Out of file space: ", argv);
		return SIGXFSZ;
	    case SIGSEGV:
		invocation(stderr, "Out of stack space: ", argv);
		return SIGSEGV;
	    default:
		if (exec_timeout) {
		    invocation(stderr, "Timeout: ", argv);
		} else {
		    invocation(stderr, "General failure: ", argv);
		}
		return 1;
	    }
	}

	abort();
    }

    abort();
}


static void
usage(void)
{
    fprintf(stderr, "Usage: %s\n"
	    "\t-timeout <seconds>\n"
	    "\t<progname> [<arguments>]\n", progname);
    exit(EXIT_FAILURE);
}

static int
parse_bytes(char *s) 
{
    char *ptr;
    long res;

    res = strtol(s, &ptr, 10);
    if (tolower(*ptr) == 'k') {
	res *= 1024;
    } else if (tolower(*ptr) == 'm') {
	res *= 1024 * 1024;
    } else if (tolower(*ptr) == 'g') {
	res *= 1024 * 1024 * 1024;
    } else if (*ptr) {
	fprintf(stderr, "Garbage at end of byte count %s\n", s);
	exit(EXIT_FAILURE);
    }

    return res;
}

static void
parse_args(int argc, char **argv)
{
    progname = argv[0];
    argv++; argc--;

    while(argc && argv[0][0] == '-') {
	if (strcmp(argv[0], "-help") == 0) {
	    usage();
	} else if (strcmp(argv[0], "-timeout") == 0) {
	    argc--; argv++;
	    if (argc == 0) usage();
	    timeout = atoi(argv[0]);
	} else {
	    usage();
	}

	argv++; argc--;
    } 

    if (argc == 0) {
	usage();
    }

    exec_argv = argv;
}


int
main(int argc, char **argv)
{
    int x;

    parse_args(argc, argv);

    x=limit(exec_argv);

    return x;
}

