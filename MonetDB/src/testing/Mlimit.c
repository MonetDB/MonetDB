#ifdef HAVE_CONFIG_H
#include <config.h>
#endif

#include <stdio.h>
#include <sys/time.h>
#include <sys/types.h>
#include <sys/stat.h>
#include <fcntl.h>
#include <unistd.h>
#include <sys/wait.h>
#include <signal.h>
#include <stdlib.h>
#include <ctype.h>
#include <assert.h>

#ifdef HAVE_SYS_RESOURCE_H 
#include <sys/resource.h>
#endif

#ifdef HAVE_RLIMIT 
#include <rlimit.h>
#endif


#if defined(__CYGWIN32__)
typedef int rlim_t;
#endif

#ifdef __CYGWIN32__
#define RLIM_INFINITY ((unsigned int)-1)
#endif


#define DEFAULT_TIMEOUT 0

#define DEFAULT_CORE   0             /* bytes */
#define DEFAULT_CPU    500	     /* seconds */
#define DEFAULT_DATA   RLIM_INFINITY /* bytes */
#define DEFAULT_FSIZE  RLIM_INFINITY /* bytes */
#define DEFAULT_NOFILE RLIM_INFINITY /* Nr of file descriptors */
#define DEFAULT_STACK  RLIM_INFINITY /* bytes */
#ifdef RLIMIT_VMEM
#define DEFAULT_VMEM   RLIM_INFINITY /* bytes */
#endif


static int timeout = DEFAULT_TIMEOUT;
static rlim_t core = DEFAULT_CORE;
static rlim_t cpu = DEFAULT_CPU;
static rlim_t data = DEFAULT_DATA;
static rlim_t fsize =  DEFAULT_FSIZE;
static rlim_t nofile = DEFAULT_NOFILE;
static rlim_t stack = DEFAULT_STACK;
#ifdef RLIMIT_VMEM
static rlim_t vmem = DEFAULT_VMEM;
#endif

static char *progname;

static pid_t exec_pid;
static char **exec_argv;
static int exec_timeout = 0;


static rlim_t
bounded_limit(rlim_t bound, rlim_t limit)
{
    if (limit <= bound) return limit;
    if (limit != RLIM_INFINITY) {
	fprintf(stderr, "Bound exceeeds hard limit (%ld > %ld)\n", (long)limit, (long)bound);
    }
    return bound;
}


static void
invocation(FILE *fp, char *prefix, char **argv)
{
    fprintf(fp, "%s", prefix);
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
    kill(-exec_pid, SIGKILL);
}

static void
limit(char **argv)
{
    struct sigaction action;
    struct rlimit rlim;
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

	if (getrlimit(RLIMIT_CORE, &rlim) != 0) perror("getrlimit(CORE)");
	rlim.rlim_max = rlim.rlim_cur = bounded_limit(rlim.rlim_max, core);
	if (setrlimit(RLIMIT_CORE, &rlim) != 0) perror("setrlimit(CORE)");
	
	if (getrlimit(RLIMIT_CPU, &rlim) != 0) perror("getrlimit(CPU)");
	rlim.rlim_max = rlim.rlim_cur = bounded_limit(rlim.rlim_max, cpu);
	if (setrlimit(RLIMIT_CPU, &rlim) != 0) perror("setrlimit(CPU)");
	
	if (getrlimit(RLIMIT_DATA, &rlim) != 0) perror("getrlimit(DATA)");
	rlim.rlim_max = rlim.rlim_cur = bounded_limit(rlim.rlim_max, data);
	if (setrlimit(RLIMIT_DATA, &rlim) != 0) perror("setrlimit(DATA)");
	
	if (getrlimit(RLIMIT_FSIZE, &rlim) != 0) perror("getrlimit(FSIZE)");
	rlim.rlim_max = rlim.rlim_cur = bounded_limit(rlim.rlim_max, fsize);
	if (setrlimit(RLIMIT_FSIZE, &rlim) != 0) perror("setrlimit(FSIZE)");
	
	if (getrlimit(RLIMIT_NOFILE, &rlim) != 0) perror("getrlimit(NOFILE)");
	rlim.rlim_max = rlim.rlim_cur = bounded_limit(rlim.rlim_max, nofile);
	if (setrlimit(RLIMIT_NOFILE, &rlim) != 0) perror("setrlimit(NOFILE)");
	
	if (getrlimit(RLIMIT_STACK, &rlim) != 0) perror("getrlimit(STACK)");
	rlim.rlim_max = rlim.rlim_cur = bounded_limit(rlim.rlim_max, stack);
	if (setrlimit(RLIMIT_STACK, &rlim) != 0) perror("setrlimit(STACK)");
	
#ifdef RLIMIT_VMEM
	if (getrlimit(RLIMIT_VMEM, &rlim) != 0) perror("getrlimit(VMEM)");
	rlim.rlim_max = rlim.rlim_cur = bounded_limit(rlim.rlim_max, vmem);
	if (setrlimit(RLIMIT_VMEM, &rlim) != 0) perror("setrlimit(VMEM)");
#endif

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
	    return;
	} else if (WIFSIGNALED(status)) { /* Got a signal */
	    switch(WTERMSIG(status)) {
	    case SIGXCPU:
		invocation(stderr, "Out of CPU bounds: ", argv);
		return;
	    case SIGXFSZ:
		invocation(stderr, "Out of file space: ", argv);
		return;
	    case SIGSEGV:
		invocation(stderr, "Out of stack space: ", argv);
		return;
	    default:
		if (exec_timeout) {
		    invocation(stderr, "Timeout: ", argv);
		} else {
		    invocation(stderr, "General failure: ", argv);
		}
		return;
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
	    "\t-core <core size>\n"
	    "\t-cpu <seconds>\n"
	    "\t-data <heap size>\n"
	    "\t-fsize <file size>\n"
	    "\t-nofile <nr files>\n"
	    "\t-stack <stack size>\n"
	    "\t-vmem <vmem size>\n"
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
	} else if (strcmp(argv[0], "-core") == 0) {
	    argc--; argv++;
	    if (argc == 0) usage();
	    core = parse_bytes(argv[0]);
	} else if (strcmp(argv[0], "-cpu") == 0) {
	    argc--; argv++;
	    if (argc == 0) usage();
	    cpu = atoi(argv[0]);
	} else if (strcmp(argv[0], "-data") == 0 ||
		   strcmp(argv[0], "-heap") == 0) {
	    argc--; argv++;
	    if (argc == 0) usage();
	    data = parse_bytes(argv[0]);
	} else if (strcmp(argv[0], "-fsize") == 0) {
	    argc--; argv++;
	    if (argc == 0) usage();
	    fsize = parse_bytes(argv[0]);
	} else if (strcmp(argv[0], "-nofile") == 0) {
	    argc--; argv++;
	    if (argc == 0) usage();
	    nofile = atoi(argv[0]);
	} else if (strcmp(argv[0], "-stack") == 0) {
	    argc--; argv++;
	    if (argc == 0) usage();
	    stack = parse_bytes(argv[0]);
	} else if (strcmp(argv[0], "-vmem") == 0) {
	    argc--; argv++;
	    if (argc == 0) usage();
#ifdef RLIMIT_VMEM
	    vmem = parse_bytes(argv[0]);
#else
	    fprintf(stderr, "Ignoring unsupported option '-vmem'\n");
#endif
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
    parse_args(argc, argv);

    limit(exec_argv);

    return 0;
}

