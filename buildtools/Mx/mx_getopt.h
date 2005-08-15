#ifdef HAVE_GETOPT_H
#include <getopt.h>
#else
#if !HAVE_DECL_GETOPT
extern char *optarg;
extern int optind, opterr;

int getopt(int argc, char **argv, const char *options);
#endif
#endif
