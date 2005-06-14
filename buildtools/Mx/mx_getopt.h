#ifdef HAVE_GETOPT_H
#include <getopt.h>
#else
extern char *optarg;
extern int optind, opterr;

int getopt(int argc, char **argv, const char *options);
#endif
