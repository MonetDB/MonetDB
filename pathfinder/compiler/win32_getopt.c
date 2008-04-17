/*
 *  win32_getopt.c - my re-implementation of getopt.
 *  Copyright 1997, 2000, 2001, 2002, 2006, Benjamin Sittler
 *
 *  Permission is hereby granted, free of charge, to any person
 *  obtaining a copy of this software and associated documentation
 *  files (the "Software"), to deal in the Software without
 *  restriction, including without limitation the rights to use, copy,
 *  modify, merge, publish, distribute, sublicense, and/or sell copies
 *  of the Software, and to permit persons to whom the Software is
 *  furnished to do so, subject to the following conditions:
 *
 *  The above copyright notice and this permission notice shall be
 *  included in all copies or substantial portions of the Software.
 *
 *  THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND,
 *  EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF
 *  MERCHANTABILITY, FITNESS FOR A PARTICULAR PURPOSE AND
 *  NONINFRINGEMENT.  IN NO EVENT SHALL THE AUTHORS OR COPYRIGHT
 *  HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER LIABILITY,
 *  WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
 *  OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER
 *  DEALINGS IN THE SOFTWARE.
 */

#include <sys/types.h>
#include <stdlib.h>
#include <stdio.h>
#include <string.h>

struct option {
  const char *name;
  int has_arg;
  int *flag;
  int val;
};

/* human-readable values for has_arg */
#undef no_argument
#define no_argument 0
#undef required_argument
#define required_argument 1
#undef optional_argument
#define optional_argument 2

int win32_optind=1, win32_opterr=1, win32_optopt=0;
char *win32_optarg=0;

/* reset argument parser to start-up values */
int win32_getopt_reset(void)
{
    win32_optind = 1;
    win32_opterr = 1;
    win32_optopt = 0;
    win32_optarg = 0;
    return 0;
}

/* this is the plain old UNIX getopt, with GNU-style extensions. */
/* if you're porting some piece of UNIX software, this is all you need. */
/* this supports GNU-style permution and optional arguments */

int win32_getopt(int argc, char * argv[], const char *opts)
{
  static int charind=0;
  const char *s;
  char mode = '+', colon_mode = '+';
  int off = 0, opt = -1;

  win32_optarg = 0;
  if(charind) {
    win32_optopt = argv[win32_optind][charind];
    for(s=opts+off; *s; s++) if(win32_optopt == *s) {
      charind++;
      if((*(++s) == ':') || ((win32_optopt == 'W') && (*s == ';'))) {
        if(argv[win32_optind][charind]) {
          win32_optarg = &(argv[win32_optind++][charind]);
          charind = 0;
        } else if(*(++s) != ':') {
          charind = 0;
          if(++win32_optind >= argc) {
            if(win32_opterr) fprintf(stderr,
                                "%s: option requires an argument -- %c\n",
                                argv[0], win32_optopt);
            opt = (colon_mode == ':') ? ':' : '?';
            goto win32_getopt_ok;
          }
          win32_optarg = argv[win32_optind++];
        }
      }
      opt = win32_optopt;
      goto win32_getopt_ok;
    }
    if(win32_opterr) fprintf(stderr,
                        "%s: illegal option -- %c\n",
                        argv[0], win32_optopt);
    opt = '?';
    if(argv[win32_optind][++charind] == '\0') {
      win32_optind++;
      charind = 0;
    }
  win32_getopt_ok:
    if(charind && ! argv[win32_optind][charind]) {
      win32_optind++;
      charind = 0;
    }
  } else if((win32_optind >= argc) ||
             ((argv[win32_optind][0] == '-') &&
              (argv[win32_optind][1] == '-') &&
              (argv[win32_optind][2] == '\0'))) {
    win32_optind++;
    opt = -1;
  } else if((argv[win32_optind][0] != '-') ||
             (argv[win32_optind][1] == '\0')) {
    char *tmp;
    int i, j, k;

    if(mode == '+') opt = -1;
    else if(mode == '-') {
      win32_optarg = argv[win32_optind++];
      charind = 0;
      opt = 1;
    } else {
      for(i=j=win32_optind; i<argc; i++) if((argv[i][0] == '-') &&
                                        (argv[i][1] != '\0')) {
        win32_optind=i;
        opt=win32_getopt(argc, argv, opts);
        while(i > j) {
          tmp=argv[--i];
          for(k=i; k+1<win32_optind; k++) argv[k]=argv[k+1];
          argv[--win32_optind]=tmp;
        }
        break;
      }
      if(i == argc) opt = -1;
    }
  } else {
    charind++;
    opt = win32_getopt(argc, argv, opts);
  }
  if (win32_optind > argc) win32_optind = argc;
  return opt;
}

/* this is the extended getopt_long{,_only}, with some GNU-like
 * extensions. Implements _getopt_internal in case any programs
 * expecting GNU libc getopt call it.
 */

int _win32_getopt_internal(int argc, char * argv[], const char *shortopts,
                     const struct option *longopts, int *longind,
                     int long_only)
{
  char mode = '+', colon_mode = '+';
  int shortoff = 0, opt = -1;

  win32_optarg = 0;
  if((win32_optind >= argc) ||
      ((argv[win32_optind][0] == '-') &&
       (argv[win32_optind][1] == '-') &&
       (argv[win32_optind][2] == '\0'))) {
    win32_optind++;
    opt = -1;
  } else if((argv[win32_optind][0] != '-') ||
            (argv[win32_optind][1] == '\0')) {
    char *tmp;
    int i, j, k;

    opt = -1;
    if(mode == '+') return -1;
    else if(mode == '-') {
      win32_optarg = argv[win32_optind++];
      return 1;
    }
    for(i=j=win32_optind; i<argc; i++) if((argv[i][0] == '-') &&
                                    (argv[i][1] != '\0')) {
      win32_optind=i;
      opt=_win32_getopt_internal(argc, argv, shortopts,
                              longopts, longind,
                              long_only);
      while(i > j) {
        tmp=argv[--i];
        for(k=i; k+1<win32_optind; k++)
          argv[k]=argv[k+1];
        argv[--win32_optind]=tmp;
      }
      break;
    }
  } else if((!long_only) && (argv[win32_optind][1] != '-'))
    opt = win32_getopt(argc, argv, shortopts);
  else {
    int charind, offset;
    int found = 0, ind, hits = 0;

    if(((win32_optopt = argv[win32_optind][1]) != '-') && ! argv[win32_optind][2]) {
      int c;

      ind = shortoff;
      while((c = shortopts[ind++])) {
        if(((shortopts[ind] == ':') ||
            ((c == 'W') && (shortopts[ind] == ';'))) &&
           (shortopts[++ind] == ':'))
          ind ++;
        if(win32_optopt == c) return win32_getopt(argc, argv, shortopts);
      }
    }
    offset = 2 - (argv[win32_optind][1] != '-');
    for(charind = offset;
        (argv[win32_optind][charind] != '\0') &&
          (argv[win32_optind][charind] != '=');
        charind++);
    for(ind = 0; longopts[ind].name && !hits; ind++)
      if((strlen(longopts[ind].name) == (size_t) (charind - offset)) &&
         (strncmp(longopts[ind].name,
                  argv[win32_optind] + offset, charind - offset) == 0))
        found = ind, hits++;
    if(!hits) for(ind = 0; longopts[ind].name; ind++)
      if(strncmp(longopts[ind].name,
                 argv[win32_optind] + offset, charind - offset) == 0)
        found = ind, hits++;
    if(hits == 1) {
      opt = 0;

      if(argv[win32_optind][charind] == '=') {
        if(longopts[found].has_arg == 0) {
          opt = '?';
          if(win32_opterr) fprintf(stderr,
                             "%s: option `--%s' doesn't allow an argument\n",
                             argv[0], longopts[found].name);
        } else {
          win32_optarg = argv[win32_optind] + ++charind;
          charind = 0;
        }
      } else if(longopts[found].has_arg == 1) {
        if(++win32_optind >= argc) {
          opt = (colon_mode == ':') ? ':' : '?';
          if(win32_opterr) fprintf(stderr,
                             "%s: option `--%s' requires an argument\n",
                             argv[0], longopts[found].name);
        } else win32_optarg = argv[win32_optind];
      }
      if(!opt) {
        if (longind) *longind = found;
        if(!longopts[found].flag) opt = longopts[found].val;
        else *(longopts[found].flag) = longopts[found].val;
      }
      win32_optind++;
    } else if(!hits) {
      if(offset == 1) opt = win32_getopt(argc, argv, shortopts);
      else {
        opt = '?';
        if(win32_opterr) fprintf(stderr,
                           "%s: unrecognized option `%s'\n",
                           argv[0], argv[win32_optind++]);
      }
    } else {
      opt = '?';
      if(win32_opterr) fprintf(stderr,
                         "%s: option `%s' is ambiguous\n",
                         argv[0], argv[win32_optind++]);
    }
  }
  if (win32_optind > argc) win32_optind = argc;
  return opt;
}

int win32_getopt_long(int argc, char * argv[], const char *shortopts,
                const struct option *longopts, int *longind)
{
  return _win32_getopt_internal(argc, argv, shortopts, longopts, longind, 0);
}

int win32_getopt_long_only(int argc, char * argv[], const char *shortopts,
                const struct option *longopts, int *longind)
{
  return _win32_getopt_internal(argc, argv, shortopts, longopts, longind, 1);
}

