
#ifdef HAVE_CONFIG_H
#include <config.h>
#endif

#include <stdlib.h>
#include <stdio.h>
#include "difflib.h"
#include <unistd.h>
#include <string.h>

#include "monet_options.h"

void showUsage(char *name)
{
    printf("Usage:  %s [-I<exp>] [-C<num>] [-A<num>] [-t<text>] [-r<rev>] <oldfile> <newfile> [<outfile>]\n",name);
    printf("\n");
    printf(" -I<exp>   : ignore lines matching <exp> during diff (optional, default: -I'^#')\n");
    printf(" -C<num>   : use <num> lines of context during diff (optional, default: -C1)\n");
    printf(" -A<num>   : accuracy for diff: 0=lines, 1=words, 2=chars (optional, default: -A1)\n");
    printf(" -t<text>  : text for caption (optional, default: empty)\n");
    printf(" -r<rev>   : revision of old file (optinal, default: empty)\n");
    printf(" <oldfile> : first file for diff\n");
    printf(" <newfile> : second file for diff\n");
    printf(" <outfile> : file for HTML output (optional, default: stdout)\n");
}

int main(int argc, char** argv)
{
  char EMPTY[]="";
  char DEFAULT[]="-I'^#'";
  char ignoreWHITE[]=" -b -B";
  char *old_fn,*new_fn,*html_fn,*caption=EMPTY,*revision=EMPTY,*ignoreEXP=DEFAULT,*ignore;
  int LWC=1,context=1,option;

  while((option=getopt(argc,argv,"hA:C:I:t:r:"))!=EOF)
   switch (option)
     {
       case 'A': LWC=atoi(optarg); break;
       case 'C': context=atoi(optarg); break;
       case 'I': ignoreEXP=(char*)malloc(strlen(optarg)+6);
#ifdef NATIVE_WIN32
		 strcpy(ignoreEXP,"-I");
#else
		 strcpy(ignoreEXP,"'-I");
#endif
		 strcat(ignoreEXP,optarg);
#ifndef NATIVE_WIN32
		 strcat(ignoreEXP,"'");
#endif
		 break;
       case 't': caption=optarg; break;
       case 'r': revision=optarg; break;
       case 'h':
       default:  showUsage(argv[0]); exit(1);
     }
     
  ignore=(char*)malloc(strlen(ignoreEXP)+strlen(ignoreWHITE)+2);
  strcpy(ignore,ignoreEXP);
  strcat(ignore,ignoreWHITE);

  optind--;
  old_fn=((argc>(++optind))?argv[optind]:"-");
  new_fn=((argc>(++optind))?argv[optind]:"-");
  html_fn=((argc>(++optind))?argv[optind]:"-");

  TRACE(fprintf(STDERR,"%s -A %i -C %i %s -t %s -r %s  %s %s %s\n",
                 argv[0],LWC,context,ignore,caption,revision,old_fn,new_fn,html_fn));

  if ( oldnew2html (LWC,context,ignore,old_fn,new_fn,html_fn,caption,revision) )
      fprintf(STDERR,"%s and %s do differ!\n",old_fn,new_fn);
    else
      fprintf(STDERR,"%s and %s are equal.\n",old_fn,new_fn);

  TRACE(fprintf(STDERR,"done.\n"));
  return 0;
}

