
#ifdef HAVE_CONFIG_H
#include <config.h>
#endif

#include "helpers.h"
#include <ctype.h>
#include <stdlib.h>
#include <string.h>

#ifndef DIR_SEP
# define DIR_SEP '/'
#endif

void ErrXit (char* text1, char* text2, int num)
{
  fprintf(stderr,"ERROR: %s%s\n",text1,text2);
  exit(num);
}
/* ErrXit */


FILE* Rfopen (char* name)
{
  FILE* fp;
  if(!strcmp(name,"-"))
    fp=stdin;
  else
    if(!(fp=fopen(name,"r"))) ErrXit("could not read file ",name,1);
  return fp;
}
/* Rfopen */


FILE* Wfopen (char* name)
{
  FILE* fp;
  if(!strcmp(name,"-"))
    fp=stdout;
  else
    if(!(fp=fopen(name,"w"))) ErrXit("could not write file ",name,1);
  return fp;
}
/* Wfopen */


FILE* Afopen (char* name)
{
  FILE* fp;
  if(!strcmp(name,"-"))
    fp=stdout;
  else
    if(!(fp=fopen(name,"a"))) ErrXit("could not append file ",name,1);
  return fp;
}
/* Afopen */


char* strconcat (char* a, char* b)
{
  char *s=(char*)malloc(strlen(a)+strlen(b)+1);
  sprintf(s,"%s%s",a,b);
  return s;
}
/* strconcat */


int isalpha_ (int c)
{
  return (isalpha(c)||(c=='_'));
}


char* filename (char* path)
{
  char* fn=strrchr(path,(int)DIR_SEP);
  if (fn)
    return (fn+1);
  else
    return path;
}

char* default_tmpdir = "/tmp";

char* tmpdir()
{
  char* rtrn=getenv("TMPDIR");
  default_tmpdir[0] = DIR_SEP;
  if (!rtrn) rtrn=default_tmpdir;
  return rtrn;
}
