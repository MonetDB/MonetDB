
#include "helpers.h"
#include <ctype.h>
#include <stdlib.h>
#include <string.h>

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
  char* fn=strrchr(path,(int)'/');
  if (fn)
    return (fn+1);
  else
    return path;
}

char* tmpdir()
{
  char* rtrn=getenv("TMPDIR");
  if (!rtrn) rtrn="/tmp";
  return rtrn;
}
