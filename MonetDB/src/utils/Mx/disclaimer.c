#include <stdio.h>
#include <stdlib.h>
#include <string.h>

#include <limits.h>
#include "disclaimer.h"

int disclaimer=0;
char disclaimerfile[256];

static char defaultfile[]="COPYRIGHT";

static FILE *openDisclaimerFile(char *filename)
{
    FILE *fp;

    if (!filename || strlen(filename)<1) filename=defaultfile;

    if ((fp=fopen(filename,"r"))==0)
        fprintf(stderr,"Mx: can't open disclaimer file '%s' (skipping).\n",
                filename);
    return fp;
}

static void writeDisclaimer(FILE *fp, char *comment_start, char *prefix, 
                            char *comment_end, char *filename)
{

    FILE *dfile;
    char line[DISC_WIDTH+2];
    int prefixLength=strlen(prefix),i,ret;
    
    dfile=openDisclaimerFile(filename);

    if (!dfile) return;
    
    if (strlen(comment_start)>0)
        {
            fwrite(comment_start,strlen(comment_start),1,fp);
            fwrite("\n",1,1,fp);
        }
    
    memcpy(line,prefix,prefixLength);

    while(!feof(dfile))
        {
            i=prefixLength;
            do
                {
                    ret=fread(&line[i++],1,1,dfile);
                }
            while(i<DISC_WIDTH && line[i-1]!='\n' && ret);
            if (!ret) break;
            
            if (line[i-1]!='\n') line[i++]='\n';
            line[i]='\0';
            fwrite(line,strlen(line),1,fp);
        }
    if (strlen(comment_end)>0)
        {
            fwrite(comment_end,strlen(comment_end),1,fp);
            fwrite("\n",1,1,fp);
        }
    
    fwrite("\n",1,1,fp);
    fclose(dfile);
    
}

#define DISC_SUFFIXES  8

/* format <suffix> <comment_begin> <comment_prefix> <comment_end> */
static char suffixes[DISC_SUFFIXES*4][128]={
    "c","/*"," * "," */",
    "h","/*"," * "," */",
    MX_CXX_SUFFIX,"/*"," * "," */",
    "html","<!--"," + "," -->",
    "tex","","% ","",
    "mil","","# ","",
    "mal","","# ","",
    "mx","","@' ",""
};

void insertDisclaimer(FILE *fp, char *rfilename)
{
    int i=0, rlength=strlen(rfilename),sfx;
    char *suffix=(char *)malloc(strlen(rfilename));

    for (i=rlength;i>=0;i--)
        if (rfilename[i]=='.') break;

    if (i<0 || i==rlength) return; /* no suffix found => no disclaimer */

    memcpy(suffix,&rfilename[i+1],rlength-i);
    
    for (sfx=0;sfx<DISC_SUFFIXES;sfx++)
        if (!strcmp(suffixes[sfx*4],suffix)) break;
    
    if (sfx<DISC_SUFFIXES)
        {
            writeDisclaimer(fp,suffixes[sfx*4+1],suffixes[sfx*4+2],
                            suffixes[sfx*4+3],disclaimerfile);
/*
            printf("file=%s sfx=%d suffix=%s sfxb=%s sfxx=%s sfxe=%s\n",
                   rfilename,sfx,suffix,
                   suffixes[sfx*4+1],suffixes[sfx*4+2],suffixes[sfx*4+3]);
                   */
        }
}
