#include <stdio.h>
#include <string.h>
#include "disclaimer.h"

#ifdef HAVE_CONFIG_H
#include <config.h>
#endif

#define END_OF_HEADER_MARKER  "@'EOHMARKER (DO NOT EDIT THIS LINE)"
#if defined(WIN32) || defined(CYGWIN32)
extern char     *optarg;
extern int      optind, opterr, optopt;
#endif
#ifdef HAVE_GETOPT_H
#include <getopt.h>
#endif

char *inputfile,*outputfile,*prefixfile;

void printUsage(char *progname)
{
    fprintf(stderr,"Usage: %s [-f <prefix>] [-o <outfile>] mxfile \n",progname);
}

FILE* stripFile(FILE *fp)
{
    char line[16384];
    int ret,i;

    fseek(fp,0,0);
    while(!feof(fp))
        {
            i=0;
            do
                {
                    ret=fread(&line[i++],1,1,fp);
                }
            while(ret && line[i-1]!='\n');
            if (strstr(line,END_OF_HEADER_MARKER)) return fp;
        }
    fseek(fp,0,0);
    return fp;
}

FILE* openFile(char *name, char *mode)
{
    FILE* fp=fopen(name,mode);
    
    if (!fp)
        {
            fprintf(stderr,"Can't open inputfile '%s' (aborting).\n",inputfile);
            exit(1);
        }
    return fp;
}

void processFile(void)
{
    FILE*fp,*op;
    int temp=0,ret;
    char letter,cmd[256];
    
    if (!outputfile)
        {
            outputfile=(char *)malloc(strlen(inputfile)+2);
            outputfile[0]='.';
            strcpy(&outputfile[1],inputfile);
            temp=1;
        }
    fp=stripFile(openFile(inputfile,"r"));
    op=openFile(outputfile,"w");

    if (prefixfile)
        strcpy(disclaimerfile,prefixfile);
    else
        strcpy(disclaimerfile,"");
    
    insertDisclaimer(op,outputfile);
    fwrite(END_OF_HEADER_MARKER,strlen(END_OF_HEADER_MARKER),1,op);
    fwrite("\n",1,1,op);
    

    while(!feof(fp))
        {
            ret=fread(&letter,1,1,fp);
            if (!ret) break;
            fwrite(&letter,1,1,op);
        }
    fclose(fp);
    fclose(op);
    if (temp)
        {
            sprintf(cmd,"mv %s %s",outputfile, inputfile);
            system(cmd);
        }
}

int main(int argc,char **argv)
{
    int option;
    
    while ((option=getopt(argc,argv,"f:o:"))!=EOF)
        switch (option) 
            {
            case 'f':
                prefixfile=(char *)malloc(strlen(optarg)+1);
                strcpy(prefixfile,optarg);
                break;
            case 'o':
                outputfile=(char *)malloc(strlen(optarg)+1);
                strcpy(outputfile,optarg);
                break;
            default:
                printUsage(argv[0]);
                exit(0);
            }
    if (optind<argc)
        {
            inputfile=(char*)malloc(strlen(argv[optind]));
            strcpy(inputfile,argv[optind]);
        }
    else 
        {
            printUsage(argv[0]);
            exit(1);
        }
    if (outputfile)
        if (!strcmp(outputfile,inputfile))
            {
                printf("inputfile and outputfile must be different names (aborting).\n");
                exit(1);
            }
    
    processFile();
    return 0;
}
