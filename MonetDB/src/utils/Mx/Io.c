#include <stdio.h>
#include <ctype.h>
#include <string.h>
#include "Mx.h"
#include "MxFcnDef.h"
#include <sys/types.h>
#include <sys/stat.h> 
#ifndef WIN32
#include <utime.h>
#endif
#include "disclaimer.h"

File	files[M_FILES];
int	nfile = 0;

#define ifile (*fptop)
FILE *  fpstack[16] = { 0 };
FILE ** fptop = fpstack;
FILE *	ofile= 0;
FILE *	ofile_body = 0;
FILE *	ofile_index = 0;

char *outputdir;
char *inputdir;
int outputdir_len;

FILE *fmustopen(char*,char*);
int CompareFiles(char*,char*);


void OutputDir( dir )
char *	dir;
{
    struct stat buf;
    if(stat(dir,&buf)==-1 || !(buf.st_mode&S_IFDIR)){
	fprintf(stderr,"Invalid target directory (%s), using \".\" instead\n",
		dir);
	return;
    }
    outputdir = StrDup(dir);
    outputdir_len = strlen( dir );
}
	
File *	GetFile(s,m)
char *	s;
CmdCode m;
{
File *	f;
char *fname;
char *bname;

        bname = BaseName(s);
        fname = (char *)malloc(outputdir_len + 
                               strlen(bname)+strlen(dir2ext(m))+16);
	
	if (strlen(dir2ext(m)) == 0)
	     sprintf(fname, "%s%c%s", outputdir, DIR_SEP, BaseName(s));
	else
	     sprintf(fname, "%s%c%s.%s", outputdir, DIR_SEP, BaseName(s), dir2ext(m));

	for( f= files; f < files + nfile; f++ ){
		if( strcmp(f->f_name, fname) == 0 ){
			return f;
		}
	}
	if( nfile == M_FILES )
		Fatal("GetFile", "Too many files");

	f->f_name= fname;

	f->f_str = fname+strlen(fname);;
	while(f->f_str>fname && f->f_str[-1]!=DIR_SEP) f->f_str--;

        f->f_tmp = (char *)malloc(strlen(outputdir)+strlen(bname)+strlen(dir2ext(m))+17); 
        sprintf(f->f_tmp,"%s%c%s.%s", outputdir,DIR_SEP,TempName(s), dir2ext(m));
       
	f->f_mode= 0;

	nfile++;
	return f;
}

int	HasSuffix(name, suffix)
char *	name;
char *	suffix;
{
	if( strlen(name) <= strlen(suffix) )
		return 0;	
	return (strcmp(name + strlen(name) - strlen(suffix), suffix) == 0);
}

/* the FileName, BaseName, TempName return names for immediate consumption */
char	bname[1024];

/* the name without preappended subdirectories */
char*	FileName(name)
char*	name;
{
    /* @f /blah/blah indicates an absolute file name, 
       often used in Makefiles etc. ignore path
       @f blah/blah indicates a relative path,
       create sub-directories if necessary
    */
    if(name[0]==DIR_SEP || (name[0]=='.' && name[1]==DIR_SEP))
    	name = (strrchr(name, DIR_SEP)? strrchr(name, DIR_SEP) + 1: name);
    strcpy(bname,name);
    return bname;
}

/* the name without extension */
char *	BaseName(name)
char *	name;
{
    char *b = strrchr(FileName(name),'.');
    if (b != NULL) *b= '\0';
    return bname;
}

/* the name with '.' preappended */
char *	TempName(name)
char *	name;
{
    	char *p,*r = p = bname+strlen(BaseName(name));
	while(--r >= bname)
		if (*r == DIR_SEP) break;
	for(r++, p[1]=0; p > r; p--) 
		p[0] = p[-1]; 
	*r = '.';
	return bname;
}

FILE *fmustopen(fname,mode)
char *fname;
char *mode;
{
    char *p=fname; 
    char *tmp;
    if(*p==DIR_SEP) p++;
    while((tmp=strchr(p,DIR_SEP))!=NULL){
	struct stat buf;	
        *tmp='\0';
        if(stat(fname,&buf)==-1 || !(buf.st_mode&S_IFDIR))
		if (mkdir(fname,S_IRWXU)==-1)
			Fatal("fmustopen:","Can't create directory %s\n",fname);
	p=tmp+1;
	*tmp=DIR_SEP;
    }
    return fopen(fname,mode);

}


char 	fname[1024];

void	IoWriteFile(s, m)
char *	s;
CmdCode	m;
{
    File *	f;
	
    if( ofile )
	fclose(ofile);
    
    f= GetFile(s,m);
    if( (f->f_mode & m) == m ){
	ofile= fmustopen(f->f_tmp, "ab");
	if(ofile==NULL)
	    Fatal("IoWriteFile","can't append to:%s",f->f_tmp);
    } else {
	f->f_mode |= m;
	ofile= fmustopen(f->f_tmp, "wb");
	if(ofile==NULL)
	    Fatal("IoWriteFile","can't create:%s",f->f_tmp);
        if (disclaimer) insertDisclaimer(ofile,f->f_tmp);
    }
}

/* this function replaces the fork of the 'cmp' utility 
 * had to be done on WIN32 because return values were screwed
 */
int CompareFiles(nm1, nm2)
char *nm1, *nm2;
{
	FILE *fp1 = fopen(nm1, "r");	
	FILE *fp2 = fopen(nm2, "r");	
	int ret = 2;

	if (fp2 == NULL) goto done;

	while(!(feof(fp1) || feof(fp2))) {
		if (fgetc(fp1) != fgetc(fp2)) {
			break;
		}
	}
	ret = (feof(fp1) && feof(fp2))?0:1;
	fclose(fp2);
done:	fclose(fp1);
	return ret;
}

void KillLines(FILE *fp, char* pattern, int killprev){
	int lastpos=0, curpos=0, patlen=strlen(pattern);
	char line[8192];

	fflush(fp);
	fseek(fp, 0, SEEK_SET);
	while(fgets(line, 8192, fp)) {	
		int newpos = ftell(fp);
		if (strlen(line)==0) continue;
		if (strncmp(line, pattern, patlen) == 0) {
			int killpos = curpos;
			int killen = newpos-curpos;

			if (killprev && lastpos) {
				killpos = lastpos;
				killen += curpos-lastpos;
			}
			fseek(fp, killpos, SEEK_SET);
			while(killen--) fputc(' ', fp);
			fflush(fp);
			fseek(fp, newpos, SEEK_SET);
		} 
		lastpos = curpos;
		curpos = ftell(fp); 
	}
	fseek(fp, 0, SEEK_END);
}
	
void UpdateFiles()
{
    char *new, *old;
    char cmp[1024];
    File *f;
    int status;
    
    if (ofile) fclose(ofile);
    ofile = NULL;

    for( f = files; f < files + nfile; f++) {
        if (ofile_index) {
	    KillLines(ofile_index,"+</font></td>", 1);
	    KillLines(ofile_body, "+</font></td>", 1);
	    KillLines(ofile_body, "<BASE target=\"_parent\">", 0);
            fclose(ofile_body);  ofile_body=NULL;
            fclose(ofile_index); ofile_index=NULL;
	    if (!itable_done) {
		/* now use the body file as the output file */ 
		unlink(filename_index);
		unlink(f->f_tmp);
		rename(filename_body, f->f_tmp);
	    }
        }
	status = CompareFiles(f->f_tmp, f->f_name);
        switch(status){
        case 0: /* identical files, remove temporary file */
            printf("%s: %s - not modified \n",mx_file,f->f_name);
#ifndef WIN32
	    if (!notouch)
            	utime(f->f_name,0); /* touch the file */
#endif
            unlink(f->f_tmp);
            break;
        case 1: /* different file */
            printf("%s: %s - modified \n",mx_file,f->f_name);
	    unlink(f->f_name);
            if (rename(f->f_tmp, f->f_name)) perror("rename");
            break;
        default: /* new file, move file */
            printf("%s: %s - created \n",mx_file,f->f_name);
            if (rename(f->f_tmp, f->f_name)) perror("rename");
            break;
        }
    }
    nfile = 0;
}

void	IoReadFile(name)
char *	name;
{
	char *p;

	if( !HasSuffix(name, ".mx") )
		Fatal("IoReadFile", "Not a mx-file:%s", name);
	if( (ifile= fopen(name, "r")) == 0 ) 
		Fatal("IoReadFile", "Can't process %s", name);
	p = (inputdir = strdup(name)) + strlen(name);
	while(--p >= inputdir && *p != DIR_SEP);
	p[1] = 0;
	mx_file= name;
	mx_line= 1;
}


void	CloseFile()
{
	fclose(ifile);
	/* mx_file= 0; */
	mx_line= 1;
}

#define MAXLINE 2048


int	EofFile()
{
	if (feof(ifile)) {
		if (fptop == fpstack) return 1;
		fclose(ifile);
		fptop--;
		return EofFile();
	}
	return 0;
}

#define MX_INCLUDE "@include"
#define MX_COMMENT "@'"
static int  fullbuf=0;
static char linebuf[MAXLINE];

char *NextLine()
{
    if (fptop == fpstack) {
	mx_line++;
    }
    if (fullbuf) { 
         fullbuf = 0;  
         return linebuf; 
    } else {
	char *s, *t;
        do {
	  s = fgets(linebuf, MAXLINE, ifile);
	} while (s == NULL && !EofFile()); 

        if (s && strncmp(s, MX_COMMENT, strlen(MX_COMMENT)) == 0) 
            s[0]='\0';
                
	if (s && strncmp(s, MX_INCLUDE, strlen(MX_INCLUDE)) == 0) {
		char path[1024];
		s += strlen(MX_INCLUDE);
	        while(isspace(*s)) s++;
	        for(t=s; *t && !isspace(*t); t++);
		*path = *t = 0;
		if (*inputdir && *s != DIR_SEP) { /* absolute path */
		    sprintf(path, "%s%c", inputdir, DIR_SEP);
	        }
		strcat(path, s);
		fptop[1] = fopen(path, "r");
		if (fptop[1] == NULL) {
			fprintf(stderr, "Mx: failed to include '%s'.\n", s);
		} else {
			fptop++;
		}
		mx_line++;
		return NextLine();
	}
	return s;
    }
}

void PrevLine()
{
    if (fptop == fpstack) {
	mx_line--;
    }
    fullbuf = 1; 
}

