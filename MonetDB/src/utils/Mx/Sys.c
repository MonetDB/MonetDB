#include	<stdio.h>
#include	<ctype.h>
#include 	<stdlib.h>
#include 	<string.h>

#include	"Mx.h"
#include	"MxFcnDef.h"
#include 	<stdarg.h>    /* va_alist.. */

int mx_out = 1;
extern int somethingPrinted; /* used for preventing to empty display lines */

/* VARARGS */
void
ofile_printf(char *format,...)
{
        va_list ap; 
        va_start (ap,format);

/*	format = va_arg(ap, char*);*/
	if (mx_out&1) vfprintf(ofile, format, ap);
	if (ofile_index && (mx_out&2)) vfprintf(ofile_index, format, ap);
	if (ofile_body && (mx_out&4)) vfprintf(ofile_body, format, ap);
        va_end (ap); 
	somethingPrinted++;
}

void
ofile_puts(s)
char* s;
{
	if (mx_out&1) fputs(s, ofile);
	if (ofile_index && (mx_out&2)) fputs(s, ofile_index);
	if (ofile_body && (mx_out&4)) fputs(s, ofile_body);
	somethingPrinted++;
}

void
ofile_putc(char c)
{
	if (mx_out&1) fputc(c, ofile);
	if (ofile_index && (mx_out&2)) fputc(c, ofile_index);
	if (ofile_body && (mx_out&4)) fputc(c, ofile_body);
	somethingPrinted++;
}


void	Fatal(char *fcn, char *format,...)
{
    va_list ap;
    va_start (ap,format);
    
	fprintf(stderr, "Mx(%s):", fcn);
	vfprintf(stderr, format, ap);
	if( mx_file )
		fprintf(stderr, "[%s:%d]", mx_file, mx_line);
	fprintf(stderr, ".\n");
        va_end(ap);
        
	exit(1);
}


char *	Malloc(size)
int	size;
{
char *	buf;

	if( (buf= malloc((unsigned) size)) == 0 )
		Fatal("Malloc", "Not enough memory");
	return buf;
}

void	Free(ptr)
char *	ptr;
{
/*
	free(ptr);
 */
}

char *	StrDup(str)
char *	str;
{
	return strcpy(Malloc(strlen(str) + 1), str);
}

/*VARGARGS*/
void	Error(char *format,...)
{
    va_list ap;
    va_start(ap,format);
    
	fprintf(stderr, "Mx:");
	vfprintf(stderr, format, ap);
	if( mx_file )
		fprintf(stderr, "[%s:%d]", mx_file, mx_line);
	fprintf(stderr, ".\n");
	mx_err++;
        va_end(ap);
        
}

/*VARGARGS1*/
void	Message(char *format,...)
{
    va_list ap;
    va_start (ap,format);
    
	vfprintf(stderr, format, ap);
	if( mx_file )
		fprintf(stderr, "[%s:%d]", mx_file, mx_line);
	fprintf(stderr, ".\n");
        va_end(ap);
        
}
