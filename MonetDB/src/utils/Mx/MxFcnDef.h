/*
 * The contents of this file are subject to the MonetDB Public
 * License Version 1.0 (the "License"); you may not use this file
 * except in compliance with the License. You may obtain a copy of
 * the License at
 * http://monetdb.cwi.nl/Legal/MonetDBLicense-1.0.html
 *
 * Software distributed under the License is distributed on an "AS
 * IS" basis, WITHOUT WARRANTY OF ANY KIND, either express or
 * implied. See the License for the specific language governing
 * rights and limitations under the License.
 *
 * The Original Code is the Monet Database System.
 *
 * The Initial Developer of the Original Code is CWI.
 * Portions created by CWI are Copyright (C) 1997-2004 CWI.
 * All Rights Reserved.
 */

#ifdef UNIX
#define		NOARGS		(void)
#define		ARGS(args)	args
#else
#define		NOARGS		(void)
#define		ARGS(args)	args
#endif

/* System

extern	char *	malloc ARGS((unsigned int));
extern	int	free ARGS((char*));
extern	long	time ARGS((int));
extern	char *	ctime ARGS((long));
extern	char *	strcpy ARGS((char*, char*));
extern	int	strlen ARGS((char*));
extern	int	strcmp ARGS((char*, char*));
extern	char *	strchr ARGS((char*, char));
extern	char *	strrchr ARGS((char*, char));
extern	int	printf ARGS((char*, ...));
extern	char*	sprintf ARGS((char*, char*, ...)); 
extern	int	sscanf ARGS((char*, char*, ...)); 
extern	int	fgetc ARGS((FILE*));
extern	FILE *	fopen ARGS((char*, char*));
extern	FILE *	freopen ARGS((char*, char*, FILE*));
#ifndef UNIX
extern	int	feof ARGS((FILE*));
#endif
extern	int	fclose ARGS((FILE*));
extern	int	exit ARGS((int));
*/

/* Code.c
 */
extern	void	GenCode NOARGS;
extern	void	CodeBlk ARGS((char*));
extern	void	CodeCall ARGS((char*));
extern	void	CodeSub ARGS((char*));
extern	char *	CodeSubBlk ARGS((char*, char**));
extern	void	CodeLine NOARGS;
extern	void	UnRef ARGS((char *));
extern	Tok* 	solveCond ARGS((Tok *));

/* Def.c
 */
extern	void	InitDef NOARGS;
extern	void	MakeDefs ARGS((char *));
extern	CmdCode	DefDir NOARGS;
extern	char *	DefCmd NOARGS;
extern	char *	DefBlk NOARGS;
extern	void	DefNl NOARGS;
extern	Def *	NwDef ARGS((CmdCode, int, int, int));
extern	Def *	GetDef ARGS((char*));
extern	void	DbDef ARGS((Def*));
extern  char *  dir2str ARGS((CmdCode));
extern  char *  dir2ext ARGS((CmdCode));
extern  CmdCode lookup ARGS((char*));
extern  int     allTrue NOARGS;
extern  char *  substr ARGS((char*,char*));


/* Display.c
 */
extern	void	PrFontStr ARGS((char*, char));
extern	void	PrModeStr ARGS((char*, char));
extern	void	PrCmd ARGS((char *));
extern	void	PrText ARGS((char *));
extern	void	PrCode ARGS((char *));
extern	void	PrRule ARGS((char *));

extern	void	PrPrelude ARGS((char*));
extern	void	PrPostlude NOARGS;

/* Form.c
 */
extern	void	GenForm NOARGS;
extern	void	FormBlk ARGS((Def*));
extern	void	FormSub ARGS((char*));
extern	void	FormTitle NOARGS;
extern	void	FormHeader NOARGS;
extern	void	FormMod ARGS((char*, int));
extern	void	FormSec ARGS((char*, int, int));	
extern	void	FormPar ARGS((char*));
extern  void    FormIf ARGS((Def*));
/* Index.c
 */
extern	void	InitIndex NOARGS;
extern	void	IndexTable ARGS((int, char*));
extern	void	IndexEntry ARGS((int, char*, int, int));
extern	void	SortIndex NOARGS;
extern	void	PrCont NOARGS;
extern	void	PrIndex NOARGS;
extern	void	PrItable ARGS((int));

/* Io.c
 */
extern  char *  FileName ARGS((char*));
extern  void    UpdateFiles NOARGS;
extern  void	OutputDir ARGS((char *));
extern	File *	GetFile ARGS((char*,CmdCode));
extern	int	HasSuffix ARGS((char*, char*));
extern	char *	BaseName ARGS((char*));
extern	char *	TempName ARGS((char*));
extern	void	IoWriteFile ARGS((char*, CmdCode));
extern	void	IoReadFile ARGS((char*));
extern	int	EofFile NOARGS;
extern	void	CloseFile NOARGS;
extern	char	NextChr NOARGS;
extern	char *	NextLine NOARGS;
extern	void	PrevChr ARGS((char));
extern  void    PrevLine NOARGS;

/* Mx.c
 */
#ifndef UNIX
extern	int	Main ARGS((int, char**));
#endif
extern	int	ModeDir ARGS((char));
extern	char *	ExtMode ARGS((int));
extern  int     extract ARGS((CmdCode));


/* Print.c
 */
extern  void    PrCodeline NOARGS;

extern	void	PrEnv ARGS((int));
extern	void	PrRef ARGS((int, int));
extern	void	PrNum ARGS((int));
extern	void	PrStr ARGS((char*));
extern  void    PrTxt ARGS((char*));
extern	void	PrChr ARGS((char));
extern	void	MathOn NOARGS;
extern	void	MathOff NOARGS;
extern	void	HideOn NOARGS;
extern	void	HideOff NOARGS;
extern	int	Hide NOARGS;
extern  void    HideText NOARGS;


/* Sys.c
 */
extern  void    ofile_putc ARGS((char));
extern  void    ofile_puts ARGS((char*));

extern	char *	Malloc ARGS((size_t));
extern	void	Free ARGS((char *));
extern	char *	StrDup ARGS((const char*));
extern	char *	Strndup ARGS((const char*,size_t));
 
extern  void    ofile_printf ARGS((char *,...));
extern	void	Fatal ARGS((char*, char*, ...)); 
extern	void	Error ARGS((char*, ...));
extern	void	Message ARGS((char*, ...));
 
/* Tok.c
 */
extern	Tok *	FstTok ARGS((char*));
extern	Tok *	NxtTok ARGS((Tok*));
extern	Tok *	SkipTok ARGS((Tok*,char));
extern	void	DbTok ARGS((Tok*));
extern	char **	MkArgv ARGS((char*));
extern	char **	RmArgv ARGS((char**));
extern	void	DbArgv ARGS((char**));


/* Mx.c
 */

extern void addextension ARGS((char *));

/* TeX2Html 
 */

extern void bib_print NOARGS;
extern void latex2html ARGS((char*,int,int));
