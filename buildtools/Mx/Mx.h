/*
 * The contents of this file are subject to the MonetDB Public License
 * Version 1.1 (the "License"); you may not use this file except in
 * compliance with the License. You may obtain a copy of the License at
 * http://www.monetdb.org/Legal/MonetDBLicense
 *
 * Software distributed under the License is distributed on an "AS IS"
 * basis, WITHOUT WARRANTY OF ANY KIND, either express or implied. See the
 * License for the specific language governing rights and limitations
 * under the License.
 *
 * The Original Code is the MonetDB Database System.
 *
 * The Initial Developer of the Original Code is CWI.
 * Portions created by CWI are Copyright (C) 1997-July 2008 CWI.
 * Copyright August 2008-2011 MonetDB B.V.
 * All Rights Reserved.
 */

/* Debug control
 */

#if defined(__MINGW32__)
#define NATIVE_WIN32
#endif 

#ifdef NATIVE_WIN32
# include <io.h>
# include <direct.h>
# define mkdir(path,op) _mkdir(path)
#endif

#include 	<stdio.h>

#ifdef NATIVE_WIN32
#define snprintf _snprintf
#endif

#ifdef HAVE_STRING_H
#include	<string.h>
#endif

#define	DB_DEF	0x10
#define	DB_TEXT	0x20
#define	DB_CODE	0x40
#define	DB_TOK	0x41

extern unsigned int db_flag;
extern int archived;

/* MX control indicators
 */
#define MARK		'@'

/* maximum number of macro arguments, arbitrarily set */
#define M_ARGS	30

typedef enum {
	Nop = 0,
	Index0, Index1, Index2, Index3, Index4, Index5, Index6, Index7, Index8, Index9,
	Bfile, Efile, Ofile, Mxmacro, Ifdef, Ifndef, Endif,
	Title, Author, Version, Date, InHide, OutHide, Comment,
	Module, Section, Subsection, Paragraph, Qcode, Continue,
	Pspec, Pimpl, Cdef, Csrc, ODLspec, SQL,
	OQLspec, Cyacc, Clex, Prolog, Haskell, MALcode,
	Qnap, HTML, Java,
	ProC, Shell, fGrammar, Macro, XML, DTD, XSL, Config,
	CCyacc, CClex
} CmdCode;


typedef struct {
	char *cmd;
	CmdCode code;
	char *ext;
} Directive;

extern char *inputdir;
extern char *outputdir;
extern Directive str2dir[];

/* CtlDir (can be any where)
 */

#define CTLSTR		"$#%!`[:{}|()"
#define MACROSTR        "[:?!"
#define T_NONE		'\0'
#define T_TEX		'$'
#define T_CODE		'#'
#define T_ITALIC	'%'
#define T_BOLD		'!'
#define T_INDEX		'`'
#define T_SGML		'['
#define T_REFERENCE	':'
#define T_POSCOND	'?'
#define T_NEGCOND	'!'


#define HIDESTR		"{|}()"
#define T_BEGHIDE	'{'
#define T_HIDETEXT	'|'
#define T_ENDHIDE	'}'
#define T_BEGARCHIVE	'('
#define T_ENDARCHIVE	')'

/*
extern	char *		strchr();
*/
#define	CmdDir(c)	(strchr(CMDSTR, c) != 0)
#define	CtlDir(c)	(strchr(CTLSTR, c) != 0)
#define CallDir(c)      (strchr(MACROSTR, c)!= 0)
#define	HideDir(c)	(strchr(HIDESTR, c) != 0)

/* 
 * MX layout types
 */

#define M_CODE	0x00ff

#define M_TEXT	0xf000
#define	M_DRAFT	0x1000
#define	M_BOOK	0x2000

#define	M_TEXI	0xf0000

#define TEXIMODE (textmode==M_TEXI)

extern int mode;

/* 
 * Environments.
 */

#define	E_NONE	0x10
#define	E_TEXT	0x20
#define	E_CODE	0x40
#define	E_DEF	0x41
#define	E_SRC	0x42
#define	E_CMD	0x80


/* Mx Currents
 */
extern int mx_err;
extern char *mx_file;
extern int mx_line;
extern int mx_chp;
extern int mx_mod;
extern int mx_sec;
extern int mx_out;

/* Mx Options
 */
extern int opt_hide;
extern char *opt_code;		/* extract code of interest only */
extern int textmode;		/* either T_TEX or T_MS */
extern int bodymode;		/* either 0= all 1= for inclusion */
extern int noline;
extern int notouch;

#define	NO_HIDE	-1

/* MX Files
 */


extern FILE *ofile;
extern FILE *ofile_body;
extern FILE *ofile_index;
extern FILE *ifile;

#define	M_FILES	128
struct file_s {
	char *f_name;
	char *f_tmp;
	char *f_str;
	int f_mode;
};
typedef struct file_s File;

extern char filename[200];	/* assume that no errors will occur */
extern char filename_index[200];
extern char filename_body[200];

/* MX Def
 */
#define M_DEFS	16384
#define	M_BLK	655360
#define	M_CMD	2048
#define	M_STR	1024

typedef
    struct {
	CmdCode d_dir;
	char *d_cmd;
	char *d_blk;
	int d_mod;
	int d_sec;
	char *d_cms;
	char *d_file;
	int d_line;
} Def;

extern Def *defs;
extern int ndef;

/* MX Tok
 */
typedef struct {
	char t_dir;
	char *t_str;
	char t_ext;
	char *t_nxt;
	char t_chr;
} Tok;
