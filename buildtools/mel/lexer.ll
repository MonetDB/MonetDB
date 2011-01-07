/*
 * The contents of this file are subject to the MonetDB Public License
 * Version 1.1 (the "License"); you may not use this file except in
 * compliance with the License. You may obtain a copy of the License at
 * http://monetdb.cwi.nl/Legal/MonetDBLicense-1.1.html
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

%option never-interactive yylineno
%{
#include <monetdb_config.h>
#include "mel.h"
#include "parser.h"
#include <stdlib.h>
#include <ctype.h>
#include <string.h>

#ifdef NATIVE_WIN32
/* The POSIX name for this item is deprecated. Instead, use the ISO
   C++ conformant name: _strdup. See online help for details. */
#define strdup _strdup
#endif

extern yystype yylval;

#ifdef FLEX_SCANNER
#if !defined(YY_FLEX_SUBMINOR_VERSION) || YY_FLEX_MAJOR_VERSION < 2 || YY_FLEX_MINOR_VERSION < 5 || YY_FLEX_SUBMINOR_VERSION < 5
/* after flex 2.5.4 flex uses yyget and yyset lineno functions, so for
   older flexs we define these here 
 */
#define yyget_lineno()		yylineno
#define yyset_lineno(ln)	yylineno = ln
#else
extern int yyget_lineno(void);
extern void yyset_lineno(int lineno);
#endif

#define YY_SKIP_YYWRAP
#else
extern int yyerrok;

#undef YYLMAX
#define YYLMAX	100000
#endif

#ifdef __MINGW32_VERSION
int
isatty(int fd)
{
	(void) fd;
	return 1;
}
#endif

void
check_lineno(char *line)
{
	int l;

	if (sscanf(line, "%d", &l) == 1) {
		char *p = strchr(line + 1, '"');

		if (p) {
			char *filename = (char *) strdup(++p);

			yyset_lineno(l);
			p = strchr(filename, '"');
			if (p)
				*p = 0;
			if (filename)
				fileinfo->FileName(filename);
		}
	}
}

/*
int
yywrap()
{
	return(1);
}
*/

#ifdef FLEX_SCANNER
void *
get_cur_buffer()
{
	return (void *) YY_CURRENT_BUFFER;
}

void
new_buffer(FILE *f)
{
	yy_switch_to_buffer(yy_create_buffer(f, YY_BUF_SIZE));
}

void
close_and_change(void *buf, FILE *)
{
	yy_delete_buffer(YY_CURRENT_BUFFER);
	yy_switch_to_buffer((yy_buffer_state *) buf);
}
#else

struct yy_buffer {
/*   	unsigned char yysbuf[YYLMAX];
	unsigned char *yysptr; */
	char yysbuf[YYLMAX];
	char *yysptr;
};

void *
get_cur_buffer()
{
	yy_buffer *b = new yy_buffer;

	memcpy(b->yysbuf, yysbuf, YYLMAX);
	b->yysptr = yysptr;
	return (void *) b;
}

void
new_buffer(FILE *f)
{
	yysptr = yysbuf;
	yyin = f;
}

void
close_and_change(void *buf, FILE *f)
{
	yy_buffer *b = (yy_buffer *) buf;

	fclose(yyin);
	memcpy(yysbuf, b->yysbuf, YYLMAX);
	yysptr = b->yysptr;
	yyin = f;
}

#endif

int script_nr = 0;
int script_len = 0;
char *script = NULL;

extern void script_start();
%}
%p 3400
%a 2300
%e 2000

YY_DIGIT    	[0-9]
YY_LETTER    	[_a-zA-Z]
YY_SPACE    	[\t\n ]
YY_SPACES    	[\t\n ]*
%%
(".iterator"|".ITERATOR") { return MEL_ITERATOR; 		  	}
(".module"|".MODULE")	  { return MEL_MODULE; 		     		}
(".USE"|".use")		  { return MEL_USE;				}
(".PRELUDE"|".prelude")	  { return MEL_PRELUDE; 			}
(".EPILOGUE"|".epilogue") { return MEL_EPILOGUE;			}
(".END"|".end")		  { return MEL_END; 	     			}
(".BUILTIN"|".builtin")	  { return MEL_BUILTIN; 			}
(".OBJECT"|".object")	  { return MEL_OBJECT; 				}
(".COMMAND"|".command")	  { return MEL_COMMAND; 			}
(".OPERATOR0"|".operator0") { return MEL_OPERATOR0; 			}
(".OPERATOR"|".operator") { return MEL_OPERATOR; 			}
(".OPERATOR1"|".operator1") { return MEL_OPERATOR1; 			}
(".ATOM"|".atom")	  { return MEL_ATOM; 	     			}
(".FIXED_ATOM"|".fixed_atom")	  { return MEL_FIXEDATOM; 		}
(".TOSTR"|".tostr")	  { yylval.i = OP_TOSTR; return MEL_ATOMOP;	}
(".FROMSTR"|".fromstr")	  { yylval.i = OP_FROMSTR; return MEL_ATOMOP; 	}
(".READ"|".read")	  { yylval.i = OP_READ; return MEL_ATOMOP;	}
(".WRITE"|".write")	  { yylval.i = OP_WRITE; return MEL_ATOMOP; 	}
(".COMP"|".comp")	  { yylval.i = OP_COMP; return MEL_ATOMOP; 	}
(".NEQUAL"|".nequal")	  { yylval.i = OP_NEQUAL; return MEL_ATOMOP; 	}
(".DEL"|".del")		  { yylval.i = OP_DEL; return MEL_ATOMOP; 	}
(".HASH"|".hash")	  { yylval.i = OP_HASH; return MEL_ATOMOP; 	}
(".NULL"|".null")	  { yylval.i = OP_NULL; return MEL_ATOMOP; 	}
(".CONVERT"|".CONVERT")	  { yylval.i = OP_CONVERT; return MEL_ATOMOP; 	}
(".HEAPCONVERT"|".heapconvert")	  { yylval.i = OP_HCONVERT; return MEL_ATOMOP; 	}
(".PUT"|".put")		  { yylval.i = OP_PUT; return MEL_ATOMOP; 	}
(".FIX"|".fix")		  { yylval.i = OP_FIX; return MEL_ATOMOP; 	}
(".UNFIX"|".unfix")	  { yylval.i = OP_UNFIX; return MEL_ATOMOP; 	}
(".LENGTH"|".length")	  { yylval.i = OP_LEN; return MEL_ATOMOP; 	}
(".HEAP"|".heap")	  { yylval.i = OP_HEAP; return MEL_ATOMOP; 	}
(".CHECK"|".check")	  { yylval.i = OP_CHECK; return MEL_ATOMOP; 	}

("ANY"|"any") 		  { return MEL_ANY;     			}
("BAT"|"bat")		  { return MEL_BAT;     			}
("VOID"|"void")		  { return MEL_VOID;   				}
("BIT"|"bit")		  { yylval.i = TYPE_BIT; return MEL_TYPE;	}
("CHR"|"chr")		  { yylval.i = TYPE_CHR; return MEL_TYPE;	}
("BTE"|"bte")		  { yylval.i = TYPE_BTE; return MEL_TYPE;	}
("SHT"|"sht")		  { yylval.i = TYPE_SHT; return MEL_TYPE;	}
("INT"|"int")		  { yylval.i = TYPE_INT; return MEL_TYPE;    	}
("PTR"|"ptr") 		  { yylval.i = TYPE_PTR; return MEL_TYPE;	}
("OID"|"oid")		  { yylval.i = TYPE_OID; return MEL_TYPE;	}
("WRD"|"wrd")		  { yylval.i = TYPE_WRD; return MEL_TYPE;	}
("FLT"|"flt")		  { yylval.i = TYPE_FLT; return MEL_TYPE;	}
("DBL"|"dbl")		  { yylval.i = TYPE_DBL; return MEL_TYPE;	}
("LNG"|"lng")		  { yylval.i = TYPE_LNG; return MEL_TYPE; 	}
("STR"|"str")		  { yylval.i = TYPE_STR; return MEL_TYPE;	}
("..."|"..")		  { return MEL_VARARGS;				}
"::"			  { return MEL_SEP;				}
"="			  { return '='; 				}
":"			  { return ':';					}
";"			  { return ';'; 				}
","			  { return ','; 				}
"("			  { return '('; 				}
")"			  { return ')'; 				}
"["			  { return '['; 				}
"]"			  { return ']'; 				}
"{"			  { return '{'; 				}
"}"			  { return '}'; 				}
\"[^"]*         	  { if (yytext[yyleng-1] == '\\') {
                      		yymore();  /* " can be escaped */
                	  } else {
				char *s; int i=1;
				yyinput(); /* read '"' */
				yytext[yyleng] = 0;
				for(s = yytext+1; *s; s++,i++)
				    if (*s=='\t' || *s=='\n') i++;
				s = (char*)malloc(i);
				yylval.s = (char*) s;
				for(i = 1; yytext[i]; i++)
				    if (yytext[i]=='\t') {
				        *s++ = '\\'; *s++='t';
				    } else if (yytext[i]=='\n') {
				        *s++ = '\\'; *s++='n';
				    } else *s++ = yytext[i];
				*s = 0;
				return MEL_STRING;
			  }						}
{YY_LETTER}({YY_LETTER}|{YY_DIGIT})* {
			yytext[yyleng] = 0; yylval.s = strdup(yytext);
			if (table->find(MEL_USE, yylval.s))
				return MEL_USED;
			return MEL_IDENTIFIER; 	     			}
{YY_DIGIT}+	{	yytext[yyleng] = 0; yylval.i = (int) atoi(yytext);
			return MEL_NUMBER;	 		     	}
"#line"		{     char line[1024], *p=line;
			while ((*p = yyinput()) != '\n') p++;
			check_lineno(line);			      	}
"#"		{     while (yyinput() != '\n');			}
{YY_SPACES}		;
.		{ 	fprintf(stderr, "discarding '%c'\n", yytext[0]);}
%%
int str = 0, esc = 0, chr = 0;

void
mel_yyerror(const char *err)
{
	if (progname) {
		cerr << progname << ": ";
	}
	if (fileinfo->FileName()) {
		cerr << "\"" << fileinfo->FileName() << "\", ";
	}
	if (yyget_lineno() > 0) {
		cerr << "line " << (yyget_lineno()) << ", ";
	}
	cerr << err;
	if (*yytext) {
		int i;

		for (i = 0; i < 20; i++) {
			if (!yytext[i] || yytext[i] == '\n') {
				break;
			}
		}
		if (i) {
			cerr << " at \"" << i << "\" of " << yytext;
		}
	} else {
		cerr << " at end-of-file";
	}
	cerr << ".\n";
	parse_error++;
}

int
melget_lineno( void )
{
	return yyget_lineno();
}

void
melset_lineno( int lineno )
{
	yyset_lineno(lineno);
}

void
script_start()
{
	script_nr = 0;
	script_len = BUFSIZ;
	script = new char[script_len];
	str = esc = chr = 0;

	script[0] = '\0';
}

int
char_concat(char c)
{
	if (script_nr + 10 > script_len) {
		script_len *= 2;
		char *new_script = new char[script_len];

		strncpy(new_script, script, script_nr);
		delete(script);
		script = new_script;
	}
	if (str == 0 && chr == 0 && esc == 0 && c == '#') {
		return 0;
	} else if (c == '\\') {
		script[script_nr++] = '\\';
		script[script_nr++] = '\\';
		if (esc == 0 && str == 0 && chr == 0) {
			esc = 1;
		}
	} else {
		if (c == '\t') {
			script[script_nr++] = '\\';
			script[script_nr++] = 't';
		} else if (c == '\'') {
			script[script_nr++] = '\'';
			if (esc == 0 && str == 0)
				chr = chr ? 0 : 4;
		} else if (c == '"') {
			script[script_nr++] = '\\';
			script[script_nr++] = '"';
			if (esc == 0 && chr == 0)
				str = str ? 0 : 1;
		} else {
			script[script_nr++] = c;
		}
		esc = 0;
	}
	if (chr)
		chr --;

	return 1;
}

const char *
get_signature(char *fcn)
{
	int c, echo = 1;

	script_start();
	while (*fcn) {
		char_concat(*fcn++);
	}
	while ((c = yyinput()) && c != '=') {
		if (c == '\n')
			echo = 1;
		if (echo)
			echo = char_concat(c);
	}
	script[script_nr] = 0;
	return script;
}
