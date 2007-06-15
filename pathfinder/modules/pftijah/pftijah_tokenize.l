%option noyywrap
%option never-interactive
%option prefix="pftijah_tokenize"
%option nounput
%{
/**
 * Copyright Notice:
 * -----------------
 *
 * The contents of this file are subject to the PfTijah Public License
 * Version 1.1 (the "License"); you may not use this file except in
 * compliance with the License. You may obtain a copy of the License at
 * http://dbappl.cs.utwente.nl/Legal/PfTijah-1.1.html
 * 
 * Software distributed under the License is distributed on an "AS IS"
 * basis, WITHOUT WARRANTY OF ANY KIND, either express or implied. See the
 * License for the specific language governing rights and limitations
 * under the License.
 * 
 * The Original Code is the PfTijah system.
 * 
 * The Initial Developer of the Original Code is the "University of Twente".
 * Portions created by the "University of Twente" are 
 * Copyright (C) 2006-2007 "University of Twente".
 * All Rights Reserved.
 *
 * Author(s): Vojkan Mihajlovic
 *	      Jan Flokstra
 *            Henning Rode
 *            Roel van Os
 *
 */

#include <pf_config.h>

#include "serialize_pftijah.h"
#include <ctype.h>
#include <string.h>

static char *c, *e;

/* This fix is to prevent problems with flex 2.5.33 and lower on Debian and
 * Gentoo systems. When flex.2.5.4 and higher is obligatory this define
 * may be removed
 */
#ifndef YY_INPUT
#define YY_INPUT(buf,result,max_size) \
	if ( ((result = fread( buf, 1, max_size, yyin )) == 0) \
		  && ferror( yyin ) ) \
		YY_FATAL_ERROR( "input in flex scanner failed" );
#endif

%}

/* XML Character Classes (http://www.w3.org/TR/REC-xml/#CharClasses) */
/* 8 bit characters from the Letter class
   see http://www.w3.org/TR/REC-xml/#NT-Letter */
Capital                             [\x41-\x5a\xc0-\xd6\xd8-\xde]
Small_Letter                        [\x61-\x7a\xdf-\xf6\xf8-\xff]
Letter                              [\x41-\x5a\x61-\x7a\xc0-\xd6\xd8-\xf6\xf8-\xff]

/* restrictive Words
   Word                                ({Capital}{Small_Letter}+)|({Small_Letter}{Small_Letter}+) */

/* 8 bit characters from the Digit class
   see http://www.w3.org/TR/REC-xml/#NT-Digit */
Digit                               [\x30-\x39]
Digits                              {Digit}+
AlphaNum			    [\x30-\x39\x41-\x5a\x61-\x7a\xc0-\xd6\xd8-\xf6\xf8-\xff]
NonAlphaNum			    [^\x30-\x39\x41-\x5a\x61-\x7a\xc0-\xd6\xd8-\xf6\xf8-\xff]

/* most relaxed condition for words, parsing almost everything expect whitespace */
Word                                {AlphaNum}{2,}

WhiteSpaceChar                      [\x20\x09\x0d\x0a]
S                                   {WhiteSpaceChar}+

%%

[&]{Letter}+[;]                     { /* zap symbols */ }
[&][#]{Digits}[;]                   { /* zap symbols */ } 
{S}	                            { /* zap white space */ }

{Word}                              { /* case WORD */ 
                                      for (c = pftijah_tokenizetext; *c != '\0'; c++) 
                                        *c = tolower(*c); 
                                      return 1;
				    }

{Letter}{1,2}[-]{Word}              { /* case e-mail, e-government, on-line 
                                         problem: make-up */ 
                                      /* strip hyphen */
                                      e = pftijah_tokenizetext;
                                      for (c = pftijah_tokenizetext; *c != '\0'; c++) 
                                        if (*c != '-') 
                                          *(e++) = tolower(*c);
                                      *e = '\0';
                                      return 1;
				    }

{Word}[\']{Letter}{1,2}             { /* case CONTRACTION */ 
                                      for (c = pftijah_tokenizetext; *c != '\0'; c++) 
                                        *c = tolower(*c); 
                                      return 1;
				    }

{Capital}[\.]({Capital}[\.])+       { /* case ACRONYM */ 
                                      e = pftijah_tokenizetext;
                                      for (c = pftijah_tokenizetext; *c != '\0'; c++) 
                                        if (*c != '.') 
                                          *(e++) = tolower(*c);
                                      *e = '\0';
                                      return 1;
                                    }

.                                   { /* zap unknown */ }

%%

#if 0
int tokenize_flex(char* buf, struct tijahContextStruct* tjCtx) {
  /* UPDATE: this delivers very strange testset results and should not be
   * used I think.
   */
  /* This is an optimized version of the flex scanner which does not copy the
   * input buffer. The only strange thing about this interface is that it
   * requires 2 YY_END_OF_BUFFER_CHAR (eg. 0) at the end of the buffer. The
   * size of the buffer is inclusive the 2 0's.
   * The last zero is toggled with its original value to prevent corruption
   * of memory management tables. This was for me the only way to prevent
   * copying here.
   */
  int len = strlen(buf);
  char remember = buf[len+1];
  buf[len+1] = YY_END_OF_BUFFER_CHAR;
  YY_BUFFER_STATE myBuf = yy_scan_buffer(buf, len+2);

  if ( !myBuf ) {
      stream_printf(GDKout,"# tokenize_flex: unable to get setup non-copy buffer.");
      return 0;
  }
  while ( pftijah_tokenizelex() ) {
      /* stream_printf(GDKout,"# scan(%s).\n",pftijah_tokenizetext); */
      if ( !handleTijahTerm(tjCtx, pftijah_tokenizetext) )
          return 0;
  }
  yy_delete_buffer(myBuf);
  buf[len+1] = remember;
  return 1;
}
#endif

int tokenize_flex(char* buf, struct tijahContextStruct* tjCtx) {
  // the original
  int len = strlen(buf);
  YY_BUFFER_STATE myBuf = yy_scan_bytes(buf, len);
  while (pftijah_tokenizelex()) {
      if ( !handleTijahTerm(tjCtx, pftijah_tokenizetext) )
          return 0;
  }
  yy_delete_buffer(myBuf);
  return 1;
}

/*
 * Implementation of the fast tokenizer in "C"
 *
 */

/* */
/* #define  TFDEBUG 1 */

#ifdef TFDEBUG
#define EMIT \
    tmpc=*s; *s=0; \
    if (TFDEBUG) stream_printf(GDKout,"#[%s]\n",base); \
    if (!handleTijahTerm(tjctx,base)) \
        return 0; \
    *s=tmpc
#else
#define EMIT \
    tmpc=*s; *s=0; \
    if (!handleTijahTerm(tjctx,base)) \
        return 0; \
    *s=tmpc
#endif

#define TOLOWER(C) if (isupper(C)) C=tolower(C);
/* */

char* scan_ENTITY(char* s) {
    if ( *s++ == '&' ) {
        if ( *s == '#' ) {
            // scan [&][#]{Digits}[;]
	    if (isdigit(*++s) ) {
	    	s++;
		while( isdigit(*s) ) s++;
		if ( *s == ';' )
		    return ++s; // return one past the entity
	    }
	} else if (isalpha(*s) ) {
	    // scan [&]{Letter}+[;] 
	    s++;
	    while( isalpha(*s) ) s++;
	    if ( *s == ';' )
	        return ++s; // return one past the entity
	}
    }
    return NULL;
}


int tokenize_fast(char* input, struct tijahContextStruct* tjctx)
{
    /* the fast function. This function is in the pftijah context with lots
     * of small strings to tokenize many times faster as the flex and the 
     * strtok() methods which seem to have a rather larger overhead.
     * In the future I will implement contraction and other goodies but there's
     * no time for that yet.
     */
    register char* s = input;
    register char tmpc, *tmpp;

#ifdef TFDEBUG
    if (TFDEBUG) stream_printf(GDKout,"#[TOKENIZE-FAST:%s\n",s);
#endif
    while ( 1 ) {
      while ( isspace( *s ) ) s++;
      if ( *s ) {
	  char* base = s;
	  if ( isalnum(*s) ) {
	      if ( isdigit(*s) ) {
	          while ( isdigit(*++s) ) ;
		  EMIT;
	      } else {
	          TOLOWER(*s);
		  if ( isalnum(*++s) ) { // the length 2 check
		     TOLOWER(*s);
	             while ( isalnum(*++s) ) TOLOWER(*s);
		     EMIT;
		  }
	      }
	  } else if ( *s == '&' ) {
	      if ( (tmpp = scan_ENTITY(s)) ) {
	          s = tmpp; // emit entity till here
		  EMIT;
	      } else {
	          s++; // no entity, just skip
	      }
	  } else {
#ifdef TFDEBUG
	      if (TFDEBUG) stream_printf(GDKout,"#[SKIPPING:%c]\n",*s);
#endif
	      s++;
	  }
      } else 
          return 1;
    }
}

char* tijah_tokenize_string(char* buf, int len, char* outbuf) {
  int cnt = 0;
  YY_BUFFER_STATE myBuf = yy_scan_bytes(buf, len);
  while (pftijah_tokenizelex()) {
      if ( cnt++ )
          strcat(outbuf," ");
      strcat(outbuf,pftijah_tokenizetext);
  }
  yy_delete_buffer(myBuf);
  return outbuf;
}

char* flexScanOneTerm(char* buf) {
  char *res;
  char resBUFF[256];
  int len = strlen(buf);

  YY_BUFFER_STATE myBuf = yy_scan_bytes(buf, len);
  if ( pftijah_tokenizelex() ) {
      res = &resBUFF[0];
      strncpy(res,pftijah_tokenizetext,256);
  } else
      res = NULL;
  yy_delete_buffer(myBuf);
  return res;
}
