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
#include <str.h>
#ifdef HAVE_MALLOC_H
#include <malloc.h>
#endif

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

#define ECHO	/* empty -- don't echo anything (we shouldn't get there anyway */

%}

/*
 * XQuery 1.0 syntactic constructs
 *
 * lex/flex currently only supports 8 bit charsets. The character classes
 * are thus limited to the lower 8 bits.
 */

/* XML Character Classes (http://www.w3.org/TR/REC-xml/#CharClasses) */

/* see http://www.w3.org/TR/REC-xml/#NT-Letter */
Letter                           {BaseChar}|{Ideographic}

/* see http://www.w3.org/TR/REC-xml/#NT-BaseChar */
OneByteBaseChar                  [\x41-\x5a\x61-\x7a]
TwoByteBaseChar                  \xc3[\x80-\x96\x98-\xb6\xb8-\xbf]|\xc4[\x80-\xb1\xb4-\xbe]|\xc5[\x81-\x88\x8a-\xbe]|\xc6[\x80-\xbf]|\xc7[\x80-\x83\x8d-\xb0\xb4\xb5\xba-\xbf]|\xc8[\x80-\x97]|\xc9[\x90-\xbf]|\xca[\x80-\xa8\xbb-\xbf]|\xcb[\x80\x81]|\xce[\x86\x88-\x8a\x8c\x8e-\xa1\xa3-\xbf]|\xcf[\x80-\x8e\x90-\x96\x9a\x9c\x9e\xa0\xa2-\xb3]|\xd0[\x81-\x8c\x8e-\xbf]|\xd1[\x80-\x8f\x91-\x9c\x9e-\xbf]|\xd2[\x80\x81\x90-\xbf]|\xd3[\x80-\x84\x87\x88\x8b\x8c\x90-\xab\xae-\xb5\xb8\xb9]|\xd4[\xb1-\xbf]|\xd5[\x80-\x96\x99\xa1-\xbf]|\xd6[\x80-\x86]|\xd7[\x90-\xaa\xb0-\xb2]|\xd8[\xa1-\xba]|\xd9[\x81-\x8a\xb1-\xbf]|\xda[\x80-\xb7\xba-\xbe]|\xdb[\x80-\x8e\x90-\x93\x95\xa5\xa6]
ThreeByteBaseChar                \xe0(\xa4[\x85-\xb9\xbd]|\xa5[\x98-\xa1]|\xa6[\x85-\x8c\x8f-\x90\x93-\xa8\xaa-\xb0\xb2\xb6-\xb9]|\xa7[\x9c\x9d\x9f-\xa1\xb0\xb1]|\xa8[\x85-\x8a\x8f\x90\x93-\xa8\xaa-\xb0\xb2\xb3\xb5\xb6\xb8\xb9]|\xa9[\x99-\x9c\x9e\xb2-\xb4]|\xaa[\x85-\x8b\x8d\x8f-\x91\x93-\xa8\xaa-\xb0\xb2\xb3\xb5-\xb9\xbd]|\xab\xa0|\xac[\x85-\x8c\x8f\90\x93-\xa8\xaa-\xb0\xb2\xb3\xb6-\xb9\xbd]|\xad[\x9c\x9d\x9f-\xa1]|\xae[\x85-\x8a\x8e-\x90\x92-\x95\x99-\x9a\x9c\x9e\x9f\xa3\xa4\xa8-\xaa\xae-\xb5\xb7-\xb9]|\xb0[\x85-\x8c\x8e\x90\x92-\xa8\xaa-\xb3\xb5-\xb9]|\xb1[\xa0\xa1]|\xb2[\x85-\x8c\x8e-\x90\x92-\xa8\xaa-\xb3\xb5-\xb9]|\xb3[\x9e\xa0\xa1]|\xb4[\x85-\x8c\x8e-\x90\x92-\xa8\xaa-\xb9]|\xb5[\xa0\xa1]|\xb8[\x81-\xae\xb0\xb2\xb3]|\xb9[\x80-\x85]|\xba[\x81\x82\x84\x87\x88\x8a\x8d\x94-\x97\x99-\x9f\xa1-\xa3\xa5\xa7\xaa\xab\xad-\xae\xb0\xb2\xb3\xbd]|\xbb[\x80-\x84]|\xbd[\x80-\x87\x89-\xa9])|\xe1(\x82[\xa0-\xbf]|\x83[\x80-\x85\x90-\xb6]|\x84[\x80\x82\x83\x85-\x87\x89\x8b\x8c\x8e-\x92\xbc\xbe]|\x85[\x80\x8c\x8e\x90\x94\x95\x99\x9f-\xa1\xa3\xa5\xa7\xa9\xad\xae\xb2\xb3\xb5]|\x86[\x9e\xa8\xab\xae\xaf\xb7\xb8\xba\xbc-\xbf]|\x87[\x80-\x82\xab\xb0\xb9]|\xb8[\80-\xbf]|\xb9[\x80-\xbf]|\xba[\x80-\x9b\xa0-\xbf]|\xbb[\x80-\xb9]|\xbc[\x80-\x95\x98-\x9d\xa0-\xbf]|\xbd[\x80-\x85\x88-\x8d\x90-\x97\x99\x9b\x9d\x9f-\xbd]|\xbe[\x80-\xb4\xb6-\xbc\xbe]|\xbf[\x82-\x84\x86-\x8c\x90-\x93\x96-\x9b\xa0-\xac\xb2\xb4\xb6-\xbc])|\xe2(\x84[\xa6\xaa\xab\xae]|\x86[\x80-\x82])|\xe3(\x81[\x81-\xbf]|\x82[\x80-\x94\xa1-\xbf]|\x83[\x80-\xba]|\x84[\x85-\xac])|\xea[\xb0-\xbf][\x80-\xbf]|[\xeb\xec][\x80-\xbf][\x80-\xbf]|\xed([\x80-\x9d][\x80-\xbf]|\x9e[\x80-\xa3])

BaseChar                         {OneByteBaseChar}|{TwoByteBaseChar}|{ThreeByteBaseChar}

/* see http://www.w3.org/TR/REC-xml/#NT-Ideographic */
Ideographic                      \xe4[\xb8-\xbf][\x80-\xbf]|[\xe5-\xe8][\x80-\xbf][\x80-\xbf]|\xe9[\x80-\xbd][\x80-\xbf]|\xe9\xbe[\x80-\xa5]|\xe3\x80[\x87\xa1-\xa9]

/* see http://www.w3.org/TR/REC-xml/#NT-CombiningChar */
CombiningChar                    \xcc[\x80-\xbf]|\xcd[\x80-\x85\xa0\xa1]|\xd2[\x83-\x86]|\xd6[\x91-\xa1\xa3-\xb9\xbb-\xbd\xbf]|\xd7[\x81\x82\x84]|\xd9[\x8b-\x92\xb0]|\xdb[\x96-\xa4\xa7\xa8\xaa-\xad]|\xe0(\xa4[\x81-\x83\xbc\xbe\xbf]|\xa5[\x80-\x8d\x91-\x94\xa2\xa3]|\xa6[\x81-\x83\xbc\xbe\xbf]|\xa7[\x80-\x84\x87\x88\x8b-\x8d\x97\xa2\xa3]|\xa8[\x82\xbc\xbe\xbf]|\xa9[\x80-\x82\x87\x88\x8b-\x8d\xb0\xb1]|\xaa[\x81-\x83\xbc\xbe\xbf]|\xab[\x80-\x85\x87-\x89\x8b-\x8d]|\xac[\x81-\x83\xbc\xbe\xbf]|\xad[\x80-\x83\x87\x88\x8b-\x8d\x96\x97]|\xae[\x82\x83\xbe\xbf]|\xaf[\x80-\x82\x86-\x88\x8a-\x8d\x97]|\xb0[\x81-\x83\xbe\xbf]|\xb1[\x80-\x84\x86-\x88\x8a-\x8d\x95\x96]|\xb2[\x82\x83\xbe\xbf]|\xb3[\80-\x84\x86-\x88\x8a-\x8d\x95\x96]|\xb4[\x82\x83\xbe\xbf]|\xb5[\x80-\x83\x86-\x88\x8a-\x8d\x97]|\xb8[\xb1\xb4-\xba]|\xb9[\x87-\x8e]|\xba[\xb1\xb4-\xb9\xbb\xbc]|\xbb[\x88-\x8d]|\xbc[\x98\x99\xb5\xb7\xb9\xbe\xbf]|\xbd[\xb1-\xbf]|\xbe[\x80-\x84\x86-\x8b\x90-\x95\x97\x99-\xad\xb1-\xb7\xb9])|\xe2(\x83[\x90-\x9c\xa1])|\xe3(\x80[\xaa-\xaf]|\x82[\x99\x9a])

/* see http://www.w3.org/TR/REC-xml/#NT-Extender */
Extender                         \xc2\xb7|\xcb[\x90\x91]|\xce\x87|\xd9\x80|\e0[\xb9\xbb]\x86|\xe3\x80[\x85\xb1-\xb5]|\xe3\x82[\x9d\x9e]|\xe3\x83[\xbc-\xbe]

/* Digit according to the XQuery definition */
Digit                            [\x30-\x39]
Digits                           {Digit}+

/* XQuery WD [145] */
PredefinedEntityRef              "&"("lt"|"gt"|"amp"|"quot"|"apos")";"


/* XQuery WD [153] or http://www.w3.org/TR/REC-xml/#NT-CharRef */
CharRef                          ("&#"[0-9]+";")|("&#x"[0-9a-fA-F]+";")

/* XQuery WD [156]: Whitespace http://www.w3.org/TR/REC-xml/#NT-S */
S                                {WhiteSpaceChar}+
WhiteSpaceChar                   [\x20\x09\x0d\x0a]

AlphaNumChar			 {Letter}|{Digit}|{CharRef}

/* XML Character Classes (http://www.w3.org/TR/REC-xml/#CharClasses) */
/* 8 bit characters from the Letter class
   see http://www.w3.org/TR/REC-xml/#NT-Letter */
Capital                          [\x41-\x5a\xc0-\xd6\xd8-\xde]


/* most relaxed condition for words, parsing almost everything expect whitespace */
Word                             {AlphaNumChar}{2,}

AsciiAlphaNumChar		 {OneByteBaseChar}|{Digit}

AsciiWord			 {AsciiAlphaNumChar}{2,}

%%

{AsciiWord}			    { /* case simple WORD */ 
                                      for (e = pftijah_tokenizetext; *e != '\0'; e++) 
                                          *e = tolower(*e); 
				      e = pftijah_tokenizetext;
				      return 1;
 				    }

{S}	                            { /* zap white space */ }

{Word}                              { /* case WORD */
                                      strLower(&e, pftijah_tokenizetext);
				      return 1;
				    }

{AsciiAlphaNumChar}{1,2}[-]{AsciiWord} { /* case e-mail, e-government, on-line 
                                         problem: make-up */ 
                                        /* strip hyphen */
                                      e = pftijah_tokenizetext;
                                      for (c = pftijah_tokenizetext; *c != '\0'; c++) 
                                          if (*c != '-') 
                                            *(e++) = tolower(*c);
                                      *e = '\0';
				      e = pftijah_tokenizetext;
                                      return 1;
				    }

{AsciiWord}[\']{AsciiAlphaNumChar}{1,2} { /* case CONTRACTION */ 
                                        /* strip "'" */
                                      e = pftijah_tokenizetext;
                                      for (c = pftijah_tokenizetext; *c != '\0'; c++) 
                                          if (*c != '\'') 
                                            *(e++) = tolower(*c);
                                      *e = '\0';
				      e = pftijah_tokenizetext;
				      return 1;
				    }
{Word}[\']{AlphaNumChar}{1,2} 	    { /* case CONTRACTION */ 
                                      bit repeat = 1;
                                      strSubstitute(&c, pftijah_tokenizetext, "'", "", &repeat); 
				      strLower(&e, c);
				      GDKfree(c);
				      return 1;
				    }

{Capital}[\.]({Capital}[\.])+       { /* case ACRONYM */ 
                                      bit repeat = 1;
                                      strSubstitute(&c, pftijah_tokenizetext, ".", "", &repeat); 
				      strLower(&e, c);
				      GDKfree(c);
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
      if ( !handleTijahTerm(tjCtx, e) )
          return 0;
      if ( e != pftijah_tokenizetext )  {
      	GDKfree(e);
      }
  }
  yy_delete_buffer(myBuf);
  return 1;
}

BAT* tokenize2bat(char* buf) {
  BAT *res = BATnew(TYPE_void, TYPE_str, 0);
  int len = strlen(buf);
  YY_BUFFER_STATE myBuf = yy_scan_bytes(buf, len);
  while (pftijah_tokenizelex()) {
      str s= (str)e;
      if ( !BUNappend(res, s, 0) )
          return 0;
      if ( e != pftijah_tokenizetext )  {
      	GDKfree(e);
      }
  }
  yy_delete_buffer(myBuf);
  return res;
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
      strcat(outbuf,e);
      if ( e != pftijah_tokenizetext )  {
      	GDKfree(e);
      }
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
      strncpy(res,e,256);
      if ( e != pftijah_tokenizetext )  {
      	GDKfree(e);
      }
  } else
      res = NULL;
  yy_delete_buffer(myBuf);
  return res;
}
