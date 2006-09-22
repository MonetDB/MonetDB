%option noyywrap
%option never-interactive
%option prefix="pftijah_tokenize"
%option nounput
%{
#include <pf_config.h>

#include "serialize_pftijah.h"

static char *c, *e;

/* This hack is to prevent problems with flex 2.5.33 and lower on Debian and
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

int useFlexScanner(char* buf, int len, struct tijahContextStruct* tjCtx) {
  YY_BUFFER_STATE myBuf = yy_scan_bytes(buf, len);
  while (pftijah_tokenizelex()) {
      if ( !handleTijahTerm(tjCtx, pftijah_tokenizetext) )
          return 0;
  }
  yy_delete_buffer(myBuf);
  return 1;
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

char* flexScanOneTerm(char* buf, int len) {
  char *res;
  char resBUFF[256];

  YY_BUFFER_STATE myBuf = yy_scan_bytes(buf, len);
  if ( pftijah_tokenizelex() ) {
      res = &resBUFF[0];
      strncpy(res,pftijah_tokenizetext,256);
  } else
      res = NULL;
  yy_delete_buffer(myBuf);
  return res;
}
