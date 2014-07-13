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
 * Copyright August 2008-2014 MonetDB B.V.
 * All Rights Reserved.
 */

#ifndef _MAL_PARSER_H
#define _MAL_PARSER_H

#include "mal_import.h"

#define MAXERRORS 250

#define CURRENT(c) (c->fdin->buf + c->fdin->pos + c->yycur)
#define currChar(X) (*CURRENT(X))
#define peekChar(X) (*((X)->fdin->buf + (X)->fdin->pos + (X)->yycur+1))
#define nextChar(X) X->yycur++
#define prevChar(X) if(X->yycur) X->yycur--

mal_export void initParser(void);   /* needed in src/mal/mal.c */
mal_export int parseMAL(Client cntxt, Symbol curPrg);
mal_export void echoInput(Client cntxt);
mal_export void debugParser(int i);
mal_export str parseError(Client cntxt, str msg);
mal_export int idLength(Client cntxt);
mal_export int stringLength(Client cntxt);
mal_export int cstToken(Client cntxt, ValPtr val);
mal_export int charCst(Client cntxt, ValPtr val);
mal_export int operatorLength(Client cntxt);
mal_export str operatorCopy(Client cntxt, int length);
mal_export int MALkeyword(Client cntxt, str kw, int length);
mal_export int MALlookahead(Client cntxt, str kw, int length);
mal_export str lastline(Client cntxt);
mal_export ssize_t position(Client cntxt);

#endif /* _MAL_PARSER_H */

