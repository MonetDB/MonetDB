/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0.  If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 *
 * Copyright 1997 - July 2008 CWI, August 2008 - 2018 MonetDB B.V.
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
mal_export int parseMAL(Client cntxt, Symbol curPrg, int skipcomments);

#endif /* _MAL_PARSER_H */

