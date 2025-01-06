/*
 * SPDX-License-Identifier: MPL-2.0
 *
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0.  If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 *
 * Copyright 2024, 2025 MonetDB Foundation;
 * Copyright August 2008 - 2023 MonetDB B.V.;
 * Copyright 1997 - July 2008 CWI.
 */

#ifndef _MAL_PARSER_H
#define _MAL_PARSER_H

#include "mal_import.h"

#define CURRENT(c) (c->fdin->buf + c->fdin->pos + c->yycur)
#define currChar(X) (*CURRENT(X))
#define peekChar(X) (*((X)->fdin->buf + (X)->fdin->pos + (X)->yycur+1))
#define nextChar(X) X->yycur++
#define prevChar(X) if(X->yycur) X->yycur--

#ifdef LIBMONETDB5
#define MAXERRORS 250

extern void parseMAL(Client cntxt, Symbol curPrg, int skipcomments, int lines,
					 MALfcn address);
#endif

#endif /* _MAL_PARSER_H */
