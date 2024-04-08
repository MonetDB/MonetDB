/*
 * SPDX-License-Identifier: MPL-2.0
 *
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0.  If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 *
 * Copyright 2024 MonetDB Foundation;
 * Copyright August 2008 - 2023 MonetDB B.V.;
 * Copyright 1997 - July 2008 CWI.
 */

#ifndef MAL_TYPE_H
#define MAL_TYPE_H
#include "mal.h"

/* #define DEBUG_MAL_TYPE 1 */

#define TMPMARKER '_'
#define REFMARKER 'X'
#define REFMARKERC 'C'

#define newBatType(T)  (1<<16 |  (T & 0377) )
#define getBatType(X)  ((X) & 0377 )
#define isaBatType(X)   (((1<<16) & (X)) != 0)

#define isAnyExpression(X) ((X) >> 17)
#define isPolymorphic(X) (isAnyExpression(X))

#define setTypeIndex(X,I) X |= ((I & 7)<<18);
#define getTypeIndex(X)  (((X)>>18) & 7)

#define setOptBat(X)   X |= (1<<9)
#define getOptBat(X)   (((X)>>9) & 1)

#define isPolyType(X) (isAnyExpression(X) && getTypeIndex(X)>0)
/*
 * The symbol/instruction kinds are introduced here instead of reusing the defines
 * derived from the parser to avoid a loop in the define-structure.
 */

#define RAISEsymbol     21		/* flow of control modifiers */
#define CATCHsymbol     22
#define RETURNsymbol    23
#define BARRIERsymbol   24
#define REDOsymbol      25
#define LEAVEsymbol     26
#define EXITsymbol      27

#define ASSIGNsymbol    40		/* interpreter entry points */
#define ENDsymbol       41

#define COMMANDsymbol   61		/* these tokens should be the last group !! */
#define FUNCTIONsymbol  62		/* the designate the signature start */
#define PATTERNsymbol   63		/* the MAL self-reflection commands */

#define FCNcall     50			/* internal symbols */
#define CMDcall     51
#define PATcall     52			/* pattern call */

#define REMsymbol     99		/* commentary to be retained */

mal_export str getTypeName(malType tpe);
mal_export str getTypeIdentifier(malType tpe);
mal_export int getAtomIndex(const char *nme, size_t len, int deftpe);
#define idcmp(n, m)	strcmp(n, m)
mal_export int isIdentifier(str s);
mal_export int findGDKtype(int type);	/* used in src/mal/mal_interpreter.c */

#endif /* MAL_TYPE_H */
