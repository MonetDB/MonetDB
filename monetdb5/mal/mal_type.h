/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0.  If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 *
 * Copyright 1997 - July 2008 CWI, August 2008 - 2018 MonetDB B.V.
 */

#ifndef MAL_TYPE_H
#define MAL_TYPE_H
#include "mal.h"

/* #define DEBUG_MAL_TYPE 1 */

#define malVARG " malVARG"
#define TMPMARKER '_'
#define REFMARKER 'X'
#define REFMARKERC 'C'

#define newBatType(T)  (1<<16 |  (T & 0377) )
#define getBatType(X)  ((X) & 0377 )
#define isaBatType(X)   ((1<<16) & (X) && (X)!= TYPE_any)

#define isAnyExpression(X) ((X) >> 17)
#define isPolymorphic(X) (isAnyExpression(X) || (X)== TYPE_any)

#define setTypeIndex(X,I) X |= ((I & 017)<<18);
#define getTypeIndex(X)  (((X)>>18) & 017)

#define isPolyType(X) (isAnyExpression(X) && getTypeIndex(X)>0)
/*
 * The symbol/instruction kinds are introduced here instead of reusing the defines
 * derived from the parser to avoid a loop in the define-structure.
 */

#define RAISEsymbol     21	/* flow of control modifiers */
#define CATCHsymbol     22
#define RETURNsymbol    23
#define BARRIERsymbol   24
#define REDOsymbol      25
#define LEAVEsymbol     26
#define YIELDsymbol     27
#define EXITsymbol      29

#define ASSIGNsymbol    40	/* interpreter entry points */
#define ENDsymbol       41
#define NOOPsymbol      43	/* no operation required */

#define COMMANDsymbol   61	/* these tokens should be the last group !! */
#define FUNCTIONsymbol  62	/* the designate the signature start */
#define FACTORYsymbol   63	/* the co-routine infrastructure */
#define PATTERNsymbol   64	/* the MAL self-reflection commands */

#define FCNcall     50		/* internal symbols */
#define FACcall     51
#define CMDcall     52
#define THRDcall    53
#define PATcall     54		/* pattern call */

#define REMsymbol     99	/* commentary to be retained */


mal_export str getTypeName(malType tpe);
mal_export str getTypeIdentifier(malType tpe);
mal_export int getAtomIndex(const char *nme, int len, int deftpe);
#define idcmp(n, m)	strcmp(n, m)
mal_export int isIdentifier(str s);
mal_export int findGDKtype(int type);	/* used in src/mal/mal_interpreter.c */

#endif /* MAL_TYPE_H */
