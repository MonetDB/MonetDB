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

#ifndef MAL_TYPE_H
#define MAL_TYPE_H
#include "mal.h"

/* #define DEBUG_MAL_TYPE 1 */


typedef int malType;

#define malVARG " malVARG"
#define TMPMARKER '_'
#define REFMARKER 'X'

#define newBatType(H,T)  (1<<16 | (((H & 0377) <<8) | (T & 0377) ))
#define newColType(T)  (1<<25 | (((TYPE_void & 0377) <<8) | (T & 0377) ))
#define getHeadType(X)  ((X>>8) & 0377 )
#define getTailType(X)  ((X) & 0377 )
#define isaBatType(X)   ((1<<16) & (X) && (X)!= TYPE_any)

#define setAnyHeadIndex(X,I) X |= ((I & 017)<<22);
#define setAnyTailIndex(X,I) X |= ((I & 017)<<18);
#define isAnyExpression(X) ((X) >> 17)
#define isPolymorphic(X) (((X) >> 17) || (X)== TYPE_any)
#define isPolyType(X) (isAnyExpression(X) && (getHeadIndex(X)>0 ||getTailIndex(X)>0))

#define getHeadIndex(X)  (((X)>>22) & 017)
#define getTailIndex(X)  (((X)>>18) & 017)
/*
 * @-
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
mal_export int getTypeIndex(str nme, int len, int deftpe);
mal_export malType reverseBatType(malType v);
mal_export malType malAnyBatType(malType t1, malType t2);
mal_export int idcmp(str n, str m);
mal_export str newTmpName(char tag, int i);
mal_export int isTmpName(str n);
mal_export int isTypeName(str n);
mal_export int isIdentifier(str s);
mal_export int findGDKtype(int type);	/* used in src/mal/mal_interpreter.c */
mal_export int isAmbiguousType(int type);

#endif /* MAL_TYPE_H */
