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

#ifndef _MAL_STACK_H_
#define _MAL_STACK_H_
#include "mal.h"

#define stackSize(CNT) (sizeof(ValRecord)*(CNT) + sizeof(MalStack))
#define newStack(S,CNT) S= (MalStkPtr) GDKzalloc(stackSize(CNT));\
		(S)->stksize=CNT;


mal_export MalStkPtr newGlobalStack(int size);
mal_export MalStkPtr reallocStack(MalStkPtr s, int cnt);
mal_export MalStkPtr reallocGlobalStack(MalStkPtr s, int cnt);
mal_export void freeStack(MalStkPtr stk);
mal_export void clearStack(MalStkPtr s);

#define VARfreeze(X)    if(X){X->frozen=TRUE;}
#define VARfixate(X)    if(X){X->constant=TRUE;}

#define getStkRecord(S,P,I) &(S)->stk[(P)->argv[I]]
#define getStkValue(S,P,I)  ( getStkType(S,P,I)== TYPE_str? \
					getStkRecord(S,P,I)->val.sval :\
					getStkRecord(S,P,I)->val.pval )
#define getStkType(S,P,I)   (S)->stk[(P)->argv[I]].vtype
#define setStkType(S,P,I,T) (S)->stk[(P)->argv[I]].vtype = T
#endif /* _MAL_STACK_H_ */
