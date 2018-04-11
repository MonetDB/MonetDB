/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0.  If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 *
 * Copyright 1997 - July 2008 CWI, August 2008 - 2018 MonetDB B.V.
 */

#ifndef _MAL_STACK_H_
#define _MAL_STACK_H_
#include "mal.h"

#define stackSize(CNT) (sizeof(ValRecord)*(CNT) + offsetof(MalStack, stk))

mal_export MalStkPtr newGlobalStack(int size);
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
