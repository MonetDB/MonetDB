/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0.  If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 *
 * Copyright 1997 - July 2008 CWI, August 2008 - 2017 MonetDB B.V.
 */

/*
 * Martin Kersten
 * The partition function breaks a target BAT into value disjoint partitions
 * using a straight forward hash
 */
#include "monetdb_config.h"
#include "partition.h"

#define MAXPART 256
#define hashpartition(TYPE)                         		\
    do { p= BATcount(b);                               		\
	 bi = bat_iterator(b);					\
        for (r=0; r < p; r++) {                    		\
            TYPE *v = (TYPE *) BUNtloc(bi, r);          	\
	    TYPE c =  mix_##TYPE((*v));      			\
	    if( BUNappend(bn[(int) (c % pieces)], (void*) v, FALSE) != GDK_SUCCEED){ \
		    msg = createException(MAL,"partition.hash","Storage failed");\
		    break;\
	    }\
        }                           \
    } while (0)

str
PARThash(Client cntxt, MalBlkPtr mb, MalStkPtr stk, InstrPtr pci)
{
	bat *ret = getArgReference_bat(stk,pci,0);
	int	i, pieces;
	BAT *b, *bn[MAXPART];
	BUN r, p; 
	BATiter bi;
	str msg = MAL_SUCCEED;


	(void) cntxt;
	(void) mb;
	pieces = pci->retc;
	if ( pieces >= MAXPART)
		throw(MAL,"partition.hash","too many partitions");
	b = BATdescriptor( stk->stk[getArg(pci, pci->retc)].val.ival);
	if ( b == NULL)
		throw(MAL, "partition.hash", SQLSTATE(HY002) RUNTIME_OBJECT_MISSING);

	for( i = 0; i < pieces; i++){
		size_t cap = BATcount(b) / pieces * 1.1;
		bn[i] = COLnew(0, b->ttype, cap, TRANSIENT);
		if (bn[i] == NULL){
			for(i--;  i>=0; i--)
				BBPunfix(bn[i]->batCacheid);
			BBPunfix(b->batCacheid);
			throw(MAL, "partition.hash", SQLSTATE(HY001) MAL_MALLOC_FAIL);
		}
		if ( b->tvheap && bn[i]->tvheap && ATOMstorage(b->ttype) != TYPE_str){
			if (HEAPextend(bn[i]->tvheap, b->tvheap->size, TRUE) != GDK_SUCCEED) {
				for(i--;  i>=0; i--)
					BBPunfix(bn[i]->batCacheid);
				BBPunfix(b->batCacheid);
				BBPunfix(bn[i]->batCacheid);
				throw(MAL, "partition.hash", SQLSTATE(HY001) MAL_MALLOC_FAIL);
			}
		}
	}
	/* distribute the elements over multiple partitions */
	switch(ATOMstorage(b->ttype)){
		case TYPE_bte:
			hashpartition(bte);
			break;
		case TYPE_sht:
			hashpartition(sht);
			break;
		case TYPE_int:
			hashpartition(int);
			break;
		case TYPE_lng:
			hashpartition(lng);
			break;
#ifdef HAVE_HGE
		case TYPE_hge:
			hashpartition(hge);
			break;
#endif
		default:
			msg = createException(MAL,"partition.hash","Non-supported type");
	}

	for(i=0; i< pieces; i++){
		ret = getArgReference_bat(stk,pci,i);
		BBPkeepref(*ret = bn[i]->batCacheid);
	}
	BBPunfix(b->batCacheid);
	return msg;
}
