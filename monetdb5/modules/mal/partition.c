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

#define keyedhashpartition(TYPE)                         		\
    do { p= BATcount(b);                               		\
	 bi = bat_iterator(b);					\
        for (r=0; r < p; r++) {                    		\
            TYPE *v = (TYPE *) BUNtloc(bi, r);          	\
	    TYPE c =  mix_##TYPE((*v));      			\
	    if((c % pieces) == key && BUNappend(bn, (void*) v, FALSE) != GDK_SUCCEED){ \
		    msg = createException(MAL,"partition.hash","Storage failed");\
		    break;\
	    }\
        }                           \
    } while (0)

#define slicepartition(TYPE)                         		\
    do { p= BATcount(b);                               		\
	 bi = bat_iterator(b);					\
        for (r=0; r < p; r++) {                    		\
            TYPE *v = (TYPE *) BUNtloc(bi, r);          	\
	    TYPE c =  mix_##TYPE((*v));      			\
	    if( BUNappend(bn[(int) (c % pieces)], (void*) &r, FALSE) != GDK_SUCCEED){ \
		    msg = createException(MAL,"partition.slice","Storage failed");\
		    break;\
	    }\
        }                           \
    } while (0)

#define keyedslicepartition(TYPE)                         		\
    do { p= BATcount(b);                               		\
	 bi = bat_iterator(b);					\
        for (r=0; r < p; r++) {                    		\
            TYPE *v = (TYPE *) BUNtloc(bi, r);          	\
	    TYPE c =  mix_##TYPE((*v));      			\
	    if( (c % pieces) == key && BUNappend(bn, (void*) &r, FALSE) != GDK_SUCCEED){ \
		    msg = createException(MAL,"partition.slice","Storage failed");\
		    break;\
	    }\
        }                           \
    } while (0)

str
PARThash(Client cntxt, MalBlkPtr mb, MalStkPtr stk, InstrPtr pci)
{
	bat *ret = getArgReference_bat(stk,pci,0);
	int bid = *getArgReference_bat(stk,pci,pci->retc);
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
	b = BATdescriptor( bid);
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
str
PARThashkeyed(Client cntxt, MalBlkPtr mb, MalStkPtr stk, InstrPtr pci)
{
	bat *ret = getArgReference_bat(stk,pci,0);
	int bid = *getArgReference_bat(stk,pci,1);
	int key = *getArgReference_int(stk,pci,2);
	int pieces = *getArgReference_int(stk,pci,3);
	BAT *b, *bn;
	BUN r, p; 
	BATiter bi;
	str msg = MAL_SUCCEED;


	(void) cntxt;
	(void) mb;
	if ( pieces >= MAXPART || pieces < 1)
		throw(MAL,"partition.slice","too many partitions");
	b = BATdescriptor( bid);
	if ( b == NULL)
		throw(MAL, "partition.hash", SQLSTATE(HY002) RUNTIME_OBJECT_MISSING);

	size_t cap = BATcount(b) / pieces * 1.1;
	bn = COLnew(0, b->ttype, cap, TRANSIENT);
	if (bn == NULL){
		BBPunfix(b->batCacheid);
		throw(MAL, "partition.hash", SQLSTATE(HY001) MAL_MALLOC_FAIL);
	}
	if ( b->tvheap && bn->tvheap && ATOMstorage(b->ttype) != TYPE_str){
		if (HEAPextend(bn->tvheap, b->tvheap->size, TRUE) != GDK_SUCCEED) {
			BBPunfix(b->batCacheid);
			BBPunfix(bn->batCacheid);
			throw(MAL, "partition.hash", SQLSTATE(HY001) MAL_MALLOC_FAIL);
		}
	}
	/* distribute the elements over multiple partitions */
	switch(ATOMstorage(b->ttype)){
		case TYPE_bte:
			keyedhashpartition(bte);
			break;
		case TYPE_sht:
			keyedhashpartition(sht);
			break;
		case TYPE_int:
			keyedhashpartition(int);
			break;
		case TYPE_lng:
			keyedhashpartition(lng);
			break;
#ifdef HAVE_HGE
		case TYPE_hge:
			keyedhashpartition(hge);
			break;
#endif
		default:
			msg = createException(MAL,"partition.hash","Non-supported type");
	}

	BBPkeepref(*ret = bn->batCacheid);
	BBPunfix(b->batCacheid);
	return msg;
}

str
PARTslice(Client cntxt, MalBlkPtr mb, MalStkPtr stk, InstrPtr pci)
{
	bat *ret = getArgReference_bat(stk,pci,0);
	int bid = *getArgReference_bat(stk,pci,pci->retc);
	int	i, pieces;
	BAT *b, *bn[MAXPART];
	BUN r, p; 
	BATiter bi;
	str msg = MAL_SUCCEED;


	(void) cntxt;
	(void) mb;
	pieces = pci->retc;
	if ( pieces >= MAXPART)
		throw(MAL,"partition.slice","too many partitions");
	b = BATdescriptor( bid);
	if ( b == NULL)
		throw(MAL, "partition.slice", SQLSTATE(HY002) RUNTIME_OBJECT_MISSING);

	for( i = 0; i < pieces; i++){
		size_t cap = BATcount(b) / pieces * 1.1;
		bn[i] = COLnew(0, TYPE_oid, cap, TRANSIENT);
		if (bn[i] == NULL){
			for(i--;  i>=0; i--)
				BBPunfix(bn[i]->batCacheid);
			BBPunfix(b->batCacheid);
			throw(MAL, "partition.slice", SQLSTATE(HY001) MAL_MALLOC_FAIL);
		}
	}
	/* distribute the elements over multiple partitions */
	switch(ATOMstorage(b->ttype)){
		case TYPE_bte:
			slicepartition(bte);
			break;
		case TYPE_sht:
			slicepartition(sht);
			break;
		case TYPE_int:
			slicepartition(int);
			break;
		case TYPE_lng:
			slicepartition(lng);
			break;
#ifdef HAVE_HGE
		case TYPE_hge:
			slicepartition(hge);
			break;
#endif
		default:
			msg = createException(MAL,"partition.slice","Non-supported type");
	}

	for(i=0; i< pieces; i++){
		ret = getArgReference_bat(stk,pci,i);
		BBPkeepref(*ret = bn[i]->batCacheid);
	}
	BBPunfix(b->batCacheid);
	return msg;
}

str
PARTslicekeyed(Client cntxt, MalBlkPtr mb, MalStkPtr stk, InstrPtr pci)
{
	bat *ret = getArgReference_bat(stk,pci,0);
	int bid = *getArgReference_bat(stk,pci,1);
	int key = *getArgReference_int(stk,pci,2);
	int pieces = *getArgReference_int(stk,pci,3);
	BAT *b, *bn;
	BUN r, p; 
	BATiter bi;
	str msg = MAL_SUCCEED;

	(void) cntxt;
	(void) mb;
	if ( pieces >= MAXPART || pieces < 1)
		throw(MAL,"partition.slice","too many partitions");
	b = BATdescriptor( bid);
	if ( b == NULL)
		throw(MAL, "partition.slice", SQLSTATE(HY002) RUNTIME_OBJECT_MISSING);

	size_t cap = BATcount(b) / pieces * 1.1;
	bn = COLnew(0, TYPE_oid, cap, TRANSIENT);
	if (bn == NULL){
		BBPunfix(b->batCacheid);
		throw(MAL, "partition.slice", SQLSTATE(HY001) MAL_MALLOC_FAIL);
	}
	/* distribute the elements over multiple partitions */
	switch(ATOMstorage(b->ttype)){
		case TYPE_bte:
			keyedslicepartition(bte);
			break;
		case TYPE_sht:
			keyedslicepartition(sht);
			break;
		case TYPE_int:
			keyedslicepartition(int);
			break;
		case TYPE_lng:
			keyedslicepartition(lng);
			break;
#ifdef HAVE_HGE
		case TYPE_hge:
			keyedslicepartition(hge);
			break;
#endif
		default:
			msg = createException(MAL,"partition.slice","Non-supported type");
	}

	BBPkeepref(*ret = bn->batCacheid);
	BBPunfix(b->batCacheid);
	return msg;
}
