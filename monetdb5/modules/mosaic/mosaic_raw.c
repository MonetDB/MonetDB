/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0.  If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 *
 * Copyright 1997 - July 2008 CWI, August 2008 - 2018 MonetDB B.V.
 */

/*
 * 2014-2016 author Martin Kersten
 * Use a chunk that has not been compressed
 */

#include "monetdb_config.h"
#include "mosaic.h"
#include "mosaic_raw.h"
#include "mosaic_private.h"

bool MOStypes_raw(BAT* b) {
	switch(b->ttype){
	case TYPE_bit: return true; // Will be mapped to bte
	case TYPE_bte: return true;
	case TYPE_sht: return true;
	case TYPE_int: return true;
	case TYPE_lng: return true;
	case TYPE_oid: return true;
	case TYPE_flt: return true;
	case TYPE_dbl: return true;
#ifdef HAVE_HGE
	case TYPE_hge: return true;
#endif
	default:
		if (b->ttype == TYPE_date) {return true;} // Will be mapped to int
		if (b->ttype == TYPE_daytime) {return true;} // Will be mapped to lng
		if (b->ttype == TYPE_timestamp) {return true;} // Will be mapped to lng
	}

	return false;
}

void
MOSlayout_raw(MOStask task, BAT *btech, BAT *bcount, BAT *binput, BAT *boutput, BAT *bproperties)
{
	MosaicBlk blk = (MosaicBlk) task->blk;
	lng cnt = MOSgetCnt(blk), input=0, output= 0;

		input = cnt * ATOMsize(task->type);
	switch(ATOMbasetype(task->type)){
	case TYPE_bte: output = wordaligned( MosaicBlkSize + sizeof(bte)* MOSgetCnt(blk),bte); break;
	case TYPE_sht: output = wordaligned( MosaicBlkSize + sizeof(sht)* MOSgetCnt(blk),sht); break;
	case TYPE_int: output = wordaligned( MosaicBlkSize + sizeof(int)* MOSgetCnt(blk),int); break;
	case TYPE_lng: output = wordaligned( MosaicBlkSize + sizeof(lng)* MOSgetCnt(blk),lng); break;
	case TYPE_oid: output = wordaligned( MosaicBlkSize + sizeof(oid)* MOSgetCnt(blk),oid); break;
	case TYPE_flt: output = wordaligned( MosaicBlkSize + sizeof(flt)* MOSgetCnt(blk),flt); break;
	case TYPE_dbl: output = wordaligned( MosaicBlkSize + sizeof(dbl)* MOSgetCnt(blk),dbl); break;
#ifdef HAVE_HGE
	case TYPE_hge: output = wordaligned( MosaicBlkSize + sizeof(hge)* MOSgetCnt(blk),hge); break;
#endif
	}
	if( BUNappend(btech, "raw blk", false) != GDK_SUCCEED ||
		BUNappend(bcount, &cnt, false) != GDK_SUCCEED ||
		BUNappend(binput, &input, false) != GDK_SUCCEED ||
		BUNappend(boutput, &output, false) != GDK_SUCCEED ||
		BUNappend(bproperties, "", false) != GDK_SUCCEED)
		return;
}

void
MOSadvance_raw(MOStask task)
{
	MosaicBlk blk = task->blk;

	task->start += MOSgetCnt(blk);
	switch(ATOMbasetype(task->type)){
	case TYPE_bte: task->blk = (MosaicBlk)( ((char*) task->blk) + wordaligned( MosaicBlkSize + sizeof(bte)* MOSgetCnt(blk),bte)); break;
	case TYPE_sht: task->blk = (MosaicBlk)( ((char*) task->blk) + wordaligned( MosaicBlkSize + sizeof(sht)* MOSgetCnt(blk),sht)); break;
	case TYPE_int: task->blk = (MosaicBlk)( ((char*) task->blk) + wordaligned( MosaicBlkSize + sizeof(int)* MOSgetCnt(blk),int)); break;
	case TYPE_lng: task->blk = (MosaicBlk)( ((char*) task->blk) + wordaligned( MosaicBlkSize + sizeof(lng)* MOSgetCnt(blk),lng)); break;
	case TYPE_oid: task->blk = (MosaicBlk)( ((char*) task->blk) + wordaligned( MosaicBlkSize + sizeof(oid)* MOSgetCnt(blk),oid)); break;
	case TYPE_flt: task->blk = (MosaicBlk)( ((char*) task->blk) + wordaligned( MosaicBlkSize + sizeof(flt)* MOSgetCnt(blk),flt)); break;
	case TYPE_dbl: task->blk = (MosaicBlk)( ((char*) task->blk) + wordaligned( MosaicBlkSize + sizeof(dbl)* MOSgetCnt(blk),dbl)); break;
#ifdef HAVE_HGE
	case TYPE_hge: task->blk = (MosaicBlk)( ((char*) task->blk) + wordaligned( MosaicBlkSize + sizeof(hge)* MOSgetCnt(blk),hge)); break;
#endif
	}
}

void
MOSskip_raw( MOStask task)
{
	MOSadvance_raw(task);
	if ( MOSgetTag(task->blk) == MOSAIC_EOL)
		task->blk = 0; // ENDOFLIST
}

#define Estimate(TPE)\
{\
	/*The raw compression technique is always applicable and only adds one item at a time.*/\
	current->compression_strategy.tag = MOSAIC_RAW;\
	current->is_applicable = true;\
	current->uncompressed_size += (BUN) sizeof(TPE);\
	unsigned int cnt = previous->compression_strategy.cnt;\
	if (previous->compression_strategy.tag == MOSAIC_RAW && cnt + 1 < (1 << CNT_BITS)) {\
		current->must_be_merged_with_previous = true;\
		cnt++;\
		current->compressed_size += sizeof(TPE);\
	}\
	else {\
		current->must_be_merged_with_previous = false;\
		cnt = 1;\
		current->compressed_size += wordaligned(MosaicBlkSize, TPE) + sizeof(TPE);\
	}\
	current->compression_strategy.cnt = cnt;\
}

str
MOSestimate_raw(MOStask task, MosaicEstimation* current, const MosaicEstimation* previous) {

	switch(ATOMbasetype(task->type)){
	case TYPE_bte: Estimate(bte); break;
	case TYPE_sht: Estimate(sht); break;
	case TYPE_int: Estimate(int); break;
	case TYPE_lng: Estimate(lng); break;
	case TYPE_oid: Estimate(oid); break;
	case TYPE_flt: Estimate(flt); break;
	case TYPE_dbl: Estimate(dbl); break;
#ifdef HAVE_HGE
	case TYPE_hge: Estimate(hge); break;
#endif
	}

	return MAL_SUCCEED;
}


// append a series of values into the non-compressed block

#define RAWcompress(TYPE)\
{\
	TYPE *v = ((TYPE*)task->src) + task->start;\
	unsigned int cnt = estimate->cnt;\
	TYPE *d = (TYPE*)task->dst;\
	for(unsigned int i = 0; i<cnt; i++,v++){\
		*d++ = (TYPE) *v;\
	}\
	hdr->checksum.sum##TYPE += *(TYPE*) task->dst;\
	task->dst += sizeof(TYPE);\
	MOSsetCnt(blk,cnt);\
}

// rather expensive simple value non-compressed store
void
MOScompress_raw(MOStask task, MosaicBlkRec* estimate)
{
	MosaicHdr hdr = task->hdr;
	MosaicBlk blk = (MosaicBlk) task->blk;

	MOSsetTag(blk,MOSAIC_RAW);

	switch(ATOMbasetype(task->type)){
	case TYPE_bte: RAWcompress(bte); break;
	case TYPE_sht: RAWcompress(sht); break;
	case TYPE_int: RAWcompress(int); break;
	case TYPE_lng: RAWcompress(lng); break;
	case TYPE_oid: RAWcompress(oid); break;
	case TYPE_flt: RAWcompress(flt); break;
	case TYPE_dbl: RAWcompress(dbl); break;
#ifdef HAVE_HGE
	case TYPE_hge: RAWcompress(hge); break;
#endif
	}
}

// the inverse operator, extend the src
#define RAWdecompress(TYPE)\
{ BUN lim = MOSgetCnt(blk); \
	for(i = 0; i < lim; i++) {\
	((TYPE*)task->src)[i] = ((TYPE*)compressed)[i]; \
	hdr->checksum2.sum##TYPE += ((TYPE*)compressed)[i]; \
	}\
	task->src += i * sizeof(TYPE);\
}

void
MOSdecompress_raw(MOStask task)
{
	MosaicHdr hdr = task->hdr;
	MosaicBlk blk = (MosaicBlk) task->blk;
	BUN i;
	char *compressed;

	compressed = ((char*)blk) + MosaicBlkSize;
	switch(ATOMbasetype(task->type)){
	case TYPE_bte: RAWdecompress(bte); break;
	case TYPE_sht: RAWdecompress(sht); break;
	case TYPE_int: RAWdecompress(int); break;
	case TYPE_lng: RAWdecompress(lng); break;
	case TYPE_oid: RAWdecompress(oid); break;
	case TYPE_flt: RAWdecompress(flt); break;
	case TYPE_dbl: RAWdecompress(dbl); break;
#ifdef HAVE_HGE
	case TYPE_hge: RAWdecompress(hge); break;
#endif
	}
}

// The remainder should provide the minimal algebraic framework
//  to apply the operator to a RAW compressed chunk


/* generic range select
 *
 * This macro is based on the combined behavior of ALGselect2 and BATselect.
 * It should return the same output on the same input.
 *
 * A complete breakdown of the various arguments follows.  Here, v, v1
 * and v2 are values from the appropriate domain, and
 * v != nil, v1 != nil, v2 != nil, v1 < v2.
 *	tl	th	li	hi	anti	result list of OIDs for values
 *	-----------------------------------------------------------------
 *	nil	nil	true	true	false	x == nil (only way to get nil)
 *	nil	nil	true	true	true	x != nil
 *	nil	nil	A*		B*		false	x != nil *it must hold that A && B == false.
 *	nil	nil	A*		B*		true	NOTHING *it must hold that A && B == false.
 *	v	v	A*		B*		true	x != nil *it must hold that A && B == false.
 *	v	v	A*		B*		false	NOTHING *it must hold that A && B == false.
 *	v2	v1	ignored	ignored	false	NOTHING
 *	v2	v1	ignored	ignored	true	x != nil
 *	nil	v	ignored	false	false	x < v
 *	nil	v	ignored	true	false	x <= v
 *	nil	v	ignored	false	true	x >= v
 *	nil	v	ignored	true	true	x > v
 *	v	nil	false	ignored	false	x > v
 *	v	nil	true	ignored	false	x >= v
 *	v	nil	false	ignored	true	x <= v
 *	v	nil	true	ignored	true	x < v
 *	v	v	true	true	false	x == v
 *	v	v	true	true	true	x != v
 *	v1	v2	false	false	false	v1 < x < v2
 *	v1	v2	true	false	false	v1 <= x < v2
 *	v1	v2	false	true	false	v1 < x <= v2
 *	v1	v2	true	true	false	v1 <= x <= v2
 *	v1	v2	false	false	true	x <= v1 or x >= v2
 *	v1	v2	true	false	true	x < v1 or x >= v2
 *	v1	v2	false	true	true	x <= v1 or x > v2
 */
#define select_raw_general(LOW, HIGH, LI, HI, HAS_NIL, ANTI, TPE) {\
	TPE *val= (TPE*) (((char*) task->blk) + MosaicBlkSize);\
\
	if		( IS_NIL(TPE, (LOW)) &&  IS_NIL(TPE, (HIGH)) && (LI) && (HI) && !(ANTI)) {\
		if(HAS_NIL) {\
			for( ; first < last; first++, val++){\
				MOSskipit();\
				if (IS_NIL(TPE, *(TPE*)val))\
					*o++ = (oid) first;\
			}\
		}\
	}\
	else if	( IS_NIL(TPE, (LOW)) &&  IS_NIL(TPE, (HIGH)) && (LI) && (HI) && (ANTI)) {\
		if(HAS_NIL) {\
			for( ; first < last; first++, val++){\
				MOSskipit();\
				if (!IS_NIL(TPE, *(TPE*)val))\
					*o++ = (oid) first;\
			}\
		}\
		else for( ; first < last; first++){ MOSskipit(); *o++ = (oid) first; }\
	}\
	else if	( IS_NIL(TPE, (LOW)) &&  IS_NIL(TPE, (HIGH)) && !((LI) && (HI)) && !(ANTI)) {\
		if(HAS_NIL) {\
			for( ; first < last; first++, val++){\
				MOSskipit();\
				if (!IS_NIL(TPE, *(TPE*)val))\
					*o++ = (oid) first;\
			}\
		}\
		else for( ; first < last; first++){ MOSskipit(); *o++ = (oid) first; }\
	}\
	else if	( IS_NIL(TPE, (LOW)) &&  IS_NIL(TPE, (HIGH)) && !((LI) && (HI)) && (ANTI)) {\
			/*Empty result set.*/\
	}\
	else if	( !IS_NIL(TPE, (LOW)) &&  !IS_NIL(TPE, (HIGH)) && (LOW) == (HIGH) && !((LI) && (HI)) && (ANTI)) {\
		if(HAS_NIL) {\
			for( ; first < last; first++, val++){\
				MOSskipit();\
				if (!IS_NIL(TPE, *(TPE*)val))\
					*o++ = (oid) first;\
			}\
		}\
		else for( ; first < last; first++){ MOSskipit(); *o++ = (oid) first; }\
	}\
	else if	( !IS_NIL(TPE, (LOW)) &&  !IS_NIL(TPE, (HIGH)) && (LOW) == (HIGH) && !((LI) && (HI)) && !(ANTI)) {\
		/*Empty result set.*/\
	}\
	else if	( !IS_NIL(TPE, (LOW)) &&  !IS_NIL(TPE, (HIGH)) && (LOW) > (HIGH) && !(ANTI)) {\
		/*Empty result set.*/\
	}\
	else if	( !IS_NIL(TPE, (LOW)) &&  !IS_NIL(TPE, (HIGH)) && (LOW) > (HIGH) && (ANTI)) {\
		if(HAS_NIL) {\
			for( ; first < last; first++, val++){\
				MOSskipit();\
				if (!IS_NIL(TPE, *(TPE*)val))\
					*o++ = (oid) first;\
			}\
		}\
		else for( ; first < last; first++){ MOSskipit(); *o++ = (oid) first; }\
	}\
	else {\
		/*normal cases.*/\
		if( IS_NIL(TPE, LOW) ){\
			for( ; first < last; first++, val++){\
				MOSskipit();\
				if (HAS_NIL && IS_NIL(TPE, *(TPE*)val)) { continue;}\
				cmp  =  (((HI) && *(TPE*)val <= (HIGH) ) || (!(HI) && *(TPE*)val < (HIGH) ));\
				if (cmp == !(ANTI))\
					*o++ = (oid) first;\
			}\
		} else\
		if( IS_NIL(TPE, HIGH) ){\
			for( ; first < last; first++, val++){\
				MOSskipit();\
				if (HAS_NIL && IS_NIL(TPE, *(TPE*)val)) { continue;}\
				cmp  =  (((LI) && *(TPE*)val >= (LOW) ) || (!(LI) && *(TPE*)val > (LOW) ));\
				if (cmp == !(ANTI))\
					*o++ = (oid) first;\
			}\
		} else{\
			for( ; first < last; first++, val++){\
				MOSskipit();\
				if (HAS_NIL && IS_NIL(TPE, *(TPE*)val)) { continue;}\
				cmp  =  (((HI) && *(TPE*)val <= (HIGH) ) || (!(HI) && *(TPE*)val < (HIGH) )) &&\
						(((LI) && *(TPE*)val >= (LOW) ) || (!(LI) && *(TPE*)val > (LOW) ));\
				if (cmp == !(ANTI))\
					*o++ = (oid) first;\
			}\
		}\
	}\
}

#define select_raw(TPE) {\
	if( nil && *anti){\
		select_raw_general(*(TPE*) low, *(TPE*) hgh, *li, *hi, true, true, TPE);\
	}\
	if( !nil && *anti){\
		select_raw_general(*(TPE*) low, *(TPE*) hgh, *li, *hi, false, true, TPE);\
	}\
	if( nil && !*anti){\
		select_raw_general(*(TPE*) low, *(TPE*) hgh, *li, *hi, true, false, TPE);\
	}\
	if( !nil && !*anti){\
		select_raw_general(*(TPE*) low, *(TPE*) hgh, *li, *hi, false, false, TPE);\
	}\
}

str
MOSselect_raw( MOStask task, void *low, void *hgh, bit *li, bit *hi, bit *anti)
{
	oid *o;
	BUN first,last;
	int cmp;

	// set the oid range covered and advance scan range
	first = task->start;
	last = first + MOSgetCnt(task->blk);
	bool nil = !task->bsrc->tnonil;

	if (task->cl && *task->cl > last){
		MOSskip_raw(task);
		return MAL_SUCCEED;
	}
	o = task->lb;

	switch(ATOMbasetype(task->type)){
	case TYPE_bte: select_raw(bte); break;
	case TYPE_sht: select_raw(sht); break;
	case TYPE_int: select_raw(int); break;
	case TYPE_lng: select_raw(lng); break;
	case TYPE_oid: select_raw(oid); break;
	case TYPE_flt: select_raw(flt); break;
	case TYPE_dbl: select_raw(dbl); break;
#ifdef HAVE_HGE
	case TYPE_hge: select_raw(hge); break;
#endif
	}
	MOSskip_raw(task);
	task->lb = o;
	return MAL_SUCCEED;
}

#define thetaselect_raw_normalized(HAS_NIL, ANTI, TPE) \
for( ; first < last; first++, v++){\
	if	(HAS_NIL && IS_NIL(TPE, *(TPE*)v)) { continue;}\
	bool cmp = (IS_NIL(TPE, low) || * v >= low) && (IS_NIL(TPE, hgh)  || * v <= hgh);\
	if (cmp == !(ANTI)) {\
		MOSskipit();\
		*o++ = (oid) first;\
	}\
}\

#define thetaselect_raw_general(HAS_NIL, TPE)\
{ 	TPE low,hgh, *v;\
	low= hgh = TPE##_nil;\
	if ( strcmp(oper,"<") == 0){\
		hgh= *(TPE*) val;\
		hgh = PREVVALUE##TPE(hgh);\
	} else\
	if ( strcmp(oper,"<=") == 0){\
		hgh= *(TPE*) val;\
	} else\
	if ( strcmp(oper,">") == 0){\
		low = *(TPE*) val;\
		low = NEXTVALUE##TPE(low);\
	} else\
	if ( strcmp(oper,">=") == 0){\
		low = *(TPE*) val;\
	} else\
	if ( strcmp(oper,"!=") == 0){\
		hgh= low= *(TPE*) val;\
		anti++;\
	} else\
	if ( strcmp(oper,"==") == 0){\
		hgh= low= *(TPE*) val;\
	} \
	v = (TPE*) (((char*)task->blk) + MosaicBlkSize);\
	if (!anti) {\
		thetaselect_raw_normalized(HAS_NIL, false, TPE);\
	}\
	else {\
		thetaselect_raw_normalized(HAS_NIL, true, TPE);\
	}\
}

#define thetaselect_raw(TPE) {\
	if( nil ){\
		thetaselect_raw_general(true, TPE);\
	}\
	else /*!nil*/{\
		thetaselect_raw_general(false, TPE);\
	}\
}

str
MOSthetaselect_raw( MOStask task, void *val, str oper)
{
	oid *o;
	int anti=0;
	BUN first,last;

	// set the oid range covered and advance scan range
	first = task->start;
	last = first + MOSgetCnt(task->blk);

	if (task->cl && *task->cl > last){
		MOSskip_raw(task);
		return MAL_SUCCEED;
	}
	if ( first + MOSgetCnt(task->blk) > last)
		last = MOSgetCnt(task->blk);
	o = task->lb;
	bool nil = !task->bsrc->tnonil;

	switch(ATOMbasetype(task->type)){
	case TYPE_bte: thetaselect_raw(bte); break;
	case TYPE_sht: thetaselect_raw(sht); break;
	case TYPE_int: thetaselect_raw(int); break;
	case TYPE_lng: thetaselect_raw(lng); break;
	case TYPE_oid: thetaselect_raw(oid); break;
	case TYPE_flt: thetaselect_raw(flt); break;
	case TYPE_dbl: thetaselect_raw(dbl); break;
#ifdef HAVE_HGE
	case TYPE_hge: thetaselect_raw(hge); break;
#endif
	}
	MOSskip_raw(task);
	task->lb =o;
	return MAL_SUCCEED;
}

#define projection_raw(TPE)\
{	TPE *val, *v;\
	v= (TPE*) task->src;\
	val = (TPE*) (((char*) task->blk) + MosaicBlkSize);\
	for(; first < last; first++, val++){\
		MOSskipit();\
		*v++ = *val;\
		task->cnt++;\
	}\
	task->src = (char*) v;\
}

str
MOSprojection_raw( MOStask task)
{
	BUN first,last;
		// set the oid range covered and advance scan range
	first = task->start;
	last = first + MOSgetCnt(task->blk);

	switch(ATOMbasetype(task->type)){
		case TYPE_bte: projection_raw(bte); break;
		case TYPE_sht: projection_raw(sht); break;
		case TYPE_int: projection_raw(int); break;
		case TYPE_lng: projection_raw(lng); break;
		case TYPE_oid: projection_raw(oid); break;
		case TYPE_flt: projection_raw(flt); break;
		case TYPE_dbl: projection_raw(dbl); break;
#ifdef HAVE_HGE
		case TYPE_hge: projection_raw(hge); break;
#endif
	}
	MOSskip_raw(task);
	return MAL_SUCCEED;
}

#define join_raw_general(HAS_NIL, NIL_MATCHES, TPE)\
{	TPE *v, *w;\
	v = (TPE*) (((char*) task->blk) + MosaicBlkSize);\
	for(oo= (oid) first; first < last; first++, v++, oo++){\
		if (HAS_NIL && !NIL_MATCHES) {\
			if ((IS_NIL(TPE, *v))) {continue;};\
		}\
		w = (TPE*) task->src;\
		for(n = task->stop, o = 0; n -- > 0; w++,o++) {\
			if (ARE_EQUAL(*w, *v, HAS_NIL, TPE)){\
				if( BUNappend(task->lbat, &oo, false)!= GDK_SUCCEED ||\
				BUNappend(task->rbat, &o, false) != GDK_SUCCEED)\
				throw(MAL,"mosaic.raw",MAL_MALLOC_FAIL);\
			}\
		}\
	}\
}

#define join_raw(TPE) {\
	if( nil && nil_matches){\
		join_raw_general(true, true, TPE);\
	}\
	if( !nil && nil_matches){\
		join_raw_general(false, true, TPE);\
	}\
	if( nil && !nil_matches){\
		join_raw_general(true, false, TPE);\
	}\
	if( !nil && !nil_matches){\
		join_raw_general(false, false, TPE);\
	}\
}



str
MOSjoin_raw( MOStask task, bit nil_matches)
{
	BUN n,first,last;
	oid o, oo;
		// set the oid range covered and advance scan range
	first = task->start;
	last = first + MOSgetCnt(task->blk);
	bool nil = !task->bsrc->tnonil;

	switch(ATOMbasetype(task->type)){
		case TYPE_bte: join_raw(bte); break;
		case TYPE_sht: join_raw(sht); break;
		case TYPE_int: join_raw(int); break;
		case TYPE_lng: join_raw(lng); break;
		case TYPE_oid: join_raw(oid); break;
		case TYPE_flt: join_raw(flt); break;
		case TYPE_dbl: join_raw(dbl); break;
#ifdef HAVE_HGE
		case TYPE_hge: join_raw(hge); break;
#endif
	}
	MOSskip_raw(task);
	return MAL_SUCCEED;
}
