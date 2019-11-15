/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0.  If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 *
 * Copyright 1997 - July 2008 CWI, August 2008 - 2018 MonetDB B.V.
 */

/*
 * 2014-2016 author Martin Kersten
 * Linear encoding
 * Replace a well-behaving series by its [start,step] value.
 */

#include "monetdb_config.h"
#include "mosaic.h"
#include "mosaic_linear.h"
#include "mosaic_private.h"

bool MOStypes_linear(BAT* b) {
	switch(b->ttype){
	case TYPE_bit: return true;
	case TYPE_bte: return true;
	case TYPE_sht: return true;
	case TYPE_int: return true;
	case TYPE_lng: return true;
	case TYPE_oid: return true;
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

#define linear_base(BLK) ((void*)(((char*) BLK)+ MosaicBlkSize))

static void*
linear_step(MOStask task, MosaicBlk blk){
	switch(ATOMbasetype(task->type)){
	case TYPE_bte : return (void*) ( ((char*)blk)+ MosaicBlkSize+ sizeof(bte));
	case TYPE_sht : return (void*) ( ((char*)blk)+ MosaicBlkSize+ sizeof(sht));
	case TYPE_int : return (void*) ( ((char*)blk)+ MosaicBlkSize+ sizeof(int));
	case TYPE_lng : return (void*) ( ((char*)blk)+ MosaicBlkSize+ sizeof(lng));
	case TYPE_oid : return (void*) ( ((char*)blk)+ MosaicBlkSize+ sizeof(oid));
#ifdef HAVE_HGE
	case TYPE_hge : return (void*) ( ((char*)blk)+ MosaicBlkSize+ sizeof(hge));
#endif
	}
	return 0;
}

void
MOSlayout_linear(MOStask task, BAT *btech, BAT *bcount, BAT *binput, BAT *boutput, BAT *bproperties)
{
	MosaicBlk blk = task->blk;
	lng cnt = MOSgetCnt(blk), input=0, output= 0;

	input = cnt * ATOMsize(task->type);
	switch(ATOMbasetype(task->type)){
	case TYPE_bte: output = wordaligned( MosaicBlkSize + 2 * sizeof(bte),bte); break;
	case TYPE_sht: output = wordaligned( MosaicBlkSize + 2 * sizeof(sht),sht); break;
	case TYPE_int: output = wordaligned( MosaicBlkSize + 2 * sizeof(int),int); break;
	case TYPE_oid: output = wordaligned( MosaicBlkSize + 2 * sizeof(oid),oid); break;
	case TYPE_lng: output = wordaligned( MosaicBlkSize + 2 * sizeof(lng),lng); break;
#ifdef HAVE_HGE
	case TYPE_hge: output = wordaligned( MosaicBlkSize + 2 * sizeof(hge),hge); break;
#endif
	}
	if( BUNappend(btech, "linear blk", false) != GDK_SUCCEED ||
		BUNappend(bcount, &cnt, false) != GDK_SUCCEED ||
		BUNappend(binput, &input, false) != GDK_SUCCEED ||
		BUNappend(boutput, &output, false) != GDK_SUCCEED ||
		BUNappend(bproperties, "", false) != GDK_SUCCEED )
		return;
}

void
MOSadvance_linear(MOStask task)
{
	task->start += MOSgetCnt(task->blk);
	switch(ATOMbasetype(task->type)){
	case TYPE_bte: task->blk = (MosaicBlk)( ((char*)task->blk) + wordaligned( MosaicBlkSize + 2 * sizeof(bte),bte)); break;
	case TYPE_sht: task->blk = (MosaicBlk)( ((char*)task->blk) + wordaligned( MosaicBlkSize + 2 * sizeof(sht),sht)); break;
	case TYPE_int: task->blk = (MosaicBlk)( ((char*)task->blk) + wordaligned( MosaicBlkSize + 2 * sizeof(int),int)); break;
	case TYPE_oid: task->blk = (MosaicBlk)( ((char*)task->blk) + wordaligned( MosaicBlkSize + 2 * sizeof(oid),oid)); break;
	case TYPE_lng: task->blk = (MosaicBlk)( ((char*)task->blk) + wordaligned( MosaicBlkSize + 2 * sizeof(lng),lng)); break;
#ifdef HAVE_HGE
	case TYPE_hge: task->blk = (MosaicBlk)( ((char*)task->blk) + wordaligned( MosaicBlkSize + 2 * sizeof(hge),hge)); break;
#endif
	}
}

void
MOSskip_linear(MOStask task)
{
	MOSadvance_linear( task);
	if ( MOSgetTag(task->blk) == MOSAIC_EOL)
		task->blk = 0; // ENDOFLIST
}

#define Estimate(TPE)\
{\
	current->compression_strategy.tag = MOSAIC_LINEAR;\
	TPE *c = ((TPE*) task->src)+task->start; /*(c)urrent value*/\
	BUN limit = task->stop - task->start > MOSAICMAXCNT? MOSAICMAXCNT: task->stop - task->start;\
	BUN i = 1;\
	if (limit > 1 ){\
		TPE *p = c++; /*(p)revious value*/\
		DeltaTpe(TPE) step = GET_DELTA(TPE, *c, *p);\
		for( ; i < limit; i++, p++, c++) {\
			DeltaTpe(TPE) current_step = GET_DELTA(TPE, *c, *p);\
			if (  current_step != step)\
				break;\
		}\
	}\
	assert(i > 0);/*Should always compress.*/\
	current->is_applicable = true;\
	current->uncompressed_size += (BUN) (i * sizeof(TPE));\
	current->compressed_size += wordaligned( MosaicBlkSize + 2 * sizeof(TPE),TPE);\
	current->compression_strategy.cnt = (unsigned int) i;\
}

// calculate the expected reduction using LINEAR in terms of elements compressed
str
MOSestimate_linear(MOStask task, MosaicEstimation* current, const MosaicEstimation* previous)
{
	(void) previous;

	switch(ATOMbasetype(task->type)){
	case TYPE_bte: Estimate(bte); break;
	case TYPE_sht: Estimate(sht); break;
	case TYPE_int: Estimate(int); break;
	case TYPE_oid: Estimate(oid); break;
	case TYPE_lng: Estimate(lng); break;
#ifdef HAVE_HGE
	case TYPE_hge: Estimate(hge); break;
#endif
	}
	return MAL_SUCCEED;
}

// insert a series of values into the compressor block using linear.
#define LINEARcompress(TPE)\
{\
	TPE *c = ((TPE*) task->src)+task->start; /*(c)urrent value*/\
	TPE step = 0;\
	BUN limit = estimate->cnt;\
	*(TPE*) linear_base(blk) = *c;\
	if (limit > 1 ){\
		TPE *p = c++; /*(p)revious value*/\
		step = (TPE) GET_DELTA(TPE, *c, *p);\
	}\
	MOSsetCnt(blk, limit);\
	*(TPE*) linear_step(task,blk) = step;\
	task->dst = ((char*) blk)+ wordaligned(MosaicBlkSize +  2 * sizeof(TPE),MosaicBlkRec);\
}

	//task->dst = ((char*) blk)+ MosaicBlkSize +  2 * sizeof(TPE);
void
MOScompress_linear(MOStask task, MosaicBlkRec* estimate)
{
	MosaicBlk blk = task->blk;

	MOSsetTag(blk,MOSAIC_LINEAR);

	switch(ATOMbasetype(task->type)){
	case TYPE_bte: LINEARcompress(bte); break;
	case TYPE_sht: LINEARcompress(sht); break;
	case TYPE_int: LINEARcompress(int); break;
	case TYPE_oid: LINEARcompress(oid); break;
	case TYPE_lng: LINEARcompress(lng); break;
#ifdef HAVE_HGE
	case TYPE_hge: LINEARcompress(hge); break;
#endif
	}
}

// the inverse operator, extend the src
#define LINEARdecompress(TPE)\
{	DeltaTpe(TPE) val = *(TPE*) linear_base(blk);\
	DeltaTpe(TPE) step = *(TPE*) linear_step(task,blk);\
	BUN lim = MOSgetCnt(blk);\
	for(i = 0; i < lim; i++, val += step) {\
		((TPE*)task->src)[i] = (TPE) val;\
	}\
	task->src += i * sizeof(TPE);\
}

void
MOSdecompress_linear(MOStask task)
{
	MosaicBlk blk =  task->blk;
	BUN i;

	switch(ATOMbasetype(task->type)){
	case TYPE_bte: LINEARdecompress(bte); break;
	case TYPE_sht: LINEARdecompress(sht); break;
	case TYPE_int: LINEARdecompress(int); break;
	case TYPE_oid: LINEARdecompress(oid); break;
	case TYPE_lng: LINEARdecompress(lng); break;
#ifdef HAVE_HGE
	case TYPE_hge: LINEARdecompress(hge); break;
#endif
	}
}

// perform relational algebra operators over non-compressed chunks
// They are bound by an oid range and possibly a candidate list

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
#define  select_delta_general(LOW, HIGH, LI, HI, HAS_NIL, ANTI, TPE) \
{\
	DeltaTpe(TPE) value	= *(TPE*) linear_base(blk) ;\
	DeltaTpe(TPE) step	= *(TPE*) linear_step(task,blk);\
	if		( IS_NIL(TPE, (LOW)) &&  IS_NIL(TPE, (HIGH)) && (LI) && (HI) && !(ANTI)) {\
		if(HAS_NIL) {\
			for( ; first < last; first++, value += step){\
				MOSskipit();\
				TPE v = (TPE) value;\
				if (IS_NIL(TPE, v))\
					*o++ = (oid) first;\
			}\
		}\
	}\
	else if	( IS_NIL(TPE, (LOW)) &&  IS_NIL(TPE, (HIGH)) && (LI) && (HI) && (ANTI)) {\
		if(HAS_NIL) {\
			for( ; first < last; first++, value += step){\
				MOSskipit();\
				TPE v = (TPE) value;\
				if (!IS_NIL(TPE, v))\
					*o++ = (oid) first;\
			}\
		}\
		else for( ; first < last; first++){ MOSskipit(); *o++ = (oid) first; }\
	}\
	else if	( IS_NIL(TPE, (LOW)) &&  IS_NIL(TPE, (HIGH)) && !((LI) && (HI)) && !(ANTI)) {\
		if(HAS_NIL) {\
			for( ; first < last; first++, value += step){\
				MOSskipit();\
				TPE v = (TPE) value;\
				if (!IS_NIL(TPE, v))\
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
			for( ; first < last; first++, value += step) {\
				MOSskipit();\
				TPE v = (TPE) value;\
				if (!IS_NIL(TPE, v))\
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
			for( ; first < last; first++, value += step){\
				MOSskipit();\
				TPE v = (TPE) value;\
				if (!IS_NIL(TPE, v))\
					*o++ = (oid) first;\
			}\
		}\
		else for( ; first < last; first++){ MOSskipit(); *o++ = (oid) first; }\
	}\
	else {\
		/*normal cases.*/\
		if( IS_NIL(TPE, (LOW)) ){\
			for( ; first < last; first++, value += step){\
				MOSskipit();\
				TPE v = (TPE) value;\
				if (HAS_NIL && IS_NIL(TPE, v)) { continue;}\
				bool cmp  =  (((HI) && v <= (HIGH) ) || (!(HI) && v < (HIGH) ));\
				if (cmp == !(ANTI))\
					*o++ = (oid) first;\
			}\
		} else\
		if( IS_NIL(TPE, (HIGH)) ){\
			for( ; first < last; first++, value += step){\
				MOSskipit();\
				TPE v = (TPE) value;\
				if (HAS_NIL && IS_NIL(TPE, v)) { continue;}\
				bool cmp  =  (((LI) && v >= (LOW) ) || (!(LI) && v > (LOW) ));\
				if (cmp == !(ANTI))\
					*o++ = (oid) first;\
			}\
		} else{\
			for( ; first < last; first++, value += step){\
				MOSskipit();\
				TPE v = (TPE) value;\
				if (HAS_NIL && IS_NIL(TPE, v)) { continue;}\
				bool cmp  =  (((HI) && v <= (HIGH) ) || (!(HI) && v < (HIGH) )) &&\
						(((LI) && v >= (LOW) ) || (!(LI) && v > (LOW) ));\
				if (cmp == !(ANTI))\
					*o++ = (oid) first;\
			}\
		}\
	}\
}

#define select_delta(TPE) {\
	if( nil && *anti) {\
		select_delta_general(*(TPE*) low, *(TPE*) hgh, *li, *hi, true, true, TPE);\
	}\
	if( !nil && *anti) {\
		select_delta_general(*(TPE*) low, *(TPE*) hgh, *li, *hi, false, true, TPE);\
	}\
	if( nil && !*anti) {\
		select_delta_general(*(TPE*) low, *(TPE*) hgh, *li, *hi, true, false, TPE);\
	}\
	if( !nil && !*anti) {\
		select_delta_general(*(TPE*) low, *(TPE*) hgh, *li, *hi, false, false, TPE);\
	}\
}

str
MOSselect_linear( MOStask task, void *low, void *hgh, bit *li, bit *hi, bit *anti){
	oid *o;
	BUN first,last;
	MosaicBlk blk =  task->blk;

	// set the oid range covered and advance scan range
	first = task->start;
	last = first + MOSgetCnt(blk);
	bool nil = !task->bsrc->tnonil;

	if (task->cl && *task->cl > last){
		MOSskip_linear(task);
		return MAL_SUCCEED;
	}
	o = task->lb;

	switch(ATOMbasetype(task->type)){
	case TYPE_bte: select_delta(bte); break;
	case TYPE_sht: select_delta(sht); break;
	case TYPE_int: select_delta(int); break;
	case TYPE_oid: select_delta(oid); break;
	case TYPE_lng: select_delta(lng); break;
#ifdef HAVE_HGE
	case TYPE_hge: select_delta(hge); break;
#endif
	}
	MOSskip_linear(task);
	task->lb = o;
	return MAL_SUCCEED;
}

#define thetaselect_linear_normalized(HAS_NIL, ANTI, TPE) \
for( ; first < last; first++, value += step){\
	TPE v = (TPE) value;\
	MOSskipit();\
	if (HAS_NIL && IS_NIL(TPE, v)) { continue;}\
	bool cmp =  (IS_NIL(TPE, low) || v >= low) && (v <= hgh || IS_NIL(TPE, hgh)) ;\
	if (cmp == !(ANTI))\
		*o++ = (oid) first;\
}\

#define thetaselect_linear_general(HAS_NIL, TPE)\
{ 	TPE low,hgh;\
	DeltaTpe(TPE) value	= *(TPE*) linear_base(blk) ;\
	DeltaTpe(TPE) step	= *(TPE*) linear_step(task,blk);\
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
		low = hgh = *(TPE*) val;\
		anti++;\
	} else\
	if ( strcmp(oper,"==") == 0){\
		hgh= low= *(TPE*) val;\
	} \
	if (!anti) {\
		thetaselect_linear_normalized(HAS_NIL, false, TPE);\
	}\
	else {\
		thetaselect_linear_normalized(HAS_NIL, true, TPE);\
	}\
}

#define thetaselect_linear(TPE) {\
	if( nil ){\
		thetaselect_linear_general(true, TPE);\
	}\
	else /*!nil*/{\
		thetaselect_linear_general(false, TPE);\
	}\
}

str
MOSthetaselect_linear( MOStask task,void *val, str oper)
{
	oid *o;
	int anti=0;
	BUN first,last;
	MosaicBlk blk= task->blk;
	
	// set the oid range covered and advance scan range
	first = task->start;
	last = first + MOSgetCnt(task->blk);
	bool nil = !task->bsrc->tnonil;

	if (task->cl && *task->cl > last){
		MOSskip_linear(task);
		return MAL_SUCCEED;
	}
	o = task->lb;

	switch(ATOMbasetype(task->type)){
	case TYPE_bte: thetaselect_linear(bte); break;
	case TYPE_sht: thetaselect_linear(sht); break;
	case TYPE_int: thetaselect_linear(int); break;
	case TYPE_oid: thetaselect_linear(oid); break;
	case TYPE_lng: thetaselect_linear(lng); break;
#ifdef HAVE_HGE
	case TYPE_hge: thetaselect_linear(hge); break;
#endif
	}
	MOSskip_linear(task);
	task->lb =o;
	return MAL_SUCCEED;
}

#define projection_linear(TPE)\
{TPE *v;\
	DeltaTpe(TPE) value	= *(TPE*) linear_base(blk) ;\
	DeltaTpe(TPE) step	= *(TPE*) linear_step(task,blk);\
	v= (TPE*) task->src;\
	for(; first < last; first++, value+=step){\
		MOSskipit();\
		*v++ = (TPE) value;\
		task->cnt++;\
	}\
	task->src = (char*) v;\
}

str
MOSprojection_linear( MOStask task)
{
	BUN first,last;
	MosaicBlk blk = task->blk;

	// set the oid range covered and advance scan range
	first = task->start;
	last = first + MOSgetCnt(task->blk);

	switch(ATOMbasetype(task->type)){
		case TYPE_bte: projection_linear(bte); break;
		case TYPE_sht: projection_linear(sht); break;
		case TYPE_int: projection_linear(int); break;
		case TYPE_oid: projection_linear(oid); break;
		case TYPE_lng: projection_linear(lng); break;
#ifdef HAVE_HGE
		case TYPE_hge: projection_linear(hge); break;
#endif
	}
	MOSskip_linear(task);
	return MAL_SUCCEED;
}

#define join_linear_general(HAS_NIL, NIL_MATCHES, TPE)\
{	TPE *w = (TPE*) task->src;\
	for(n = task->stop, o = 0; n -- > 0; w++,o++) {\
		DeltaTpe(TPE) value	= *(TPE*) linear_base(blk) ;\
		DeltaTpe(TPE) step	= *(TPE*) linear_step(task,blk);\
		for(oo= (oid) first; oo < (oid) last; value+=step, oo++) {\
			if (HAS_NIL && !NIL_MATCHES) {\
				if (IS_NIL(TPE, (TPE) value)) { continue;}\
			}\
			if (ARE_EQUAL(*w, (TPE) value, HAS_NIL, TPE)){\
				if(BUNappend(task->lbat, &oo, false) != GDK_SUCCEED ||\
				BUNappend(task->rbat, &o, false) != GDK_SUCCEED )\
				throw(MAL,"mosaic.delta",MAL_MALLOC_FAIL);\
			}\
		}\
	}\
}	

#define join_linear(TPE) {\
	if( nil && nil_matches){\
		join_linear_general(true, true, TPE);\
	}\
	if( !nil && nil_matches){\
		join_linear_general(false, true, TPE);\
	}\
	if( nil && !nil_matches){\
		join_linear_general(true, false, TPE);\
	}\
	if( !nil && !nil_matches){\
		join_linear_general(false, false, TPE);\
	}\
}

str
MOSjoin_linear( MOStask task, bit nil_matches)
{
	MosaicBlk blk = task->blk;
	BUN n,first,last;
	oid o, oo;

	// set the oid range covered and advance scan range
	first = task->start;
	last = first + MOSgetCnt(task->blk);
	bool nil = !task->bsrc->tnonil;

	switch(ATOMbasetype(task->type)){
		case TYPE_bte: join_linear(bte); break;
		case TYPE_sht: join_linear(sht); break;
		case TYPE_int: join_linear(int); break;
		case TYPE_lng: join_linear(lng); break;
		case TYPE_oid: join_linear(oid); break;
#ifdef HAVE_HGE
		case TYPE_hge: join_linear(hge); break;
#endif
	}
	MOSskip_linear(task);
	return MAL_SUCCEED;
}
