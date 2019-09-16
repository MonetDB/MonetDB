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

#define linear_base(BLK) ((void*)(((char*) BLK)+ MosaicBlkSize))

static void*
linear_step(MOStask task, MosaicBlk blk){
	switch(ATOMbasetype(task->type)){
	case TYPE_bte : return (void*) ( ((char*)blk)+ MosaicBlkSize+ sizeof(bte));
	case TYPE_sht : return (void*) ( ((char*)blk)+ MosaicBlkSize+ sizeof(sht));
	case TYPE_int : return (void*) ( ((char*)blk)+ MosaicBlkSize+ sizeof(int));
	case TYPE_lng : return (void*) ( ((char*)blk)+ MosaicBlkSize+ sizeof(lng));
	case TYPE_oid : return (void*) ( ((char*)blk)+ MosaicBlkSize+ sizeof(oid));
	case TYPE_flt : return (void*) ( ((char*)blk)+ MosaicBlkSize+ sizeof(flt));
	case TYPE_dbl : return (void*) ( ((char*)blk)+ MosaicBlkSize+ sizeof(dbl));
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
	case TYPE_flt: output = wordaligned( MosaicBlkSize + 2 * sizeof(flt),flt); break;
	case TYPE_dbl: output = wordaligned( MosaicBlkSize + 2 * sizeof(dbl),dbl); break;
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
	case TYPE_flt: task->blk = (MosaicBlk)( ((char*)task->blk) + wordaligned( MosaicBlkSize + 2 * sizeof(flt),flt)); break;
	case TYPE_dbl: task->blk = (MosaicBlk)( ((char*)task->blk) + wordaligned( MosaicBlkSize + 2 * sizeof(dbl),dbl)); break;
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

#define Estimate(TYPE)\
{	TYPE *v = ((TYPE*) task->src)+task->start, val = *v++;\
	TYPE step = *v - val;\
	BUN limit = task->stop - task->start > MOSAICMAXCNT? MOSAICMAXCNT: task->stop - task->start;\
	if( task->range[MOSAIC_LINEAR] > task->start + 1){\
		i = task->range[MOSAIC_LINEAR] - task->start;\
		if (i * sizeof(TYPE) <= wordaligned( MosaicBlkSize + 2 * sizeof(TYPE),TYPE))\
			return 0.0;\
		factor = ((flt) i * sizeof(TYPE))/ wordaligned(MosaicBlkSize + 2 * sizeof(TYPE),TYPE);\
		return factor;\
	}\
	for( i=1; i < limit; i++, val = *v, v++)\
	if (  *v - val != step)\
		break;\
	if(i * sizeof(TYPE) <= wordaligned( MosaicBlkSize + 2 * sizeof(TYPE),TYPE))\
		return 0.0;\
	if( task->dst +  wordaligned(MosaicBlkSize + 2 * sizeof(TYPE),TYPE) >= task->bsrc->tmosaic->base + task->bsrc->tmosaic->size)\
		return 0.0;\
	factor =  ( (flt)i * sizeof(TYPE))/wordaligned( MosaicBlkSize + 2 * sizeof(TYPE),TYPE);\
}

// calculate the expected reduction using LINEAR in terms of elements compressed
flt
MOSestimate_linear(MOStask task)
{	BUN i = -1;
	flt factor = 0.0;

	switch(ATOMbasetype(task->type)){
	case TYPE_bte: Estimate(bte); break;
	case TYPE_sht: Estimate(sht); break;
	case TYPE_int: Estimate(int); break;
	case TYPE_oid: Estimate(oid); break;
	case TYPE_lng: Estimate(lng); break;
	case TYPE_flt: Estimate(flt); break;
	case TYPE_dbl: Estimate(dbl); break;
#ifdef HAVE_HGE
	case TYPE_hge: Estimate(hge); break;
#endif
	}
	task->factor[MOSAIC_LINEAR] = factor;
	task->range[MOSAIC_LINEAR] = task->start + i;
	return factor;
}

// insert a series of values into the compressor block using linear.
#define LINEARcompress(TYPE)\
{	TYPE *v = ((TYPE*) task->src) + task->start, val = *v++;\
	TYPE step = *v - val;\
	BUN limit = task->stop - task->start >= MOSAICMAXCNT? MOSAICMAXCNT : task->stop - task->start;\
	*(TYPE*) linear_base(blk) = val;\
	hdr->checksum.sum##TYPE += val;\
	*(TYPE*) linear_step(task,blk) = step;\
	for(i=1; i<limit; i++, val = *v++){\
		if (  *v - val != step)\
			break;\
		hdr->checksum.sum##TYPE += *v;\
	} MOSsetCnt(blk, i);\
	task->dst = ((char*) blk)+ wordaligned(MosaicBlkSize +  2 * sizeof(TYPE),MosaicBlkRec);\
}

	//task->dst = ((char*) blk)+ MosaicBlkSize +  2 * sizeof(TYPE);
void
MOScompress_linear(MOStask task)
{
	BUN i;
	MosaicHdr hdr = task->hdr;
	MosaicBlk blk = task->blk;

	MOSsetTag(blk,MOSAIC_LINEAR);

	switch(ATOMbasetype(task->type)){
	case TYPE_bte: LINEARcompress(bte); break;
	case TYPE_sht: LINEARcompress(sht); break;
	case TYPE_int: LINEARcompress(int); break;
	case TYPE_oid: LINEARcompress(oid); break;
	case TYPE_lng: LINEARcompress(lng); break;
	case TYPE_flt: LINEARcompress(flt); break;
	case TYPE_dbl: LINEARcompress(dbl); break;
#ifdef HAVE_HGE
	case TYPE_hge: LINEARcompress(hge); break;
#endif
	}
}

// the inverse operator, extend the src
#define LINEARdecompress(TYPE)\
{	TYPE val = *(TYPE*) linear_base(blk);\
	TYPE step = *(TYPE*) linear_step(task,blk);\
	BUN lim = MOSgetCnt(blk);\
	for(i = 0; i < lim; i++) {\
		((TYPE*)task->src)[i] = val + (TYPE) (i * step);\
		hdr->checksum2.sum##TYPE += ((TYPE*)task->src)[i];\
	}\
	task->src += i * sizeof(TYPE);\
}

void
MOSdecompress_linear(MOStask task)
{
	MosaicHdr hdr =  task->hdr;
	MosaicBlk blk =  task->blk;
	BUN i;

	switch(ATOMbasetype(task->type)){
	case TYPE_bte: LINEARdecompress(bte); break;
	case TYPE_sht: LINEARdecompress(sht); break;
	case TYPE_int: LINEARdecompress(int); break;
	case TYPE_oid: LINEARdecompress(oid); break;
	case TYPE_lng: LINEARdecompress(lng); break;
	case TYPE_flt: LINEARdecompress(flt); break;
	case TYPE_dbl: LINEARdecompress(dbl); break;
#ifdef HAVE_HGE
	case TYPE_hge: LINEARdecompress(hge); break;
#endif
	}
}

// perform relational algebra operators over non-compressed chunks
// They are bound by an oid linear and possibly a candidate list

#define  select_linear(TYPE) \
{	TYPE val = *(TYPE*) linear_base(blk) ;\
	TYPE step = *(TYPE*) linear_step(task,blk);\
	if( !*anti){\
		if( *(TYPE*) low == TYPE##_nil && *(TYPE*) hgh == TYPE##_nil){\
			for( ; first < last; first++){\
				MOSskipit();\
				*o++ = (oid) first;\
			}\
		} else\
		if( *(TYPE*) low == TYPE##_nil ){\
			for( ; first < last; first++, val+=step){\
				MOSskipit();\
				cmp  =  ((*hi && val <= * (TYPE*)hgh ) || (!*hi && val < *(TYPE*)hgh ));\
				if (cmp )\
					*o++ = (oid) first;\
			}\
		} else\
		if( *(TYPE*) hgh == TYPE##_nil ){\
			for( ; first < last; first++, val+= step){\
				MOSskipit();\
				cmp  =  ((*li && val >= * (TYPE*)low ) || (!*li && val > *(TYPE*)low ));\
				if (cmp )\
					*o++ = (oid) first;\
			}\
		} else{\
			for( ; first < last; first++,val+=step){\
				MOSskipit();\
				cmp  =  ((*hi && val <= * (TYPE*)hgh ) || (!*hi && val < *(TYPE*)hgh )) &&\
						((*li && val >= * (TYPE*)low ) || (!*li && val > *(TYPE*)low ));\
				if (cmp )\
					*o++ = (oid) first;\
			}\
		}\
	} else {\
		if( *(TYPE*) low == TYPE##_nil && *(TYPE*) hgh == TYPE##_nil){\
			/* nothing is matching */\
		} else\
		if( *(TYPE*) low == TYPE##_nil ){\
			for( ; first < last; first++, val+=step){\
				MOSskipit();\
				cmp  =  ((*hi && val <= * (TYPE*)hgh ) || (!*hi && val < *(TYPE*)hgh ));\
				if ( !cmp )\
					*o++ = (oid) first;\
			}\
		} else\
		if( *(TYPE*) hgh == TYPE##_nil ){\
			for( ; first < last; first++, val+=step){\
				MOSskipit();\
				cmp  =  ((*li && val >= * (TYPE*)low ) || (!*li && val > *(TYPE*)low ));\
				if ( !cmp )\
					*o++ = (oid) first;\
			}\
		} else{\
			for( ; first < last; first++, val+=step){\
				MOSskipit();\
				cmp  =  ((*hi && val <= * (TYPE*)hgh ) || (!*hi && val < *(TYPE*)hgh )) &&\
						((*li && val >= * (TYPE*)low ) || (!*li && val > *(TYPE*)low ));\
				if (!cmp)\
					*o++ = (oid) first;\
			}\
		}\
	}\
}

str
MOSselect_linear( MOStask task, void *low, void *hgh, bit *li, bit *hi, bit *anti){
	oid *o;
	BUN first,last;
	int cmp;
	MosaicBlk blk =  task->blk;

	// set the oid range covered and advance scan range
	first = task->start;
	last = first + MOSgetCnt(blk);

	if (task->cl && *task->cl > last){
		MOSskip_linear(task);
		return MAL_SUCCEED;
	}
	o = task->lb;

	switch(ATOMbasetype(task->type)){
	case TYPE_bte: select_linear(bte); break;
	case TYPE_sht: select_linear(sht); break;
	case TYPE_int: select_linear(int); break;
	case TYPE_oid: select_linear(oid); break;
	case TYPE_lng: select_linear(lng); break;
	case TYPE_flt: select_linear(flt); break;
	case TYPE_dbl: select_linear(dbl); break;
#ifdef HAVE_HGE
	case TYPE_hge: select_linear(hge); break;
#endif
	}
	MOSskip_linear(task);
	task->lb = o;
	return MAL_SUCCEED;
}

#define thetaselect_linear(TYPE)\
{ 	TYPE low,hgh;\
	TYPE v = *(TYPE*) linear_base(blk) ;\
	TYPE step = *(TYPE*) linear_step(task,blk);\
	low= hgh = TYPE##_nil;\
	if ( strcmp(oper,"<") == 0){\
		hgh= *(TYPE*) val;\
		hgh = PREVVALUE##TYPE(hgh);\
	} else\
	if ( strcmp(oper,"<=") == 0){\
		hgh= *(TYPE*) val;\
	} else\
	if ( strcmp(oper,">") == 0){\
		low = *(TYPE*) val;\
		low = NEXTVALUE##TYPE(low);\
	} else\
	if ( strcmp(oper,">=") == 0){\
		low = *(TYPE*) val;\
	} else\
	if ( strcmp(oper,"!=") == 0){\
		low = hgh = *(TYPE*) val;\
		anti++;\
	} else\
	if ( strcmp(oper,"==") == 0){\
		hgh= low= *(TYPE*) val;\
	} \
	if ( !anti) {\
		for( ; first < last; first++, v+=step){\
			MOSskipit();\
			if( (low == TYPE##_nil || v >= low) && (v <= hgh || hgh == TYPE##_nil) )\
				*o++ = (oid) first;\
		}\
	} else\
	if( anti)\
		for( ; first < last; first++, v+=step){\
			MOSskipit();\
			if( !( (low == TYPE##_nil || v >= low) && (v <= hgh || hgh == TYPE##_nil) ))\
			*o++ = (oid) first;\
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
	case TYPE_flt: thetaselect_linear(flt); break;
	case TYPE_dbl: thetaselect_linear(dbl); break;
	}
	MOSskip_linear(task);
	task->lb =o;
	return MAL_SUCCEED;
}

#define projection_linear(TYPE)\
{TYPE *v;\
	TYPE val = *(TYPE*) linear_base(blk) ;\
	TYPE step = *(TYPE*) linear_step(task,blk);\
	v= (TYPE*) task->src;\
	for(; first < last; first++, val+=step){\
		MOSskipit();\
		*v++ = val;\
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
		case TYPE_flt: projection_linear(flt); break;
		case TYPE_dbl: projection_linear(dbl); break;
#ifdef HAVE_HGE
		case TYPE_hge: projection_linear(hge); break;
#endif
	}
	MOSskip_linear(task);
	return MAL_SUCCEED;
}

#define join_linear(TYPE)\
{	TYPE *w = (TYPE*) task->src;\
	TYPE step = *(TYPE*) linear_step(task,blk);\
	for(n = task->stop, o = 0; n -- > 0; w++,o++) {\
		TYPE val = *(TYPE*) linear_base(blk) ;\
		for(oo= (oid) first; oo < (oid) last; val+=step, oo++)\
			if ( *w == val){\
				if(BUNappend(task->lbat, &oo, false)!= GDK_SUCCEED ||\
				BUNappend(task->rbat, &o, false)!= GDK_SUCCEED)\
				throw(MAL,"mosaic.linear",MAL_MALLOC_FAIL);;\
			}\
	}\
}

str
MOSjoin_linear( MOStask task)
{
	MosaicBlk blk = task->blk;
	BUN n,first,last;
	oid o, oo;

	// set the oid range covered and advance scan range
	first = task->start;
	last = first + MOSgetCnt(task->blk);

	switch(ATOMbasetype(task->type)){
		case TYPE_bte: join_linear(bte); break;
		case TYPE_sht: join_linear(sht); break;
		case TYPE_int: join_linear(int); break;
		case TYPE_oid: join_linear(oid); break;
		case TYPE_lng: join_linear(lng); break;
		case TYPE_flt: join_linear(flt); break;
		case TYPE_dbl: join_linear(dbl); break;
#ifdef HAVE_HGE
		case TYPE_hge: join_linear(hge); break;
#endif
	}
	MOSskip_linear(task);
	return MAL_SUCCEED;
}
