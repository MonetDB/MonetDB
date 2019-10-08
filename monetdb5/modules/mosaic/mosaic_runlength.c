/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0.  If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 *
 * Copyright 1997 - July 2008 CWI, August 2008 - 2018 MonetDB B.V.
 */

/*
 * 2014-2016 author Martin Kersten
 * Run-length encoding framework for a single chunk
 */

#include "monetdb_config.h"
#include "mosaic.h"
#include "mosaic_runlength.h"
#include "mosaic_private.h"

bool MOStypes_runlength(BAT* b) {
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
MOSlayout_runlength(MOStask task, BAT *btech, BAT *bcount, BAT *binput, BAT *boutput, BAT *bproperties)
{
	MosaicBlk blk = task->blk;
	lng cnt = MOSgetCnt(blk), input=0, output= 0;

	input = cnt * ATOMsize(task->type);
	switch(ATOMbasetype(task->type)){
	case TYPE_bte: output = wordaligned( MosaicBlkSize + sizeof(bte),bte); break;
	case TYPE_sht: output = wordaligned( MosaicBlkSize + sizeof(sht),sht); break;
	case TYPE_int: output = wordaligned( MosaicBlkSize + sizeof(int),int); break;
	case TYPE_lng: output = wordaligned( MosaicBlkSize + sizeof(lng),lng); break;
	case TYPE_oid: output = wordaligned( MosaicBlkSize + sizeof(oid),oid); break;
	case TYPE_flt: output = wordaligned( MosaicBlkSize + sizeof(flt),flt); break;
	case TYPE_dbl: output = wordaligned( MosaicBlkSize + sizeof(dbl),dbl); break;
#ifdef HAVE_HGE
	case TYPE_hge: output = wordaligned( MosaicBlkSize + sizeof(hge),hge); break;
#endif
	}
	if( BUNappend(btech, "runlength blk", false) != GDK_SUCCEED ||
		BUNappend(bcount, &cnt, false) != GDK_SUCCEED ||
		BUNappend(binput, &input, false) != GDK_SUCCEED ||
		BUNappend(boutput, &output, false) != GDK_SUCCEED ||
		BUNappend(bproperties, "", false) != GDK_SUCCEED )
		return;
}

void
MOSadvance_runlength(MOStask task)
{

	task->start += MOSgetCnt(task->blk);
	//task->stop = task->stop;
	switch(ATOMbasetype(task->type)){
	case TYPE_bte: task->blk = (MosaicBlk)( ((char*)task->blk) + wordaligned( MosaicBlkSize + sizeof(bte),bte)); break;
	case TYPE_sht: task->blk = (MosaicBlk)( ((char*)task->blk) + wordaligned( MosaicBlkSize + sizeof(sht),sht)); break;
	case TYPE_int: task->blk = (MosaicBlk)( ((char*)task->blk) + wordaligned( MosaicBlkSize + sizeof(int),int)); break;
	case TYPE_lng: task->blk = (MosaicBlk)( ((char*)task->blk) + wordaligned( MosaicBlkSize + sizeof(lng),lng)); break;
	case TYPE_oid: task->blk = (MosaicBlk)( ((char*)task->blk) + wordaligned( MosaicBlkSize + sizeof(oid),oid)); break;
	case TYPE_flt: task->blk = (MosaicBlk)( ((char*)task->blk) + wordaligned( MosaicBlkSize + sizeof(flt),flt)); break;
	case TYPE_dbl: task->blk = (MosaicBlk)( ((char*)task->blk) + wordaligned( MosaicBlkSize + sizeof(dbl),dbl)); break;
#ifdef HAVE_HGE
	case TYPE_hge: task->blk = (MosaicBlk)( ((char*)task->blk) + wordaligned( MosaicBlkSize + sizeof(hge),hge)); break;
#endif
	}
}

void
MOSskip_runlength(MOStask task)
{
	MOSadvance_runlength(task);
	if ( MOSgetTag(task->blk) == MOSAIC_EOL)
		task->blk = 0; // ENDOFLIST
}

#define Estimate(TYPE)\
{	TYPE *v = ((TYPE*) task->src) + task->start, val = *v;\
	BUN limit = task->stop - task->start > MOSAICMAXCNT? MOSAICMAXCNT: task->stop - task->start;\
	for(v++,i = 1; i < limit; i++,v++) if ( *v != val) break;\
	assert(i > 0);/*Should always compress.*/\
	current->is_applicable = true;\
	current->uncompressed_size += (BUN) (i * sizeof(TYPE));\
	current->compressed_size += wordaligned( MosaicBlkSize, TYPE) + sizeof(TYPE);\
	current->compression_strategy.cnt = i;\
}

// calculate the expected reduction using RLE in terms of elements compressed
str
MOSestimate_runlength(MOStask task, MosaicEstimation* current, const MosaicEstimation* previous)
{	unsigned int i = 0;
	flt factor = 0.0;
	(void) previous;
	current->compression_strategy.tag = MOSAIC_RLE;

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
	task->factor[MOSAIC_RLE] = factor;
	task->range[MOSAIC_RLE] = task->start + i;
	return MAL_SUCCEED;
}

// insert a series of values into the compressor block using rle.
#define RLEcompress(TYPE)\
{	TYPE *v = ((TYPE*) task->src)+task->start, val = *v;\
	TYPE *dst = (TYPE*) task->dst;\
	BUN limit = task->stop - task->start > MOSAICMAXCNT ? MOSAICMAXCNT: task->stop - task->start;\
	*dst = val;\
	for(v++, i =1; i<limit; i++,v++)\
	if ( *v != val)\
		break;\
	hdr->checksum.sum##TYPE += (TYPE) (i * val);\
	MOSsetCnt(blk, i);\
	task->dst +=  sizeof(TYPE);\
}

void
MOScompress_runlength(MOStask task)
{
	BUN i ;
	MosaicHdr hdr  = task->hdr;
	MosaicBlk blk = task->blk;

		MOSsetTag(blk, MOSAIC_RLE);

	switch(ATOMbasetype(task->type)){
	case TYPE_bte: RLEcompress(bte); break;
	case TYPE_sht: RLEcompress(sht); break;
	case TYPE_int: RLEcompress(int); break;
	case TYPE_lng: RLEcompress(lng); break;
	case TYPE_oid: RLEcompress(oid); break;
	case TYPE_flt: RLEcompress(flt); break;
	case TYPE_dbl: RLEcompress(dbl); break;
#ifdef HAVE_HGE
	case TYPE_hge: RLEcompress(hge); break;
#endif
	}
}

// the inverse operator, extend the src
#define RLEdecompress(TYPE)\
{	TYPE val = *(TYPE*) compressed;\
	BUN lim = MOSgetCnt(blk);\
	for(i = 0; i < lim; i++)\
		((TYPE*)task->src)[i] = val;\
	hdr->checksum2.sum##TYPE += (TYPE)(i * val);\
	task->src += i * sizeof(TYPE);\
}

void
MOSdecompress_runlength(MOStask task)
{
	MosaicHdr hdr = task->hdr;
	MosaicBlk blk =  ((MosaicBlk) task->blk);
	BUN i;
	char *compressed;

	compressed = (char*) blk + MosaicBlkSize;
	switch(ATOMbasetype(task->type)){
	case TYPE_bte: RLEdecompress(bte); break;
	case TYPE_sht: RLEdecompress(sht); break;
	case TYPE_lng: RLEdecompress(lng); break;
	case TYPE_oid: RLEdecompress(oid); break;
	case TYPE_flt: RLEdecompress(flt); break;
	case TYPE_dbl: RLEdecompress(dbl); break;
#ifdef HAVE_HGE
	case TYPE_hge: RLEdecompress(hge); break;
#endif
	case TYPE_int:
		{	int val = *(int*) compressed ;
			BUN lim= MOSgetCnt(blk);
			for(i = 0; i < lim; i++)
				((int*)task->src)[i] = val;
			hdr->checksum2.sumint += (int) (i * val);
			task->src += i * sizeof(int);
		}
		break;
	}
}

// perform relational algebra operators over non-compressed chunks
// They are bound by an oid range and possibly a candidate list

#define  select_runlength(TPE) \
{ 	TPE *val= (TPE*) (((char*) task->blk) + MosaicBlkSize);\
\
	if( !*anti){\
		if( is_nil(TPE, *(TPE*) low) && is_nil(TPE, *(TPE*) hgh)){\
			for( ; first < last; first++){\
				MOSskipit();\
				*o++ = (oid) first;\
			}\
		} else\
		if( is_nil(TPE, *(TPE*) low) ){\
			cmp  =  ((*hi && *(TPE*)val <= * (TPE*)hgh ) || (!*hi && *(TPE*)val < *(TPE*)hgh ));\
			if (cmp )\
			for( ; first < last; first++){\
				MOSskipit();\
				*o++ = (oid) first;\
			}\
		} else\
		if( is_nil(TPE, *(TPE*) hgh) ){\
			cmp  =  ((*li && *(TPE*)val >= * (TPE*)low ) || (!*li && *(TPE*)val > *(TPE*)low ));\
			if (cmp )\
			for( ; first < last; first++){\
				MOSskipit();\
				*o++ = (oid) first;\
			}\
		} else{\
			cmp  =  ((*hi && *(TPE*)val <= * (TPE*)hgh ) || (!*hi && *(TPE*)val < *(TPE*)hgh )) &&\
					((*li && *(TPE*)val >= * (TPE*)low ) || (!*li && *(TPE*)val > *(TPE*)low ));\
			if (cmp )\
			for( ; first < last; first++){\
				MOSskipit();\
				*o++ = (oid) first;\
			}\
		}\
	} else {\
		if( is_nil(TPE, *(TPE*) low) && is_nil(TPE, *(TPE*) hgh)){\
			/* nothing is matching */\
		} else\
		if( is_nil(TPE, *(TPE*) low) ){\
			cmp  =  ((*hi && *(TPE*)val <= * (TPE*)hgh ) || (!*hi && *(TPE*)val < *(TPE*)hgh ));\
			if ( !cmp )\
			for( ; first < last; first++){\
				MOSskipit();\
				*o++ = (oid) first;\
			}\
		} else\
		if( is_nil(TPE, *(TPE*) hgh) ){\
			cmp  =  ((*li && *(TPE*)val >= * (TPE*)low ) || (!*li && *(TPE*)val > *(TPE*)low ));\
			if ( !cmp )\
			for( ; first < last; first++, val++){\
				MOSskipit();\
				*o++ = (oid) first;\
			}\
		} else{\
			cmp  =  ((*hi && *(TPE*)val <= * (TPE*)hgh ) || (!*hi && *(TPE*)val < *(TPE*)hgh )) &&\
					((*li && *(TPE*)val >= * (TPE*)low ) || (!*li && *(TPE*)val > *(TPE*)low ));\
			if (!cmp)\
			for( ; first < last; first++, val++){\
				MOSskipit();\
				*o++ = (oid) first;\
			}\
		}\
	}\
}

str
MOSselect_runlength( MOStask task, void *low, void *hgh, bit *li, bit *hi, bit *anti){
	oid *o;
	int cmp;
	BUN first,last;

	// set the oid range covered
	first = task->start;
	last = first + MOSgetCnt(task->blk);

	if (task->cl && *task->cl > last){
		MOSadvance_runlength(task);
		return MAL_SUCCEED;
	}
	o = task->lb;

	switch(ATOMbasetype(task->type)){
	case TYPE_bte: select_runlength(bte); break;
	case TYPE_sht: select_runlength(sht); break;
	case TYPE_int: select_runlength(int); break;
	case TYPE_lng: select_runlength(lng); break;
	case TYPE_oid: select_runlength(oid); break;
	case TYPE_flt: select_runlength(flt); break;
	case TYPE_dbl: select_runlength(dbl); break;
#ifdef HAVE_HGE
	case TYPE_hge: select_runlength(hge); break;
#endif
	}
	MOSadvance_runlength(task);
	task->lb = o;
	return MAL_SUCCEED;
}

#define thetaselect_runlength(TPE)\
{ 	TPE low,hgh,*v;\
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
	v = (TPE*) (((char*)task->blk) + MosaicBlkSize);\
	if( (is_nil(TPE, low) || * v >= low) && (* v <= hgh || is_nil(TPE, hgh)) ){\
			if ( !anti) {\
				for( ; first < last; first++){\
					MOSskipit();\
					*o++ = (oid) first;\
				}\
			}\
	} else\
	if( anti)\
		for( ; first < last; first++){\
			MOSskipit();\
			*o++ = (oid) first;\
		}\
}

str
MOSthetaselect_runlength( MOStask task, void *val, str oper)
{
	oid *o;
	int anti=0;
	BUN first,last;

	// set the oid range covered and advance scan range
	first = task->start;
	last = first + MOSgetCnt(task->blk);

	if (task->cl && *task->cl > last){
		MOSskip_runlength(task);
		return MAL_SUCCEED;
	}
	o = task->lb;

	switch(ATOMbasetype(task->type)){
	case TYPE_bte: thetaselect_runlength(bte); break;
	case TYPE_sht: thetaselect_runlength(sht); break;
	case TYPE_int: thetaselect_runlength(int); break;
	case TYPE_lng: thetaselect_runlength(lng); break;
	case TYPE_oid: thetaselect_runlength(oid); break;
	case TYPE_flt: thetaselect_runlength(flt); break;
	case TYPE_dbl: thetaselect_runlength(dbl); break;
#ifdef HAVE_HGE
	case TYPE_hge: thetaselect_runlength(hge); break;
#endif
	}
	MOSskip_runlength(task);
	task->lb =o;
	return MAL_SUCCEED;
}

#define projection_runlength(TPE)\
{	TPE val, *v;\
	v= (TPE*) task->src;\
	val = *(TPE*) (((char*) task->blk) + MosaicBlkSize);\
	for(; first < last; first++){\
		MOSskipit();\
		*v++ = val;\
		task->cnt++;\
	}\
	task->src = (char*) v;\
}

str
MOSprojection_runlength( MOStask task)
{
	BUN first,last;

	// set the oid range covered and advance scan range
	first = task->start;
	last = first + MOSgetCnt(task->blk);

	switch(ATOMbasetype(task->type)){
		case TYPE_bte: projection_runlength(bte); break;
		case TYPE_sht: projection_runlength(sht); break;
		case TYPE_int: projection_runlength(int); break;
		case TYPE_lng: projection_runlength(lng); break;
		case TYPE_oid: projection_runlength(oid); break;
		case TYPE_flt: projection_runlength(flt); break;
		case TYPE_dbl: projection_runlength(dbl); break;
#ifdef HAVE_HGE
		case TYPE_hge: projection_runlength(hge); break;
#endif
	}
	MOSskip_runlength(task);
	return MAL_SUCCEED;
}

#define join_runlength(TPE)\
{	TPE *v, *w;\
	v = (TPE*) (((char*) task->blk) + MosaicBlkSize);\
	w = (TPE*) task->src;\
	for(n = task->stop, o = 0; n -- > 0; w++,o++)\
	if ( *w == *v)\
		for(oo= (oid) first; oo < (oid) last; oo++){\
			if(BUNappend(task->lbat, &oo, false) != GDK_SUCCEED ||\
			BUNappend(task->rbat, &o, false) != GDK_SUCCEED )\
			throw(MAL,"mosaic.runlength",MAL_MALLOC_FAIL);\
		}\
}

str
MOSjoin_runlength( MOStask task)
{
	BUN n,first,last;
	oid o, oo;

	// set the oid range covered and advance scan range
	first = task->start;
	last = first + MOSgetCnt(task->blk);

	switch(ATOMbasetype(task->type)){
		case TYPE_bte: join_runlength(bte); break;
		case TYPE_sht: join_runlength(sht); break;
		case TYPE_int: join_runlength(int); break;
		case TYPE_lng: join_runlength(lng); break;
		case TYPE_oid: join_runlength(oid); break;
		case TYPE_flt: join_runlength(flt); break;
		case TYPE_dbl: join_runlength(dbl); break;
#ifdef HAVE_HGE
		case TYPE_hge: join_runlength(hge); break;
#endif
	}
	MOSskip_runlength(task);
	return MAL_SUCCEED;
}
