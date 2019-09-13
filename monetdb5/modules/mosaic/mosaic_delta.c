/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0.  If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 *
 * Copyright 1997 - July 2008 CWI, August 2008 - 2018 MonetDB B.V.
 */

/*
 * 2014-2016 author Martin Kersten
 * Byte-wise delta encoding for SHT,INT,LNG, OID, WRD, STR-offsets, TIMESTAMP
 */

#include "monetdb_config.h"
#include "mosaic.h"
#include "mosaic_delta.h"
#include "mosaic_private.h"

//#define _DEBUG_MOSAIC_

bool MOStypes_delta(BAT* b) {
	switch(b->ttype) {
	case TYPE_sht: return true;
	case TYPE_int: return true;
	case TYPE_lng: return true;
	case TYPE_oid: return true;
	/* TODO: case TYPE_flt: return true; */
	/* TODO: case TYPE_dbl: return true; */
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
MOSadvance_delta(MOStask task)
{
	MosaicBlk blk = task->blk;

	task->start += MOSgetCnt(blk);
	task->stop = task->stop;
	switch(ATOMbasetype(task->type)){
	case TYPE_sht: task->blk = (MosaicBlk)( ((char*) blk)+ wordaligned(MosaicBlkSize + sizeof(sht) + MOSgetCnt(blk)-1,sht)); break ;
	case TYPE_int: task->blk = (MosaicBlk)( ((char*) blk)+ wordaligned(MosaicBlkSize + sizeof(int) + MOSgetCnt(blk)-1,int)); break ;
	case TYPE_oid: task->blk = (MosaicBlk)( ((char*) blk)+ wordaligned(MosaicBlkSize + sizeof(oid) + MOSgetCnt(blk)-1,oid)); break ;
	case TYPE_lng: task->blk = (MosaicBlk)( ((char*) blk)+ wordaligned(MosaicBlkSize + sizeof(lng) + MOSgetCnt(blk)-1,lng)); break ;
	//case TYPE_flt: case TYPE_dbl: to be looked into.
#ifdef HAVE_HGE
	case TYPE_hge: task->blk = (MosaicBlk)( ((char*) blk)+ wordaligned(MosaicBlkSize + sizeof(hge) + MOSgetCnt(blk)-1,hge)); break ;	
#endif
	}
}


void
MOSlayout_delta(MOStask task, BAT *btech, BAT *bcount, BAT *binput, BAT *boutput, BAT *bproperties)
{
	MosaicBlk blk = task->blk;
	lng cnt = MOSgetCnt(blk), input=0, output= 0;

	input = cnt * ATOMsize(task->type);
	switch(ATOMbasetype(task->type)){
	case TYPE_sht: output = wordaligned(MosaicBlkSize + sizeof(sht) + MOSgetCnt(blk)-1,sht); break ;
	case TYPE_int: output = wordaligned(MosaicBlkSize + sizeof(int) + MOSgetCnt(blk)-1,int); break ;
	case TYPE_oid: output = wordaligned(MosaicBlkSize + sizeof(oid) + MOSgetCnt(blk)-1,oid); break ;
	case TYPE_lng: output = wordaligned(MosaicBlkSize + sizeof(lng) + MOSgetCnt(blk)-1,lng); break ;
	//case TYPE_flt: case TYPE_dbl: to be looked into.
#ifdef HAVE_HGE
	case TYPE_hge: output = wordaligned(MosaicBlkSize + sizeof(hge) + MOSgetCnt(blk)-1,hge); break ;
#endif
	}
	if( BUNappend(btech, "delta", false) != GDK_SUCCEED ||
		BUNappend(bcount, &cnt, false) != GDK_SUCCEED ||
		BUNappend(binput, &input, false) != GDK_SUCCEED ||
		BUNappend(boutput, &output, false) != GDK_SUCCEED ||
		BUNappend(bproperties, "", false) != GDK_SUCCEED)
		return;
}

void
MOSskip_delta(MOStask task)
{
	MOSadvance_delta(task);
	if ( MOSgetTag(task->blk) == MOSAIC_EOL)
		task->blk = 0; // ENDOFLIST
}


// append a series of values into the non-compressed block
#define Estimate_delta(TPE, EXPR)\
{	TPE *v = ((TPE*)task->src) + task->start, val= *v, delta = 0;\
	BUN limit = task->stop - task->start > MOSAICMAXCNT? MOSAICMAXCNT: task->stop-task->start;\
	for(v++,i =1; i<limit; i++,v++){\
		delta = *v -val;\
		if ( EXPR)\
			break;\
		val = *v;\
	}\
	if( i == 1 || i * sizeof(TPE) <= wordaligned(MosaicBlkSize + sizeof(TPE) + i-1,MosaicBlkRec))\
		return 0.0;\
	if( task->dst +  wordaligned(MosaicBlkSize + sizeof(int) + i-1,MosaicBlkRec) >=task->bsrc->tmosaic->base+ task->bsrc->tmosaic->size)\
		return 0.0;\
	factor = ((flt) i * sizeof(TPE))/ wordaligned(MosaicBlkSize + sizeof(TPE) + i-1,MosaicBlkRec);\
}

// estimate the compression level 
flt
MOSestimate_delta(MOStask task)
{	BUN i = 0;
	flt factor = 1.0;

	switch(ATOMbasetype(task->type)){
		//case TYPE_bte: case TYPE_bit: no compression achievable
		case TYPE_sht: Estimate_delta(sht,  (delta < -127 || delta >127)); break;
		case TYPE_int: Estimate_delta(int,  (delta < -127 || delta >127)); break;
		case TYPE_oid: Estimate_delta(oid,  (delta > 255)); break;
		case TYPE_lng: Estimate_delta(lng,  (delta < -127 || delta >127)); break;
	#ifdef HAVE_HGE
		case TYPE_hge: Estimate_delta(hge,  (delta < -127 || delta >127)); break;
	#endif
	}
	task->factor[MOSAIC_DELTA] = factor;
	task->range[MOSAIC_DELTA] = task->start + i;
	return factor;
}

#define DELTAcompress(TPE,EXPR)\
{	TPE *v = ((TPE*)task->src) + task->start, val= *v, delta =0;\
	BUN limit = task->stop - task->start > MOSAICMAXCNT? MOSAICMAXCNT:task->stop - task->start;\
	task->dst = MOScodevector(task); \
	*(TPE*)task->dst = val;\
	hdr->checksum.sum##TPE += *v;\
	task->dst += sizeof(TPE);\
	for(v++,i =1; i<limit; i++,v++){\
		delta = *v -val;\
		if ( EXPR )\
			break;\
		hdr->checksum.sum##TPE += *v;\
		*(bte*)task->dst++ = (bte) delta;\
		val = *v;\
	}\
	MOSsetCnt(blk,i);\
}

// rather expensive simple value non-compressed store
void
MOScompress_delta(MOStask task)
{
	MosaicHdr hdr = (MosaicHdr) task->hdr;
	MosaicBlk blk = (MosaicBlk) task->blk;
	BUN i = 0;

	MOSsetTag(blk,MOSAIC_DELTA);

	switch(ATOMbasetype(task->type)){
	//case TYPE_bte: case TYPE_bit: no compression achievable
	case TYPE_sht: DELTAcompress(sht,(delta < -127 || delta >127)); break;
	case TYPE_int: DELTAcompress(int,(delta < -127 || delta >127)); break;
	case TYPE_oid: DELTAcompress(oid,(delta > 255)); break;
	case TYPE_lng: DELTAcompress(lng,(delta < -127 || delta >127)); break;
#ifdef HAVE_HGE
	case TYPE_hge: DELTAcompress(hge,(delta < -127 || delta < -127 || delta >127)); break;
#endif
	}
}

// the inverse operator, extend the src
#define DELTAdecompress(TPE)\
{ 	TPE val;\
	BUN lim = MOSgetCnt(blk);\
	task->dst = MOScodevector(task);\
	val = *(TPE*)task->dst ;\
	task->dst += sizeof(TPE);\
	for(i = 0; i < lim; i++) {\
		hdr->checksum2.sum##TPE += val;\
		((TPE*)task->src)[i] = val;\
		val = val + *(bte*) (task->dst);\
		task->dst++;\
	}\
	task->src += i * sizeof(TPE);\
}

void
MOSdecompress_delta(MOStask task)
{
	MosaicHdr hdr = (MosaicHdr) task->hdr;
	MosaicBlk blk = (MosaicBlk) task->blk;
	BUN i;

	switch(ATOMbasetype(task->type)){
	//case TYPE_bte: case TYPE_bit: no compression achievable
	case TYPE_sht: DELTAdecompress(sht); break;
	case TYPE_int: DELTAdecompress(int); break;
	case TYPE_oid: DELTAdecompress(oid); break;
#ifdef HAVE_HGE
	case TYPE_hge: DELTAdecompress(hge); break;
#endif
	case TYPE_lng: DELTAdecompress(lng); break;
	}
}

// The remainder should provide the minimal algebraic framework
//  to apply the operator to a DELTA compressed chunk

	
#define select_delta(TPE) {\
		TPE val= * (TPE*) (((char*) task->blk) + MosaicBlkSize);\
		task->dst = MOScodevector(task)  + sizeof(TPE);\
		if( !*anti){\
			if( is_nil(TPE, *(TPE*) low) && is_nil(TPE, *(TPE*) hgh) ){\
				for( ; first < last; first++){\
					MOSskipit();\
					*o++ = (oid) first;\
				}\
			} else\
			if( is_nil(TPE, *(TPE*) low) ){\
				for( ; first < last; first++, val+= *(bte*)task->dst, task->dst++){\
					MOSskipit();\
					cmp  =  ((*hi && val <= * (TPE*)hgh ) || (!*hi && val < *(TPE*)hgh ));\
					if (cmp )\
						*o++ = (oid) first;\
				}\
			} else\
			if( is_nil(TPE, *(TPE*) hgh) ){\
				for( ; first < last; first++, val+= *(bte*)task->dst, task->dst++){\
					MOSskipit();\
					cmp  =  ((*li && val >= * (TPE*)low ) || (!*li && val > *(TPE*)low ));\
					if (cmp )\
						*o++ = (oid) first;\
				}\
			} else{\
				for( ; first < last; first++, val+= *(bte*)task->dst, task->dst++){\
					MOSskipit();\
					cmp  =  ((*hi && val <= * (TPE*)hgh ) || (!*hi && val < *(TPE*)hgh )) &&\
							((*li && val >= * (TPE*)low ) || (!*li && val > *(TPE*)low ));\
					if (cmp )\
						*o++ = (oid) first;\
				}\
			}\
		} else {\
			if( is_nil(TPE, *(TPE*) low) && is_nil(TPE, *(TPE*) hgh)){\
				/* nothing is matching */\
			} else\
			if( is_nil(TPE, *(TPE*) low) ){\
				for( ; first < last; first++, val+= *(bte*)task->dst, task->dst++){\
					MOSskipit();\
					cmp  =  ((*hi && val <= * (TPE*)hgh ) || (!*hi && val < *(TPE*)hgh ));\
					if ( !cmp )\
						*o++ = (oid) first;\
				}\
			} else\
			if( is_nil(TPE, *(TPE*) hgh) ){\
				for( ; first < last; first++, val+= *(bte*)task->dst, task->dst++){\
					MOSskipit();\
					cmp  =  ((*li && val >= * (TPE*)low ) || (!*li && val > *(TPE*)low ));\
					if ( !cmp )\
						*o++ = (oid) first;\
				}\
			} else{\
				for( ; first < last; first++, val+= *(bte*)task->dst, task->dst++){\
					MOSskipit();\
					cmp  =  ((*hi && val <= * (TPE*)hgh ) || (!*hi && val < *(TPE*)hgh )) &&\
							((*li && val >= * (TPE*)low ) || (!*li && val > *(TPE*)low ));\
					if ( !cmp )\
						*o++ = (oid) first;\
				}\
			}\
		}\
	}

str
MOSselect_delta( MOStask task, void *low, void *hgh, bit *li, bit *hi, bit *anti)
{
	oid *o;
	BUN first,last;
	int cmp;

	// set the oid range covered and advance scan range
	first = task->start;
	last = first + MOSgetCnt(task->blk);

	if (task->cl && *task->cl > last){
		MOSskip_delta(task);
		return MAL_SUCCEED;
	}
	o = task->lb;

	switch(ATOMbasetype(task->type)){
	case TYPE_sht: select_delta(sht); break;
	case TYPE_int: select_delta(int); break;
	case TYPE_lng: select_delta(lng); break;
	case TYPE_oid: select_delta(oid); break;
#ifdef HAVE_HGE
	case TYPE_hge: select_delta(hge); break;
#endif
	}
	MOSskip_delta(task);
	task->lb = o;
	return MAL_SUCCEED;
}

#define thetaselect_delta(TPE)\
{ 	TPE low,hgh, v;\
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
	v= *(TPE*) (((char*) task->blk) + MosaicBlkSize);\
	task->dst = MOScodevector(task) + sizeof(TPE);\
	for( ; first < last; first++, v+= *(bte*)task->dst, task->dst++){\
		if( (is_nil(TPE, low) || v >= low) && (v <= hgh || is_nil(TPE, hgh)) ){\
			if ( !anti) {\
				MOSskipit();\
				*o++ = (oid) first;\
			}\
		} else\
		if( anti){\
			MOSskipit();\
			*o++ = (oid) first;\
		}\
	}\
} 

str
MOSthetaselect_delta( MOStask task, void *val, str oper)
{
	oid *o;
	int anti=0;
	BUN first,last;
	
	// set the oid range covered and advance scan range
	first = task->start;
	last = first + MOSgetCnt(task->blk);

	if (task->cl && *task->cl > last){
		MOSskip_delta(task);
		return MAL_SUCCEED;
	}
	o = task->lb;

	switch(ATOMbasetype(task->type)){
	case TYPE_sht: thetaselect_delta(sht); break;
	case TYPE_int: thetaselect_delta(int); break;
	case TYPE_lng: thetaselect_delta(lng); break;
	case TYPE_oid: thetaselect_delta(oid); break;
#ifdef HAVE_HGE
	case TYPE_hge: thetaselect_delta(hge); break;
#endif
	}
	MOSskip_delta(task);
	task->lb =o;
	return MAL_SUCCEED;
}

#define projection_delta(TPE)\
{	TPE val, *v;\
	bte *delta;\
	v= (TPE*) task->src;\
	val = *(TPE*) (((char*) task->blk) + MosaicBlkSize);\
	delta = (bte*) (((char*)task->blk + MosaicBlkSize) + sizeof(TPE));\
	for(; first < last; first++, val+= *delta,delta++){\
		MOSskipit();\
		*v++ = val;\
		task->cnt++;\
	}\
	task->src = (char*) v;\
}

str
MOSprojection_delta( MOStask task)
{
	BUN first, last;

	// set the oid range covered and advance scan range
	first = task->start;
	last = first + MOSgetCnt(task->blk);

	switch(ATOMbasetype(task->type)){
		case TYPE_sht: projection_delta(sht); break;
		case TYPE_int: projection_delta(int); break;
		case TYPE_lng: projection_delta(lng); break;
		case TYPE_oid: projection_delta(oid); break;
#ifdef HAVE_HGE
		case TYPE_hge: projection_delta(hge); break;
#endif
	}
	MOSskip_delta(task);
	return MAL_SUCCEED;
}

#define join_delta(TPE)\
{	TPE *w,base;\
	bte *v;\
	base = *(TPE*) (((char*) task->blk) + MosaicBlkSize);\
	v = (bte*) (((char*) task->blk) + MosaicBlkSize + sizeof(TPE));\
	for(oo= (oid) first; first < last; first++, base += *v,v++, oo++){\
		w = (TPE*) task->src;\
		for(n = task->stop, o = 0; n -- > 0; w++,o++)\
		if ( *w == base){\
			if( BUNappend(task->lbat, &oo, false) != GDK_SUCCEED ||\
			BUNappend(task->rbat, &o, false) !=GDK_SUCCEED) \
			throw(MAL,"mosaic.delta",MAL_MALLOC_FAIL);\
		}\
	}\
}

str
MOSjoin_delta( MOStask task)
{
	BUN n, first, last;
	oid o, oo;

	// set the oid range covered and advance scan range
	first = task->start;
	last = first + MOSgetCnt(task->blk);

	switch(ATOMbasetype(task->type)){
		case TYPE_sht: join_delta(sht); break;
		case TYPE_int: join_delta(int); break;
		case TYPE_lng: join_delta(lng); break;
		case TYPE_oid: join_delta(oid); break;
#ifdef HAVE_HGE
		case TYPE_hge: join_delta(hge); break;
#endif
	}
	MOSskip_delta(task);
	return MAL_SUCCEED;
}
