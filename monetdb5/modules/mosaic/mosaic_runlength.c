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
 *                * Copyright August 2008-2014 MonetDB B.V.
 * All Rights Reserved.
 */

/*
 * 2014-2015 author Martin Kersten
 * Run-length encoding framework for a single chunk
 */

#include "monetdb_config.h"
#include "mosaic.h"
#include "mosaic_runlength.h"

/* Beware, the dump routines use the compressed part of the task */
void
MOSdump_runlength(Client cntxt, MOStask task)
{
	MosaicBlk blk= task->blk;
	void *val = (void*)(((char*) blk) + MosaicBlkSize);

	mnstr_printf(cntxt->fdout,"#rle "BUNFMT" ", MOSgetCnt(blk));
	switch(task->type){
	case TYPE_bte:
		mnstr_printf(cntxt->fdout,"bte %hhd", *(bte*) val); break;
	case TYPE_sht:
		mnstr_printf(cntxt->fdout,"sht %hd", *(sht*) val); break;
	case TYPE_int:
		mnstr_printf(cntxt->fdout,"int %d", *(int*) val); break;
	case  TYPE_oid:
		mnstr_printf(cntxt->fdout,"oid "OIDFMT, *(oid*) val); break;
	case  TYPE_lng:
		mnstr_printf(cntxt->fdout,"lng "LLFMT, *(lng*) val); break;
#ifdef HAVE_HGE
	case  TYPE_hge:
		mnstr_printf(cntxt->fdout,"hge %.40g", (dbl) *(hge*) val); break;
#endif
	case TYPE_flt:
		mnstr_printf(cntxt->fdout,"flt  %f", *(flt*) val); break;
	case TYPE_dbl:
		mnstr_printf(cntxt->fdout,"flt  %f", *(dbl*) val); break;
	case TYPE_str:
		mnstr_printf(cntxt->fdout,"str TBD"); break;
	default:
		if( task->type == TYPE_date)
			mnstr_printf(cntxt->fdout,"date %d ", *(int*) val); 
		if( task->type == TYPE_daytime)
			mnstr_printf(cntxt->fdout,"daytime %d ", *(int*) val);
		if( task->type == TYPE_timestamp)
			mnstr_printf(cntxt->fdout,"int "LLFMT, *(lng*) val); 
	}
	mnstr_printf(cntxt->fdout,"\n");
}

void
MOSlayout_runlength(Client cntxt, MOStask task, BAT *btech, BAT *bcount, BAT *binput, BAT *boutput, BAT *bproperties)
{
	MosaicBlk blk = task->blk;
	lng cnt = MOSgetCnt(blk), input=0, output= 0;
	(void) cntxt;

	BUNappend(btech, "runlength", FALSE);
	BUNappend(bcount, &cnt, FALSE);
	input = cnt * ATOMsize(task->type);
	switch(ATOMbasetype(task->type)){
	case TYPE_bte: output = wordaligned( MosaicBlkSize + sizeof(bte),bte); break;
	case TYPE_bit: output = wordaligned( MosaicBlkSize + sizeof(bit),bit); break;
	case TYPE_sht: output = wordaligned( MosaicBlkSize + sizeof(sht),sht); break;
	case TYPE_int: output = wordaligned( MosaicBlkSize + sizeof(int),int); break;
	case TYPE_lng: output = wordaligned( MosaicBlkSize + sizeof(lng),lng); break;
	case TYPE_oid: output = wordaligned( MosaicBlkSize + sizeof(oid),oid); break;
	case TYPE_flt: output = wordaligned( MosaicBlkSize + sizeof(flt),flt); break;
	case TYPE_dbl: output = wordaligned( MosaicBlkSize + sizeof(dbl),dbl); break;
#ifdef HAVE_HGE
	case TYPE_hge: output = wordaligned( MosaicBlkSize + sizeof(hge),hge); break;
#endif
	case  TYPE_str:
		// we only have to look at the index width, not the values
		switch(task->bsrc->twidth){
		case 1: output = wordaligned( MosaicBlkSize + sizeof(bte),bte); break;
		case 2: output = wordaligned( MosaicBlkSize + sizeof(sht),sht); break;
		case 4: output = wordaligned( MosaicBlkSize + sizeof(int),int); break;
		case 8: output = wordaligned( MosaicBlkSize + sizeof(lng),lng); break;
		}
		break;
	}
	BUNappend(binput, &input, FALSE);
	BUNappend(boutput, &output, FALSE);
	BUNappend(bproperties, "", FALSE);
}

void
MOSadvance_runlength(Client cntxt, MOStask task)
{
	(void) cntxt;

	task->start += MOSgetCnt(task->blk);
	//task->stop = task->elm;
	switch(ATOMbasetype(task->type)){
	case TYPE_bte: task->blk = (MosaicBlk)( ((char*)task->blk) + wordaligned( MosaicBlkSize + sizeof(bte),bte)); break;
	case TYPE_bit: task->blk = (MosaicBlk)( ((char*)task->blk) + wordaligned( MosaicBlkSize + sizeof(bit),bit)); break;
	case TYPE_sht: task->blk = (MosaicBlk)( ((char*)task->blk) + wordaligned( MosaicBlkSize + sizeof(sht),sht)); break;
	case TYPE_int: task->blk = (MosaicBlk)( ((char*)task->blk) + wordaligned( MosaicBlkSize + sizeof(int),int)); break;
	case TYPE_lng: task->blk = (MosaicBlk)( ((char*)task->blk) + wordaligned( MosaicBlkSize + sizeof(lng),lng)); break;
	case TYPE_oid: task->blk = (MosaicBlk)( ((char*)task->blk) + wordaligned( MosaicBlkSize + sizeof(oid),oid)); break;
	case TYPE_flt: task->blk = (MosaicBlk)( ((char*)task->blk) + wordaligned( MosaicBlkSize + sizeof(flt),flt)); break;
	case TYPE_dbl: task->blk = (MosaicBlk)( ((char*)task->blk) + wordaligned( MosaicBlkSize + sizeof(dbl),dbl)); break;
#ifdef HAVE_HGE
	case TYPE_hge: task->blk = (MosaicBlk)( ((char*)task->blk) + wordaligned( MosaicBlkSize + sizeof(hge),hge)); break;
#endif
	case  TYPE_str:
		// we only have to look at the index width, not the values
		switch(task->bsrc->twidth){
		case 1: task->blk = (MosaicBlk)( ((char*)task->blk) + wordaligned( MosaicBlkSize + sizeof(bte),bte)); break;
		case 2: task->blk = (MosaicBlk)( ((char*)task->blk) + wordaligned( MosaicBlkSize + sizeof(sht),sht)); break;
		case 4: task->blk = (MosaicBlk)( ((char*)task->blk) + wordaligned( MosaicBlkSize + sizeof(int),int)); break;
		case 8: task->blk = (MosaicBlk)( ((char*)task->blk) + wordaligned( MosaicBlkSize + sizeof(lng),lng)); break;
		}
		break;
	}
}

void
MOSskip_runlength(Client cntxt, MOStask task)
{
	MOSadvance_runlength(cntxt, task);
	if ( MOSgetTag(task->blk) == MOSAIC_EOL)
		task->blk = 0; // ENDOFLIST
}

#define Estimate(TYPE)\
{	TYPE *v = ((TYPE*) task->src) + task->start, val = *v;\
	BUN limit = task->stop - task->start > MOSlimit()? MOSlimit(): task->stop - task->start;\
	if( task->range[MOSAIC_RLE] > task->start+1 ){\
		if( (i= task->range[MOSAIC_RLE] - task->start) * sizeof(TYPE) < wordaligned( MosaicBlkSize + sizeof(TYPE),TYPE) )return 0.0;\
		factor = ((flt) i * sizeof(TYPE))/ wordaligned(MosaicBlkSize + sizeof(TYPE),TYPE);\
		return factor;\
	}\
	for(v++,i = 1; i < limit; i++,v++)\
	if ( *v != val)\
		break;\
	if( i * sizeof(TYPE) <= wordaligned( MosaicBlkSize + sizeof(TYPE),TYPE) )\
		return 0.0;\
	if( task->dst +  wordaligned(MosaicBlkSize + sizeof(TYPE),sizeof(TYPE)) >= task->bsrc->tmosaic->base + task->bsrc->tmosaic->size)\
		return 0.0;\
	factor = ( (flt)i * sizeof(TYPE))/ wordaligned( MosaicBlkSize + sizeof(TYPE),TYPE);\
}

// calculate the expected reduction using RLE in terms of elements compressed
flt
MOSestimate_runlength(Client cntxt, MOStask task)
{	BUN i = 0;
	flt factor = 0.0;
	(void) cntxt;

	switch(ATOMbasetype(task->type)){
	case TYPE_bte: Estimate(bte); break;
	case TYPE_bit: Estimate(bit); break;
	case TYPE_sht: Estimate(sht); break;
	case TYPE_oid: Estimate(oid); break;
	case TYPE_lng: Estimate(lng); break;
	case TYPE_flt: Estimate(flt); break;
	case TYPE_dbl: Estimate(dbl); break;
#ifdef HAVE_HGE
	case TYPE_hge: Estimate(hge); break;
#endif
	case TYPE_int:
		{	int *v = ((int*)task->src)+ task->start, val = *v;
			BUN limit = task->stop - task->start > MOSlimit()? MOSlimit(): task->stop - task->start;
			if( task->range[MOSAIC_RLE] > task->start+1){
				if( (i= task->range[MOSAIC_RLE] - task->start) * sizeof(int) < wordaligned(MosaicBlkSize + sizeof(int),int))
					return 0.0;
				factor = ((flt) i * sizeof(int))/ wordaligned(MosaicBlkSize + sizeof(int),int);
				return factor;
			}
			for(v++,i =1; i<limit; i++, v++)
			if ( *v != val)
				break;
			if( i *sizeof(int) <= wordaligned( MosaicBlkSize + sizeof(int),int) )
				return 0.0;
			if( task->dst +  wordaligned(MosaicBlkSize + sizeof(int),sizeof(int)) >= task->bsrc->tmosaic->base + task->bsrc->tmosaic->size)
				return 0.0;
			factor = ( (flt)i * sizeof(int))/ wordaligned( MosaicBlkSize + sizeof(int),int);
		}
		break;
	case  TYPE_str:
		// we only have to look at the index width, not the values
		switch(task->bsrc->twidth){
		case 1: Estimate(bte); break;
		case 2: Estimate(sht); break;
		case 4: Estimate(int); break;
		case 8: Estimate(lng); break;
		}
	}
#ifdef _DEBUG_MOSAIC_
	mnstr_printf(cntxt->fdout,"#estimate rle "BUNFMT" elm %4.3f factor\n",i,factor);
#endif
	task->factor[MOSAIC_RLE] = factor;
	task->range[MOSAIC_RLE] = task->start + i;
	return factor;
}

// insert a series of values into the compressor block using rle.
#define RLEcompress(TYPE)\
{	TYPE *v = ((TYPE*) task->src)+task->start, val = *v;\
	TYPE *dst = (TYPE*) task->dst;\
	BUN limit = task->stop - task->start > MOSlimit() ? MOSlimit(): task->stop - task->start;\
	*dst = val;\
	for(v++, i =1; i<limit; i++,v++)\
	if ( *v != val)\
		break;\
	hdr->checksum.sum##TYPE += (TYPE) (i * val);\
	MOSsetCnt(blk, i);\
	task->dst +=  sizeof(TYPE);\
}

void
MOScompress_runlength(Client cntxt, MOStask task)
{
	BUN i ;
	MosaicHdr hdr  = task->hdr;
	MosaicBlk blk = task->blk;

	(void) cntxt;
	MOSsetTag(blk, MOSAIC_RLE);

	switch(ATOMbasetype(task->type)){
	case TYPE_bte: RLEcompress(bte); break;
	case TYPE_bit: RLEcompress(bit); break;
	case TYPE_sht: RLEcompress(sht); break;
	case TYPE_lng: RLEcompress(lng); break;
	case TYPE_oid: RLEcompress(oid); break;
	case TYPE_flt: RLEcompress(flt); break;
	case TYPE_dbl: RLEcompress(dbl); break;
#ifdef HAVE_HGE
	case TYPE_hge: RLEcompress(hge); break;
#endif
	case TYPE_int:
		{	int *v = ((int*) task->src)+task->start, val = *v;
			int *dst = (int*) task->dst;
			BUN limit = task->stop - task->start > MOSlimit() ? MOSlimit(): task->stop - task->start;
			*dst = val;
			for(v++,i = 1; i<limit; i++, v++)
			if ( *v != val)
				break;
			hdr->checksum.sumint += (int)(i * val);
			MOSsetCnt(blk,i);
			task->dst +=  sizeof(int);
		}
		break;
	case  TYPE_str:
		// we only have to look at the index width, not the values
		switch(task->bsrc->twidth){
		case 1: RLEcompress(bte); break;
		case 2: RLEcompress(sht); break;
		case 4: RLEcompress(int); break;
		case 8: RLEcompress(lng); break;
		}
	}
#ifdef _DEBUG_MOSAIC_
	MOSdump_runlength(cntxt, task);
#endif
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
MOSdecompress_runlength(Client cntxt, MOStask task)
{
	MosaicHdr hdr = task->hdr;
	MosaicBlk blk =  ((MosaicBlk) task->blk);
	BUN i;
	char *compressed;
	(void) cntxt;

	compressed = (char*) blk + MosaicBlkSize;
	switch(ATOMbasetype(task->type)){
	case TYPE_bte: RLEdecompress(bte); break ;
	case TYPE_bit: RLEdecompress(bit); break ;
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
	case  TYPE_str:
		// we only have to look at the index width, not the values
		switch(task->bsrc->twidth){
		case 1: RLEdecompress(bte); break;
		case 2: RLEdecompress(sht); break;
		case 4: RLEdecompress(int); break;
		case 8: RLEdecompress(lng); break;
		}
	}
}

// perform relational algebra operators over non-compressed chunks
// They are bound by an oid range and possibly a candidate list

#define  subselect_runlength(TPE) \
{ 	TPE *val= (TPE*) (((char*) task->blk) + MosaicBlkSize);\
\
	if( !*anti){\
		if( *(TPE*) low == TPE##_nil && *(TPE*) hgh == TPE##_nil){\
			for( ; first < last; first++){\
				MOSskipit();\
				*o++ = (oid) first;\
			}\
		} else\
		if( *(TPE*) low == TPE##_nil ){\
			cmp  =  ((*hi && *(TPE*)val <= * (TPE*)hgh ) || (!*hi && *(TPE*)val < *(TPE*)hgh ));\
			if (cmp )\
			for( ; first < last; first++){\
				MOSskipit();\
				*o++ = (oid) first;\
			}\
		} else\
		if( *(TPE*) hgh == TPE##_nil ){\
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
		if( *(TPE*) low == TPE##_nil && *(TPE*) hgh == TPE##_nil){\
			/* nothing is matching */\
		} else\
		if( *(TPE*) low == TPE##_nil ){\
			cmp  =  ((*hi && *(TPE*)val <= * (TPE*)hgh ) || (!*hi && *(TPE*)val < *(TPE*)hgh ));\
			if ( !cmp )\
			for( ; first < last; first++){\
				MOSskipit();\
				*o++ = (oid) first;\
			}\
		} else\
		if( *(TPE*) hgh == TPE##_nil ){\
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
MOSsubselect_runlength(Client cntxt,  MOStask task, void *low, void *hgh, bit *li, bit *hi, bit *anti){
	oid *o;
	int cmp;
	BUN first,last;
	(void) cntxt;

	// set the oid range covered
	first = task->start;
	last = first + MOSgetCnt(task->blk);

	if (task->cl && *task->cl > last){
		MOSadvance_runlength(cntxt,task);
		return MAL_SUCCEED;
	}
	o = task->lb;

	switch(ATOMbasetype(task->type)){
	case TYPE_bit: subselect_runlength(bit); break;
	case TYPE_bte: subselect_runlength(bte); break;
	case TYPE_sht: subselect_runlength(sht); break;
	case TYPE_lng: subselect_runlength(lng); break;
	case TYPE_oid: subselect_runlength(oid); break;
	case TYPE_flt: subselect_runlength(flt); break;
	case TYPE_dbl: subselect_runlength(dbl); break;
#ifdef HAVE_HGE
	case TYPE_hge: subselect_runlength(hge); break;
#endif
	case TYPE_int:
	// Expanded MOSselect_runlength for debugging
	{ 	int *val= (int*) (((char*) task->blk) + MosaicBlkSize);

		if( !*anti){
			if( *(int*) low == int_nil && *(int*) hgh == int_nil){
				for( ; first < last; first++){
					MOSskipit();
					*o++ = (oid) first;
				}
			} else
			if( *(int*) low == int_nil ){
				cmp  =  ((*hi && *(int*)val <= * (int*)hgh ) || (!*hi && *(int*)val < *(int*)hgh ));
				if (cmp )
				for( ; first < last; first++){
					MOSskipit();
					*o++ = (oid) first;
				}
			} else
			if( *(int*) hgh == int_nil ){
				cmp  =  ((*li && *(int*)val >= * (int*)low ) || (!*li && *(int*)val > *(int*)low ));
				if (cmp )
				for( ; first < last; first++){
					MOSskipit();
					*o++ = (oid) first;
				}
			} else{
				cmp  =  ((*hi && *(int*)val <= * (int*)hgh ) || (!*hi && *(int*)val < *(int*)hgh )) &&
						((*li && *(int*)val >= * (int*)low ) || (!*li && *(int*)val > *(int*)low ));
				if (cmp )
				for( ; first < last; first++){
					MOSskipit();
					*o++ = (oid) first;
				}
			}
		} else {
			if( *(int*) low == int_nil && *(int*) hgh == int_nil){
				/* nothing is matching */
			} else
			if( *(int*) low == int_nil ){
				cmp  =  ((*hi && *(int*)val <= * (int*)hgh ) || (!*hi && *(int*)val < *(int*)hgh ));
				if ( !cmp )
				for( ; first < last; first++){
					MOSskipit();
					*o++ = (oid) first;
				}
			} else
			if( *(int*) hgh == int_nil ){
				cmp  =  ((*li && *(int*)val >= * (int*)low ) || (!*li && *(int*)val > *(int*)low ));
				if ( !cmp )
				for( ; first < last; first++){
					MOSskipit();
					*o++ = (oid) first;
				}
			} else{
				cmp  =  ((*hi && *(int*)val <= * (int*)hgh ) || (!*hi && *(int*)val < *(int*)hgh )) &&
						((*li && *(int*)val >= * (int*)low ) || (!*li && *(int*)val > *(int*)low ));
				if (!cmp)
				for( ; first < last; first++){
					MOSskipit();
					*o++ = (oid) first;
				}
			}
		}
	}
	break;
	case  TYPE_str:
		// we only have to look at the index width, not the values
		switch(task->bsrc->twidth){
		case 1: break;
		case 2: break;
		case 4: break;
		case 8: break;
		}
		break;
	default:
		if( task->type == TYPE_date)
			subselect_runlength(date); 
		if( task->type == TYPE_daytime)
			subselect_runlength(daytime); 
		if( task->type == TYPE_timestamp)
			subselect_runlength(lng); 
	}
	MOSadvance_runlength(cntxt,task);
	task->lb = o;
	return MAL_SUCCEED;
}

#define thetasubselect_runlength(TPE)\
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
	if( (low == TPE##_nil || * v >= low) && (* v <= hgh || hgh == TPE##_nil) ){\
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
MOSthetasubselect_runlength(Client cntxt,  MOStask task, void *val, str oper)
{
	oid *o;
	int anti=0;
	BUN first,last;
	(void) cntxt;
	
	// set the oid range covered and advance scan range
	first = task->start;
	last = first + MOSgetCnt(task->blk);

	if (task->cl && *task->cl > last){
		MOSskip_runlength(cntxt,task);
		return MAL_SUCCEED;
	}
	o = task->lb;

	switch(ATOMbasetype(task->type)){
	case TYPE_bit: thetasubselect_runlength(bit); break;
	case TYPE_bte: thetasubselect_runlength(bte); break;
	case TYPE_sht: thetasubselect_runlength(sht); break;
	case TYPE_lng: thetasubselect_runlength(lng); break;
	case TYPE_oid: thetasubselect_runlength(oid); break;
	case TYPE_flt: thetasubselect_runlength(flt); break;
	case TYPE_dbl: thetasubselect_runlength(dbl); break;
#ifdef HAVE_HGE
	case TYPE_hge: thetasubselect_runlength(hge); break;
#endif
	case TYPE_int:
		{ 	int low,hgh,*v;
			low= hgh = int_nil;
			if ( strcmp(oper,"<") == 0){
				hgh= *(int*) val;
				hgh = PREVVALUEint(hgh);
			} else
			if ( strcmp(oper,"<=") == 0){
				hgh= *(int*) val;
			} else
			if ( strcmp(oper,">") == 0){
				low = *(int*) val;
				low = NEXTVALUEint(low);
			} else
			if ( strcmp(oper,">=") == 0){
				low = *(int*) val;
			} else
			if ( strcmp(oper,"!=") == 0){
				low = hgh = *(int*) val;
				anti++;
			} else
			if ( strcmp(oper,"==") == 0){
				hgh= low= *(int*) val;
			} 
			v = (int*) (((char*)task->blk) + MosaicBlkSize);
			if( (low == int_nil || * v >= low) && (* v <= hgh || hgh == int_nil) ){
					if ( !anti) {
						for( ; first < last; first++){
							MOSskipit();
							*o++ = (oid) first;
						}
					}
			} else
			if( anti)
				for( ; first < last; first++){
					MOSskipit();
					*o++ = (oid) first;
				}
		}
		break;
	case  TYPE_str:
		// we only have to look at the index width, not the values
		switch(task->bsrc->twidth){
		case 1: break;
		case 2: break;
		case 4: break;
		case 8: break;
		}
		break;
	}
	MOSskip_runlength(cntxt,task);
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
		task->n--;\
		task->cnt++;\
	}\
	task->src = (char*) v;\
}

str
MOSprojection_runlength(Client cntxt,  MOStask task)
{
	BUN first,last;
	(void) cntxt;

	// set the oid range covered and advance scan range
	first = task->start;
	last = first + MOSgetCnt(task->blk);

	switch(ATOMbasetype(task->type)){
		case TYPE_bit: projection_runlength(bit); break;
		case TYPE_bte: projection_runlength(bte); break;
		case TYPE_sht: projection_runlength(sht); break;
		case TYPE_lng: projection_runlength(lng); break;
		case TYPE_oid: projection_runlength(oid); break;
		case TYPE_flt: projection_runlength(flt); break;
		case TYPE_dbl: projection_runlength(dbl); break;
#ifdef HAVE_HGE
		case TYPE_hge: projection_runlength(hge); break;
#endif
		case TYPE_int:
		{	int val, *v;
			v= (int*) task->src;
			val = *(int*) (((char*) task->blk) + MosaicBlkSize);
			for(; first < last; first++){
				MOSskipit();
				*v++ = val;
				task->n--;
			}
			task->src = (char*) v;
			task->cnt++;
		}
		break;
	case  TYPE_str:
		// we only have to look at the index width, not the values
		switch(task->bsrc->twidth){
		case 1: projection_runlength(bte); break;
		case 2: projection_runlength(sht); break;
		case 4: projection_runlength(int); break;
		case 8: projection_runlength(lng); break;
		}
		break;
	}
	MOSskip_runlength(cntxt,task);
	return MAL_SUCCEED;
}

#define join_runlength(TPE)\
{	TPE *v, *w;\
	v = (TPE*) (((char*) task->blk) + MosaicBlkSize);\
	w = (TPE*) task->src;\
	for(n = task->elm, o = 0; n -- > 0; w++,o++)\
	if ( *w == *v)\
		for(oo= (oid) first; oo < (oid) last; v++, oo++){\
			BUNappend(task->lbat, &oo, FALSE);\
			BUNappend(task->rbat, &o, FALSE);\
		}\
}

str
MOSsubjoin_runlength(Client cntxt,  MOStask task)
{
	BUN n,first,last;
	oid o, oo;
	(void) cntxt;

	// set the oid range covered and advance scan range
	first = task->start;
	last = first + MOSgetCnt(task->blk);

	switch(ATOMbasetype(task->type)){
		case TYPE_bit: join_runlength(bit); break;
		case TYPE_bte: join_runlength(bte); break;
		case TYPE_sht: join_runlength(sht); break;
		case TYPE_lng: join_runlength(lng); break;
		case TYPE_oid: join_runlength(oid); break;
		case TYPE_flt: join_runlength(flt); break;
		case TYPE_dbl: join_runlength(dbl); break;
#ifdef HAVE_HGE
		case TYPE_hge: join_runlength(hge); break;
#endif
		case TYPE_int:
			{	int *v, *w;
				v = (int*) (((char*) task->blk) + MosaicBlkSize);
				w = (int*) task->src;
				for(n = task->elm, o = 0; n -- > 0; w++,o++)
				if ( *w == *v)
					for(oo= (oid) first; oo < (oid) last; v++, oo++){
						BUNappend(task->lbat, &oo, FALSE);
						BUNappend(task->rbat, &o, FALSE);
					}
			}
			break;
		case  TYPE_str:
		// we only have to look at the index width, not the values
		switch(task->bsrc->twidth){
		case 1: break;
		case 2: break;
		case 4: break;
		case 8: break;
		}
			break;
	}
	MOSskip_runlength(cntxt,task);
	return MAL_SUCCEED;
}
