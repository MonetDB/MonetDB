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
 * (c)2014 author Martin Kersten
 * Run-length encoding framework for a single chunk
 */

#include "monetdb_config.h"
#include "mosaic.h"
#include "mosaic_rle.h"

/* Beware, the dump routines use the compressed part of the task */
void
MOSdump_rle(Client cntxt, MOStask task)
{
	MosaicBlk blk= task->blk;
	void *val = (void*)(((char*) blk) + MosaicBlkSize);

	mnstr_printf(cntxt->fdout,"#rle "BUNFMT" ", MOScnt(blk));
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
	case  TYPE_wrd:
		mnstr_printf(cntxt->fdout,"wrd "SZFMT, *(wrd*) val); break;
	case TYPE_flt:
		mnstr_printf(cntxt->fdout,"flt  %f", *(flt*) val); break;
	case TYPE_dbl:
		mnstr_printf(cntxt->fdout,"flt  %f", *(dbl*) val); break;
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
MOSadvance_rle(Client cntxt, MOStask task)
{
	(void) cntxt;

	task->start += MOScnt(task->blk);
	switch(task->type){
	case TYPE_bte: task->blk = (MosaicBlk)( ((char*)task->blk) + MosaicBlkSize + wordaligned(sizeof(bte),bte)); break;
	case TYPE_bit: task->blk = (MosaicBlk)( ((char*)task->blk) + MosaicBlkSize + wordaligned(sizeof(bit),bit)); break;
	case TYPE_sht: task->blk = (MosaicBlk)( ((char*)task->blk) + MosaicBlkSize + wordaligned(sizeof(sht),sht)); break;
	case TYPE_int: task->blk = (MosaicBlk)( ((char*)task->blk) + MosaicBlkSize + wordaligned(sizeof(int),int)); break;
	case TYPE_oid: task->blk = (MosaicBlk)( ((char*)task->blk) + MosaicBlkSize + wordaligned(sizeof(oid),oid)); break;
	case TYPE_lng: task->blk = (MosaicBlk)( ((char*)task->blk) + MosaicBlkSize + wordaligned(sizeof(lng),lng)); break;
#ifdef HAVE_HGE
	case TYPE_hge: task->blk = (MosaicBlk)( ((char*)task->blk) + MosaicBlkSize + wordaligned(sizeof(hge),hge)); break;
#endif
	case TYPE_wrd: task->blk = (MosaicBlk)( ((char*)task->blk) + MosaicBlkSize + wordaligned(sizeof(wrd),wrd)); break;
	case TYPE_flt: task->blk = (MosaicBlk)( ((char*)task->blk) + MosaicBlkSize + wordaligned(sizeof(flt),flt)); break;
	case TYPE_dbl: task->blk = (MosaicBlk)( ((char*)task->blk) + MosaicBlkSize + wordaligned(sizeof(dbl),dbl)); break;
	default:
		if( task->type == TYPE_date)
			task->blk = (MosaicBlk)( ((char*)task->blk) + MosaicBlkSize + wordaligned(sizeof(date),date)); 
		if( task->type == TYPE_daytime)
			task->blk = (MosaicBlk)( ((char*)task->blk) + MosaicBlkSize + wordaligned(sizeof(daytime),daytime)); 
		if( task->type == TYPE_timestamp)
			task->blk = (MosaicBlk)( ((char*)task->blk) + MosaicBlkSize + wordaligned(sizeof(timestamp),timestamp)); 
	}
}

void
MOSskip_rle(Client cntxt, MOStask task)
{
	MOSadvance_rle(cntxt, task);
	if ( MOStag(task->blk) == MOSAIC_EOL)
		task->blk = 0; // ENDOFLIST
}

#define Estimate(TYPE)\
{	TYPE val = *(TYPE*) task->src;\
	for(i =1; i < task->elm; i++)\
	if ( ((TYPE*)task->src)[i] != val)\
		break;\
	if ( i > MOSlimit() ) i = MOSlimit();\
	factor = ( (flt)i * sizeof(TYPE))/ (MosaicBlkSize + sizeof(TYPE));\
}

// calculate the expected reduction using RLE in terms of elements compressed
flt
MOSestimate_rle(Client cntxt, MOStask task)
{	BUN i = -1;
	flt factor = 1.0;
	(void) cntxt;

	switch(ATOMstorage(task->type)){
	case TYPE_bte: Estimate(bte); break;
	case TYPE_bit: Estimate(bit); break;
	case TYPE_sht: Estimate(sht); break;
	case TYPE_oid: Estimate(oid); break;
	case TYPE_lng: Estimate(lng); break;
#ifdef HAVE_HGE
	case TYPE_hge: Estimate(hge); break;
#endif
	case TYPE_wrd: Estimate(wrd); break;
	case TYPE_flt: Estimate(flt); break;
	case TYPE_dbl: Estimate(dbl); break;
	case TYPE_int:
		{	int val = *(int*)task->src;
			for(i =1; i<task->elm; i++)
			if ( ((int*)task->src)[i] != val)
				break;
			if ( i > MOSlimit() ) i = MOSlimit();
			factor = ( (flt)i * sizeof(int))/ (MosaicBlkSize + sizeof(int));
		}
	}
#ifdef _DEBUG_MOSAIC_
	mnstr_printf(cntxt->fdout,"#estimate rle "BUNFMT" elm %4.3f factor\n",i,factor);
#endif
	return factor;
}

// insert a series of values into the compressor block using rle.
#define RLEcompress(TYPE)\
{	TYPE val = *(TYPE*) task->src;\
	TYPE *dst = (TYPE*) task->dst;\
	BUN limit = task->elm > MOSlimit()? MOSlimit(): task->elm;\
	*dst = val;\
	for(i =1; i<limit; i++)\
	if ( ((TYPE*)task->src)[i] != val)\
		break;\
	MOSinc(blk,i);\
	task->dst +=  sizeof(TYPE);\
	task->src += i * sizeof(TYPE);\
}

void
MOScompress_rle(Client cntxt, MOStask task)
{
	BUN i ;
	MosaicBlk blk = task->blk;

	(void) cntxt;
	*blk = MOSrle;

	switch(ATOMstorage(task->type)){
	case TYPE_bte: RLEcompress(bte); break;
	case TYPE_bit: RLEcompress(bit); break;
	case TYPE_sht: RLEcompress(sht); break;
	case TYPE_oid: RLEcompress(oid); break;
	case TYPE_lng: RLEcompress(lng); break;
#ifdef HAVE_HGE
	case TYPE_hge: RLEcompress(hge); break;
#endif
	case TYPE_wrd: RLEcompress(wrd); break;
	case TYPE_flt: RLEcompress(flt); break;
	case TYPE_dbl: RLEcompress(dbl); break;
	case TYPE_int:
		{	int val = *(int*) task->src;
			int *dst = (int*) task->dst;
			BUN limit = task->elm > MOSlimit()? MOSlimit(): task->elm;
			*dst = val;
			for(i =1; i<limit; i++)
			if ( ((int*)task->src)[i] != val)
				break;
			MOSinc(blk,i);
			task->dst +=  sizeof(int);
			task->src += i * sizeof(int);
		}
		break;
	}
#ifdef _DEBUG_MOSAIC_
	MOSdump_rle(cntxt, task);
#endif
}

// the inverse operator, extend the src
#define RLEdecompress(TYPE)\
{	TYPE val = *(TYPE*) task->dst;\
	BUN lim = MOScnt(blk);\
	for(i = 0; i < lim; i++)\
		((TYPE*)task->src)[i] = val;\
	task->src += i * sizeof(TYPE);\
}

void
MOSdecompress_rle(Client cntxt, MOStask task)
{
	MosaicBlk blk =  ((MosaicBlk) task->blk);
	BUN i;
	char *compressed;
	(void) cntxt;

	compressed = (char*) blk + MosaicBlkSize;
	switch(ATOMstorage(task->type)){
	case TYPE_bte: RLEdecompress(bte); break ;
	case TYPE_bit: RLEdecompress(bit); break ;
	case TYPE_sht: RLEdecompress(sht); break;
	case TYPE_oid: RLEdecompress(oid); break;
	case TYPE_lng: RLEdecompress(lng); break;
#ifdef HAVE_HGE
	case TYPE_hge: RLEdecompress(hge); break;
#endif
	case TYPE_wrd: RLEdecompress(wrd); break;
	case TYPE_flt: RLEdecompress(flt); break;
	case TYPE_dbl: RLEdecompress(dbl); break;
	case TYPE_int:
		{	int val = *(int*) compressed ;
			BUN lim= MOScnt(blk);
			for(i = 0; i < lim; i++)
				((int*)task->src)[i] = val;
			task->src += i * sizeof(int);
		}
	}
}

// perform relational algebra operators over non-compressed chunks
// They are bound by an oid range and possibly a candidate list

#define  subselect_rle(TPE) \
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
MOSsubselect_rle(Client cntxt,  MOStask task, void *low, void *hgh, bit *li, bit *hi, bit *anti){
	oid *o;
	int cmp;
	BUN first,last;
	(void) cntxt;

	// set the oid range covered
	first = task->start;
	last = first + MOScnt(task->blk);

	if (task->cl && *task->cl > last){
		MOSadvance_rle(cntxt,task);
		return MAL_SUCCEED;
	}
	o = task->lb;

	switch(ATOMstorage(task->type)){
	case TYPE_bit: subselect_rle(bit); break;
	case TYPE_bte: subselect_rle(bte); break;
	case TYPE_sht: subselect_rle(sht); break;
	case TYPE_oid: subselect_rle(oid); break;
	case TYPE_lng: subselect_rle(lng); break;
#ifdef HAVE_HGE
	case TYPE_hge: subselect_rle(hge); break;
#endif
	case TYPE_wrd: subselect_rle(wrd); break;
	case TYPE_flt: subselect_rle(flt); break;
	case TYPE_dbl: subselect_rle(dbl); break;
	case TYPE_int:
	// Expanded MOSselect_rle for debugging
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
	default:
		if( task->type == TYPE_date)
			subselect_rle(date); 
		if( task->type == TYPE_daytime)
			subselect_rle(daytime); 
		if( task->type == TYPE_timestamp)
			subselect_rle(lng); 
	}
	MOSadvance_rle(cntxt,task);
	task->lb = o;
	return MAL_SUCCEED;
}

#define thetasubselect_rle(TPE)\
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
MOSthetasubselect_rle(Client cntxt,  MOStask task, void *val, str oper)
{
	oid *o;
	int anti=0;
	BUN first,last;
	(void) cntxt;
	
	// set the oid range covered and advance scan range
	first = task->start;
	last = first + MOScnt(task->blk);

	if (task->cl && *task->cl > last){
		MOSskip_rle(cntxt,task);
		return MAL_SUCCEED;
	}
	o = task->lb;

	switch(task->type){
	case TYPE_bit: thetasubselect_rle(bit); break;
	case TYPE_bte: thetasubselect_rle(bte); break;
	case TYPE_sht: thetasubselect_rle(sht); break;
	case TYPE_oid: thetasubselect_rle(oid); break;
	case TYPE_lng: thetasubselect_rle(lng); break;
#ifdef HAVE_HGE
	case TYPE_hge: thetasubselect_rle(hge); break;
#endif
	case TYPE_wrd: thetasubselect_rle(wrd); break;
	case TYPE_flt: thetasubselect_rle(flt); break;
	case TYPE_dbl: thetasubselect_rle(dbl); break;
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
	default:
		if( task->type == TYPE_date)
			thetasubselect_rle(date); 
		if( task->type == TYPE_daytime)
			thetasubselect_rle(daytime); 
		if( task->type == TYPE_timestamp)
			thetasubselect_rle(lng); 
	}
	MOSskip_rle(cntxt,task);
	task->lb =o;
	return MAL_SUCCEED;
}

#define leftfetchjoin_rle(TPE)\
{	TPE val, *v;\
	v= (TPE*) task->src;\
	val = *(TPE*) (((char*) task->blk) + MosaicBlkSize);\
	for(; first < last; first++){\
		MOSskipit();\
		*v++ = val;\
		task->n--;\
	}\
	task->src = (char*) v;\
}

str
MOSleftfetchjoin_rle(Client cntxt,  MOStask task)
{
	BUN first,last;
	(void) cntxt;

	// set the oid range covered and advance scan range
	first = task->start;
	last = first + MOScnt(task->blk);

	switch(task->type){
		case TYPE_bit: leftfetchjoin_rle(bit); break;
		case TYPE_bte: leftfetchjoin_rle(bte); break;
		case TYPE_sht: leftfetchjoin_rle(sht); break;
		case TYPE_oid: leftfetchjoin_rle(oid); break;
		case TYPE_lng: leftfetchjoin_rle(lng); break;
#ifdef HAVE_HGE
		case TYPE_hge: leftfetchjoin_rle(hge); break;
#endif
		case TYPE_wrd: leftfetchjoin_rle(wrd); break;
		case TYPE_flt: leftfetchjoin_rle(flt); break;
		case TYPE_dbl: leftfetchjoin_rle(dbl); break;
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
		}
		break;
		default:
			if( task->type == TYPE_timestamp)
			{	timestamp val, *v;
				v= (timestamp*) task->src;
				val = *(timestamp*) (((char*) task->blk) + MosaicBlkSize);
				for(; first < last; first++){
					MOSskipit();
					*v++ = val;
					task->n--;
				}
				task->src = (char*) v;
			}
	}
	MOSskip_rle(cntxt,task);
	return MAL_SUCCEED;
}

#define join_rle(TPE)\
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
MOSjoin_rle(Client cntxt,  MOStask task)
{
	BUN n,first,last;
	oid o, oo;
	(void) cntxt;

	// set the oid range covered and advance scan range
	first = task->start;
	last = first + MOScnt(task->blk);

	switch(task->type){
		case TYPE_bit: join_rle(bit); break;
		case TYPE_bte: join_rle(bte); break;
		case TYPE_sht: join_rle(sht); break;
		case TYPE_oid: join_rle(oid); break;
		case TYPE_lng: join_rle(lng); break;
#ifdef HAVE_HGE
		case TYPE_hge: join_rle(hge); break;
#endif
		case TYPE_wrd: join_rle(wrd); break;
		case TYPE_flt: join_rle(flt); break;
		case TYPE_dbl: join_rle(dbl); break;
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
		default:
			if( task->type == TYPE_timestamp){
			{	timestamp *v, *w;
				v = (timestamp*) (((char*) task->blk) + MosaicBlkSize);
				w = (timestamp*) task->src;
				for(n = task->elm, o = 0; n -- > 0; w++,o++)
				if ( w->days == v->days && w->msecs == v->msecs)
					for(oo= (oid) first; oo < (oid) last; v++, oo++){
						BUNappend(task->lbat, &oo, FALSE);
						BUNappend(task->rbat, &o, FALSE);
					}
			}
			}
	}
	MOSskip_rle(cntxt,task);
	return MAL_SUCCEED;
}
