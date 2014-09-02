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
 * Local variance dictionary encoding based on 8-bits.
 * The complete variance values are recorded. 
 * The first value in the dictionary contains the base, the rest are deltas.
 */

#include "monetdb_config.h"
#include "mosaic.h"
#include "mosaic_dictionary.h"
#include "mosaic_variance.h"

void
MOSadvance_variance(Client cntxt, MOStask task)
{
	(void) cntxt;

	task->start += MOScnt(task->blk);
	switch(task->type){
	case TYPE_sht: task->blk = (MosaicBlk)( ((char*)task->blk) + 2* MosaicBlkSize + dictsize * sizeof(sht)+ wordaligned(sizeof(bte) * MOScnt(task->blk),sht)); break;
	case TYPE_int: task->blk = (MosaicBlk)( ((char*)task->blk) + 2* MosaicBlkSize + dictsize * sizeof(int)+ wordaligned(sizeof(bte) * MOScnt(task->blk),int)); break;
	case TYPE_oid: task->blk = (MosaicBlk)( ((char*)task->blk) + 2* MosaicBlkSize + dictsize * sizeof(oid)+ wordaligned(sizeof(bte) * MOScnt(task->blk),oid)); break;
	case TYPE_lng: task->blk = (MosaicBlk)( ((char*)task->blk) + 2* MosaicBlkSize + dictsize * sizeof(lng)+ wordaligned(sizeof(bte) * MOScnt(task->blk),lng)); break;
	case TYPE_wrd: task->blk = (MosaicBlk)( ((char*)task->blk) + 2* MosaicBlkSize + dictsize * sizeof(wrd)+ wordaligned(sizeof(bte) * MOScnt(task->blk),wrd)); break;
	case TYPE_flt: task->blk = (MosaicBlk)( ((char*)task->blk) + 2* MosaicBlkSize + dictsize * sizeof(flt)+ wordaligned(sizeof(bte) * MOScnt(task->blk),flt)); break;
	case TYPE_dbl: task->blk = (MosaicBlk)( ((char*)task->blk) + 2* MosaicBlkSize + dictsize * sizeof(dbl)+ wordaligned(sizeof(bte) * MOScnt(task->blk),dbl)); break;
#ifdef HAVE_HGE
	case TYPE_hge: task->blk = (MosaicBlk)( ((char*)task->blk) + 2* MosaicBlkSize + dictsize * sizeof(hge)+ wordaligned(sizeof(bte) * MOScnt(task->blk),hge)); break;
#endif
	default:
		if( task->type == TYPE_timestamp)
				task->blk = (MosaicBlk)( ((char*)task->blk) + 2* MosaicBlkSize + dictsize * sizeof(timestamp)+ wordaligned(sizeof(bte) * MOScnt(task->blk),timestamp)); 
		if( task->type == TYPE_date)
				task->blk = (MosaicBlk)( ((char*)task->blk) + 2* MosaicBlkSize + dictsize * sizeof(date)+ wordaligned(sizeof(bte) * MOScnt(task->blk),date)); 
		if( task->type == TYPE_daytime)
				task->blk = (MosaicBlk)( ((char*)task->blk) + 2* MosaicBlkSize + dictsize * sizeof(date)+ wordaligned(sizeof(bte) * MOScnt(task->blk),daytime)); 
	}
}

/* Beware, the dump routines use the compressed part of the task */
void
MOSdump_variance(Client cntxt, MOStask task)
{
	MosaicBlk blk= task->blk;
	int i;
	lng *size;
	void *val = (void*)(((char*) blk) + MosaicBlkSize);

	size = (lng*) (((char*)blk) + MosaicBlkSize);
	mnstr_printf(cntxt->fdout,"#dict " BUNFMT" ", MOScnt(blk));
	switch(task->type){
	case TYPE_sht:
		for(i=0; i< *size; i++)
		mnstr_printf(cntxt->fdout,"sht [%d] %hd",i, ((sht*) val)[i]); break;
	case TYPE_int:
		for(i=0; i< *size; i++)
		mnstr_printf(cntxt->fdout,"int [%d] %d",i, ((int*) val)[i]); break;
	case  TYPE_oid:
		for(i=0; i< *size; i++)
		mnstr_printf(cntxt->fdout,"oid [%d] "OIDFMT, i, ((oid*) val)[i]); break;
	case  TYPE_lng:
		for(i=0; i< *size; i++)
		mnstr_printf(cntxt->fdout,"lng [%d] "LLFMT, i, ((lng*) val)[i]); break;
	case  TYPE_wrd:
		for(i=0; i< *size; i++)
		mnstr_printf(cntxt->fdout,"wrd [%d] "SZFMT, i, ((wrd*) val)[i]); break;
	case  TYPE_flt:
		for(i=0; i< *size; i++)
		mnstr_printf(cntxt->fdout,"flt [%d] %f", i, ((flt*) val)[i]); break;
	case  TYPE_dbl:
		for(i=0; i< *size; i++)
		mnstr_printf(cntxt->fdout,"dbl [%d] %g", i, ((dbl*) val)[i]); break;
#ifdef HAVE_HGE
	case  TYPE_hge:
		for(i=0; i< *size; i++)
		mnstr_printf(cntxt->fdout,"hge [%d] %.40g", i, (dbl) ((hge*) val)[i]); break;
#endif
	default:
		if( task->type == TYPE_date){
		}
		if( task->type == TYPE_daytime){
		}
		if( task->type == TYPE_timestamp){
		}
	}
	mnstr_printf(cntxt->fdout,"\n");
}

void
MOSskip_variance(Client cntxt, MOStask task)
{
	MOSadvance_variance(cntxt, task);
	if ( MOStag(task->blk) == MOSAIC_EOL)
		task->blk = 0; // ENDOFLIST
}

#define estimateDict(TPE)\
{	TPE *v = (TPE*)task->src, val = *v++, delta;\
	TPE *dict = (TPE*)((char*)task->dst + 2 * MosaicBlkSize);\
	task->dst = ((char*) dict)+ sizeof(TPE)*dictsize;\
	dict[0]= val;\
	*size = *size+1;\
	for(i =0; i<task->elm; i++, val++){\
		delta = *v-val;\
		val = *v;\
		for(j= 0; j< *size; j++)\
			if( dict[j] == delta) {cnt++;break;}\
		if ( j == *size){\
			if ( *size == dictsize)\
				break;\
			dict[j] = delta;\
			*size= *size+1;\
			cnt++;\
		}\
	}\
	if(i) factor = (flt) ((int)i * sizeof(int)) / (2 * MosaicBlkSize + sizeof(int) * dictsize +i);\
}

// calculate the expected reduction using dictionary in terms of elements compressed
flt
MOSestimate_variance(Client cntxt, MOStask task)
{	BUN i = -1;
	int cnt =0,j;
	lng *size;
	flt factor= 1.0;
	(void) cntxt;

	// use the dst to avoid overwriting noneblocked
	size = (lng*) (((char*)task->dst) + MosaicBlkSize);
	*size = 0;
	switch(ATOMstorage(task->type)){
	case TYPE_sht: estimateDict(sht); break;
	case TYPE_oid: estimateDict(oid); break;
	case TYPE_lng: estimateDict(lng); break;
	case TYPE_wrd: estimateDict(wrd); break;
	case TYPE_flt: estimateDict(flt); break;
	case TYPE_dbl: estimateDict(dbl); break;
#ifdef HAVE_HGE
	case TYPE_hge: estimateDict(hge); break;
#endif
	case TYPE_int:
		{	int *v = (int*)task->src, val = *v++,delta;
			int *dict = (int*)((char*)task->dst + 2 * MosaicBlkSize);
			task->dst = ((char*) dict)+ sizeof(int)*dictsize;
			dict[0]= val;
			*size = *size+1;
			for(i =0; i<task->elm; i++, v++){
				delta = *v-val;
				val = *v;
				for(j= 0; j< *size; j++)
					if( dict[j] == delta) {cnt++;break;}
				if ( j == *size){
					if ( *size == dictsize)
						break;
					dict[j] = delta;
					*size= *size+1;
					cnt++;
				}
			}
			if(i) factor = (flt) ((int)i * sizeof(int)) / (2 * MosaicBlkSize + sizeof(int) * dictsize +i);
		}
	}
#ifdef _DEBUG_MOSAIC_
	mnstr_printf(cntxt->fdout,"#estimate dict "BUNFMT" elm %4.2f factor\n", i, factor);
#endif
	return factor; 
}

// insert a series of values into the compressor block using dictionary
#define VARDICTcompress(TPE)\
{	TPE *v = (TPE*)task->src, val=*v++, delta;\
	TPE *dict = (TPE*)((char*)task->blk+ 2 * MosaicBlkSize);\
	BUN limit = task->elm > MOSlimit()? MOSlimit(): task->elm;\
	task->dst = ((char*) dict)+ sizeof(TPE)*dictsize;\
	dict[0]= val;\
	*size = *size+1;\
	MOSinc(blk,1);\
	for(i =1; i<limit; i++, v++){\
		delta = *v - val;\
		val = *v;\
		for(j= 0; j< *size; j++)\
			if( dict[j] == delta) {\
				MOSinc(blk,1);\
				*task->dst++ = (char) j;\
				break;\
			}\
		if ( j == *size){\
			if ( *size == dictsize){\
				task->dst += wordaligned(MOScnt(blk) %2,TPE);\
				break;\
			}\
			MOSinc(blk,1);\
			dict[j] = delta;\
			*size = *size+1;\
			*task->dst++ = (char) j;\
		}\
	}\
	task->src = (char*) v;\
}

void
MOScompress_variance(Client cntxt, MOStask task)
{
	BUN i ;
	int j;
	lng *size;
	MosaicBlk blk = task->blk;

	(void) cntxt;
	size = (lng*) (((char*)blk) + MosaicBlkSize);
	*size = 0;
	*blk = MOSdict;

	switch(ATOMstorage(task->type)){
	case TYPE_sht: VARDICTcompress(sht); break;
	case TYPE_int: VARDICTcompress(int); break;
	case TYPE_oid: VARDICTcompress(oid); break;
	case TYPE_wrd: VARDICTcompress(wrd); break;
	case TYPE_flt: VARDICTcompress(flt); break;
	case TYPE_dbl: VARDICTcompress(dbl); break;
#ifdef HAVE_HGE
	case TYPE_hge: VARDICTcompress(hge); break;
#endif
	case TYPE_lng:
		{	lng *v = (lng*)task->src, val = *v++, delta;
			lng *dict = (lng*)((char*)task->blk+ 2 * MosaicBlkSize);
			BUN limit = task->elm > MOSlimit()? MOSlimit(): task->elm;
			task->dst = ((char*) dict)+ sizeof(lng)*dictsize;
			dict[0]= val;
			*size = *size+1;
			MOSinc(blk,1);
			for(i =1; i<limit; i++, v++){
				delta = *v - val;
				val = *v;
				for(j= 0; j< *size; j++)
					if( dict[j] == delta) {
						MOSinc(blk,1);
						*task->dst++ = (char) j;
						break;
					}
				if ( j == *size){
					if ( *size == dictsize){
						// align on word boundary
						task->dst += wordaligned(MOScnt(blk) %2,lng);
						break;
					}
					MOSinc(blk,1);
					dict[j] = delta;
					*size = *size+1;
					*task->dst++ = (char) j;
				}
			}
			task->src = (char*) v;
		}
		break;
	}
#ifdef _DEBUG_MOSAIC_
	MOSdump_variance(cntxt, task);
#endif
}

// the inverse operator, extend the src
#define VARDICTdecompress(TPE)\
{	bte *idx = (bte*)(compressed + dictsize * sizeof(TPE));\
	TPE *dict = (TPE*) compressed,val = dict[0];\
	BUN lim = MOScnt(blk);\
	((int*)task->src)[0] = val;\
	for(i = 1; i < lim; i++,idx++){\
		val += dict[ (bte)*idx];\
		((TPE*)task->src)[i] = val;\
	}\
	task->src += i * sizeof(TPE);\
}

void
MOSdecompress_variance(Client cntxt, MOStask task)
{
	MosaicBlk blk =  ((MosaicBlk) task->blk);
	BUN i;
	char *compressed;
	(void) cntxt;

	compressed = (char*) blk + 2 * MosaicBlkSize;
	switch(ATOMstorage(task->type)){
	case TYPE_sht: VARDICTdecompress(sht); break;
	case TYPE_oid: VARDICTdecompress(oid); break;
#ifdef HAVE_HGE
	case TYPE_hge: VARDICTdecompress(hge); break;
#endif
	case TYPE_wrd: VARDICTdecompress(wrd); break;
	case TYPE_int:
		{	bte *idx = (bte*)(compressed + dictsize * sizeof(int));
			int *dict = (int*) compressed,val= dict[0];
			BUN lim = MOScnt(blk);
			((int*)task->src)[0] = val;
			for(i = 1; i < lim; i++,idx++){
				val += dict[ (bte)*idx];
				((int*)task->src)[i] = val;
			}
			task->src += i * sizeof(int);
		}
	}
}

// perform relational algebra operators over non-compressed chunks
// They are bound by an oid range and possibly a candidate list

#define subselect_variance(TPE) {\
 	TPE *dict= (TPE*) (((char*) task->blk) + 2 * MosaicBlkSize ),val= dict[0];\
	bte *idx = (bte*) (((char*) task->blk) + 2 * MosaicBlkSize + dictsize * sizeof(TPE));\
	if( !*anti){\
		if( *(TPE*) low == TPE##_nil && *(TPE*) hgh == TPE##_nil){\
			for( ; first < last; first++, idx++){\
				MOSskipit();\
				*o++ = (oid) first;\
			}\
		} else\
		if( *(TPE*) low == TPE##_nil ){\
			for( ; first < last; first++, val += dict[*idx++]){\
				MOSskipit();\
				cmp  =  ((*hi && val <= * (TPE*)hgh ) || (!*hi && val < *(TPE*)hgh ));\
				if (cmp )\
					*o++ = (oid) first;\
			}\
		} else\
		if( *(TPE*) hgh == TPE##_nil ){\
			for( ; first < last; first++, val += dict[*idx++]){\
				MOSskipit();\
				cmp  =  ((*li && val >= * (TPE*)low ) || (!*li && val > *(TPE*)low ));\
				if (cmp )\
					*o++ = (oid) first;\
			}\
		} else{\
			for( ; first < last; first++, val+= dict[*idx++]){\
				MOSskipit();\
				cmp  =  ((*hi && val <= * (TPE*)hgh ) || (!*hi && val < *(TPE*)hgh )) &&\
						((*li && val >= * (TPE*)low ) || (!*li && val > *(TPE*)low ));\
				if (cmp )\
					*o++ = (oid) first;\
			}\
		}\
	} else {\
		if( *(TPE*) low == TPE##_nil && *(TPE*) hgh == TPE##_nil){\
			/* nothing is matching */\
		} else\
		if( *(TPE*) low == TPE##_nil ){\
			for( ; first < last; first++, val+=dict[*idx++]){\
				MOSskipit();\
				cmp  =  ((*hi && val <= * (TPE*)hgh ) || (!*hi && val < *(TPE*)hgh ));\
				if ( !cmp )\
					*o++ = (oid) first;\
			}\
		} else\
		if( *(TPE*) hgh == TPE##_nil ){\
			for( ; first < last; first++, val+=dict[*idx++]){\
				MOSskipit();\
				cmp  =  ((*li && val >= * (TPE*)low ) || (!*li && val > *(TPE*)low ));\
				if ( !cmp )\
					*o++ = (oid) first;\
			}\
		} else{\
			for( ; first < last; first++, val+=dict[*idx++]){\
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
MOSsubselect_variance(Client cntxt,  MOStask task, void *low, void *hgh, bit *li, bit *hi, bit *anti)
{
	oid *o;
	BUN first,last;
	int cmp;
	(void) cntxt;

	// set the oid range covered and advance scan range
	first = task->start;
	last = first + MOScnt(task->blk);

	if (task->cl && *task->cl > last){
		MOSskip_variance(cntxt,task);
		return MAL_SUCCEED;
	}
	o = task->lb;

	switch(task->type){
	case TYPE_sht: subselect_variance(sht); break;
	case TYPE_oid: subselect_variance(oid); break;
	case TYPE_lng: subselect_variance(lng); break;
#ifdef HAVE_HGE
	case TYPE_hge: subselect_variance(hge); break;
#endif
	case TYPE_wrd: subselect_variance(wrd); break;
	case TYPE_int:
	// Expanded MOSselect_variance for debugging
	{ 	int *dict= (int*) (((char*) task->blk) + 2 * MosaicBlkSize) ,val= dict[0];
		bte *idx = (bte*) (((char*) task->blk) + 2 * MosaicBlkSize + dictsize * sizeof(int));

		if( !*anti){
			if( *(int*) low == int_nil && *(int*) hgh == int_nil){
				for( ; first < last; first++){
					MOSskipit();
					*o++ = (oid) first;
				}
			} else
			if( *(int*) low == int_nil ){
				for( ; first < last; first++, val+=dict[*idx++]){
					MOSskipit();
					cmp  =  ((*hi && val <= * (int*)hgh ) || (!*hi && val < *(int*)hgh ));
					if (cmp )
						*o++ = (oid) first;
				}
			} else
			if( *(int*) hgh == int_nil ){
				for( ; first < last; first++, val+=dict[*idx++]){
					MOSskipit();
					cmp  =  ((*li && val >= * (int*)low ) || (!*li && val > *(int*)low ));
					if (cmp )
						*o++ = (oid) first;
				}
			} else{
				for( ; first < last; first++, val+=dict[*idx++]){
					MOSskipit();
					cmp  =  ((*hi && val <= * (int*)hgh ) || (!*hi && val < *(int*)hgh )) &&
							((*li && val >= * (int*)low ) || (!*li && val > *(int*)low ));
					if (cmp )
						*o++ = (oid) first;
				}
			}
		} else {
			if( *(int*) low == int_nil && *(int*) hgh == int_nil){
				/* nothing is matching */
			} else
			if( *(int*) low == int_nil ){
				for( ; first < last; first++, val+=dict[*idx++]){
					MOSskipit();
					cmp  =  ((*hi && val <= * (int*)hgh ) || (!*hi && val < *(int*)hgh ));
					if ( !cmp )
						*o++ = (oid) first;
				}
			} else
			if( *(int*) hgh == int_nil ){
				for( ; first < last; first++, idx++){
					MOSskipit();
					cmp  =  ((*li && val >= * (int*)low ) || (!*li && val > *(int*)low ));
					if ( !cmp )
						*o++ = (oid) first;
				}
			} else{
				for( ; first < last; first++, val+=dict[*idx++]){
					MOSskipit();
					cmp  =  ((*hi && val <= * (int*)hgh ) || (!*hi && val < *(int*)hgh )) &&
							((*li && val >= * (int*)low ) || (!*li && val > *(int*)low ));
					if ( !cmp )
						*o++ = (oid) first;
				}
			}
		}
	}
	break;
	default:
		if( task->type == TYPE_date)
			subselect_variance(date);
		if( task->type == TYPE_daytime)
			subselect_variance(daytime);
		if( task->type == TYPE_timestamp)
		{ 	lng *dict= (lng*) (((char*) task->blk) + 2 * MosaicBlkSize) ,val =dict[0];
			bte *idx = (bte*) (((char*) task->blk) + 2 * MosaicBlkSize + dictsize * sizeof(lng));
			int lownil = timestamp_isnil(*(timestamp*)low);
			int hghnil = timestamp_isnil(*(timestamp*)hgh);

			if( !*anti){
				if( lownil && hghnil){
					for( ; first < last; first++){
						MOSskipit();
						*o++ = (oid) first;
					}
				} else
				if( lownil){
					for( ; first < last; first++, val+= dict[*idx++]){
						MOSskipit();
						cmp  =  ((*hi && val <= * (lng*)hgh ) || (!*hi && val < *(lng*)hgh ));
						if (cmp )
							*o++ = (oid) first;
					}
				} else
				if( hghnil){
					for( ; first < last; first++, val+= dict[*idx++]){
						MOSskipit();
						cmp  =  ((*li && val >= * (lng*)low ) || (!*li && val > *(lng*)low ));
						if (cmp )
							*o++ = (oid) first;
					}
				} else{
					for( ; first < last; first++, val+= dict[*idx++]){
						MOSskipit();
						cmp  =  ((*hi && val <= * (lng*)hgh ) || (!*hi && val < *(lng*)hgh )) &&
								((*li && val >= * (lng*)low ) || (!*li && val > *(lng*)low ));
						if (cmp )
							*o++ = (oid) first;
					}
				}
			} else {
				if( lownil && hghnil){
					/* nothing is matching */
				} else
				if( lownil){
					for( ; first < last; first++, val+= dict[*idx++]){
						MOSskipit();
						cmp  =  ((*hi && val <= * (lng*)hgh ) || (!*hi && val < *(lng*)hgh ));
						if ( !cmp )
							*o++ = (oid) first;
					}
				} else
				if( hghnil){
					for( ; first < last; first++, val+= dict[*idx++]){
						MOSskipit();
						cmp  =  ((*li && val >= * (lng*)low ) || (!*li && val > *(lng*)low ));
						if ( !cmp )
							*o++ = (oid) first;
					}
				} else{
					for( ; first < last; first++, val+= dict[*idx++]){
						MOSskipit();
						cmp  =  ((*hi && val <= * (lng*)hgh ) || (!*hi && val < *(lng*)hgh )) &&
								((*li && val >= * (lng*)low ) || (!*li && val > *(lng*)low ));
						if ( !cmp )
							*o++ = (oid) first;
					}
				}
			}
		}
	}
	MOSskip_variance(cntxt,task);
	task->lb = o;
	return MAL_SUCCEED;
}

#define thetasubselect_variance(TPE)\
{ 	TPE low,hgh, *dict,w;\
	bte *idx = (bte*) (((char*) task->blk) + 2 * MosaicBlkSize + dictsize * sizeof(lng));\
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
	dict = (TPE*) (((char*)task->blk) + MosaicBlkSize);\
	w = dict[0];\
	for(idx=0 ; first < last; first++, w+=dict[*idx++]){\
		if( (low == TPE##_nil || w >= low) && (w <= hgh || hgh == TPE##_nil) ){\
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
MOSthetasubselect_variance(Client cntxt,  MOStask task, void *val, str oper)
{
	oid *o;
	int anti=0;
	BUN first,last;
	(void) cntxt;
	
	// set the oid range covered and advance scan range
	first = task->start;
	last = first + MOScnt(task->blk);

	if (task->cl && *task->cl > last){
		MOSskip_variance(cntxt,task);
		return MAL_SUCCEED;
	}
	o = task->lb;

	switch(task->type){
	case TYPE_sht: thetasubselect_variance(sht); break;
	case TYPE_oid: thetasubselect_variance(oid); break;
	case TYPE_lng: thetasubselect_variance(lng); break;
#ifdef HAVE_HGE
	case TYPE_hge: thetasubselect_variance(hge); break;
#endif
	case TYPE_wrd: thetasubselect_variance(wrd); break;
	case TYPE_int:
		{ 	int low,hgh;
			int *dict= (int*) (((char*) task->blk) + 2 * MosaicBlkSize ),v=dict[0];
			bte *idx = (bte*) (((char*) task->blk) + 2 * MosaicBlkSize + dictsize * sizeof(int));
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
				hgh= low= *(int*) val;
				anti++;
			} else
			if ( strcmp(oper,"==") == 0){
				hgh= low= *(int*) val;
			} 
			for( ; first < last; first++, v+= dict[*idx++]){
				MOSskipit();
				if( (low == int_nil || v >= low) && (v <= hgh || hgh == int_nil) ){
					if ( !anti) {
						*o++ = (oid) first;
					}
				} else
				if( anti){
					*o++ = (oid) first;
				}
			}
		} 
		break;
	default:
		if( task->type == TYPE_timestamp){
		{ 	lng *dict= (lng*) (((char*) task->blk) + 2 * MosaicBlkSize ), v = dict[0];
			bte *idx = (bte*) (((char*) task->blk) + 2 * MosaicBlkSize + dictsize * sizeof(lng));
			lng low,hgh;

			low= hgh = int_nil;
			if ( strcmp(oper,"<") == 0){
				hgh= *(lng*) val;
				hgh = PREVVALUElng(hgh);
			} else
			if ( strcmp(oper,"<=") == 0){
				hgh= *(lng*) val;
			} else
			if ( strcmp(oper,">") == 0){
				low = *(lng*) val;
				low = NEXTVALUElng(low);
			} else
			if ( strcmp(oper,">=") == 0){
				low = *(lng*) val;
			} else
			if ( strcmp(oper,"!=") == 0){
				hgh= low= *(lng*) val;
				anti++;
			} else
			if ( strcmp(oper,"==") == 0){
				hgh= low= *(lng*) val;
			} 
			for( ; first < last; first++, v+= dict[*idx++]){
				MOSskipit();
				if( (low == int_nil || v >= low) && (v <= hgh || hgh == int_nil) ){
					if ( !anti) {
						*o++ = (oid) first;
					}
				} else
				if( anti){
					*o++ = (oid) first;
				}
			}
		} 
		}
	}
	MOSskip_variance(cntxt,task);
	task->lb =o;
	return MAL_SUCCEED;
}

#define leftfetchjoin_variance(TPE)\
{	TPE *v;\
	TPE *dict= (TPE*) (((char*) task->blk) + 2 * MosaicBlkSize ),val = dict[0];\
	bte *idx = (bte*) (((char*) task->blk) + 2 * MosaicBlkSize + dictsize * sizeof(TPE));\
	v= (TPE*) task->src;\
	for(; first < last; first++, val+= dict[*idx++]){\
		MOSskipit();\
		*v++ = val;\
		task->n--;\
	}\
	task->src = (char*) v;\
}

str
MOSleftfetchjoin_variance(Client cntxt,  MOStask task)
{
	BUN first,last;
	(void) cntxt;
	// set the oid range covered and advance scan range
	first = task->start;
	last = first + MOScnt(task->blk);

	switch(ATOMstorage(task->type)){
		case TYPE_sht: leftfetchjoin_variance(sht); break;
		case TYPE_oid: leftfetchjoin_variance(oid); break;
		case TYPE_lng: leftfetchjoin_variance(lng); break;
		case TYPE_wrd: leftfetchjoin_variance(wrd); break;
		case TYPE_flt: leftfetchjoin_variance(flt); break;
		case TYPE_dbl: leftfetchjoin_variance(dbl); break;
#ifdef HAVE_HGE
		case TYPE_hge: leftfetchjoin_variance(hge); break;
#endif
		case TYPE_int:
		{	int *v;
			int *dict= (int*) (((char*) task->blk) + 2 * MosaicBlkSize ),val = dict[0];
			bte *idx = (bte*) (((char*) task->blk) + 2 * MosaicBlkSize + dictsize * sizeof(int));
			v= (int*) task->src;
			for(; first < last; first++, val+=dict[*idx++]){
				MOSskipit();
				*v++ = val;
				task->n--;
			}
			task->src = (char*) v;
		}
	}
	MOSskip_variance(cntxt,task);
	return MAL_SUCCEED;
}

#define join_variance(TPE)\
{	TPE  *w;\
	TPE *dict= (TPE*) (((char*) task->blk) + 2 * MosaicBlkSize ),val=dict[0];\
	bte *idx = (bte*) (((char*) task->blk) + 2 * MosaicBlkSize + dictsize * sizeof(int));\
	for(oo= (oid) first; first < last; first++, val+=dict[*idx++], oo++){\
		w = (TPE*) task->src;\
		for(n = task->elm, o = 0; n -- > 0; w++,o++)\
		if ( *w == val){\
			BUNappend(task->lbat, &oo, FALSE);\
			BUNappend(task->rbat, &o, FALSE);\
		}\
	}\
}

str
MOSjoin_variance(Client cntxt,  MOStask task)
{
	BUN n,first,last;
	oid o, oo;
	(void) cntxt;

	// set the oid range covered and advance scan range
	first = task->start;
	last = first + MOScnt(task->blk);

	switch(task->type){
		case TYPE_sht: join_variance(sht); break;
		case TYPE_oid: join_variance(oid); break;
		case TYPE_lng: join_variance(lng); break;
		case TYPE_wrd: join_variance(wrd); break;
		case TYPE_flt: join_variance(flt); break;
		case TYPE_dbl: join_variance(dbl); break;
#ifdef HAVE_HGE
		case TYPE_hge: join_variance(hge); break;
#endif
		case TYPE_int:
		{	int  *w;
			int *dict= (int*) (((char*) task->blk) + 2 * MosaicBlkSize ), val=dict[0];
			bte *idx = (bte*) (((char*) task->blk) + 2 * MosaicBlkSize + dictsize * sizeof(int));
			for(oo= (oid) first; first < last; first++, val+= dict[*idx++], oo++){
				w = (int*) task->src;
				for(n = task->elm, o = 0; n -- > 0; w++,o++)
				if ( *w == val){
					BUNappend(task->lbat, &oo, FALSE);
					BUNappend(task->rbat, &o, FALSE);
				}
			}
		}
		break;
		default:
			if( task->type == TYPE_timestamp)
			{	timestamp  *w;
				lng *dict = (lng*) (((char*) task->blk) + 2 * MosaicBlkSize ), lval = dict[0];
				timestamp *tval = (timestamp*) &lval;
				bte *idx = (bte*) (((char*) task->blk) + 2 * MosaicBlkSize + dictsize * sizeof(timestamp));
				for(oo= (oid) first; first < last; first++, lval += dict[*idx++], oo++){
					w = (timestamp*) task->src;
					for(n = task->elm, o = 0; n -- > 0; w++,o++)
					if ( w->days == tval->days && w->msecs == tval->msecs){
						BUNappend(task->lbat, &oo, FALSE);
						BUNappend(task->rbat, &o, FALSE);
					}
				}
			}
	}
	MOSskip_variance(cntxt,task);
	return MAL_SUCCEED;
}
