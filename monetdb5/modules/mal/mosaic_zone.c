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
 * Adaptive zone indexing for non-compressed blocls
 */

#include "monetdb_config.h"
#include "mosaic.h"
#include "mosaic_zone.h"

#define zone_min(BLK) (void*)(((char*) BLK)+ MosaicBlkSize)
#define zone_max(BLK) (void*)(((char*) BLK)+ 2 * MosaicBlkSize)

void
MOSdump_zone(Client cntxt, MOStask task)
{
#ifdef _DEBUG_MOSAIC_
	MosaicBlk blk = task->blk;
	mnstr_printf(cntxt->fdout,"#zone "BUNFMT" elms ", MOScnt(blk));
	switch(task->type){
	case TYPE_bte: {bte low= *(bte*)zone_min(blk), max =*(bte*) zone_max(blk);  mnstr_printf(cntxt->fdout," [%d - %d]\n", low,max); }break;
	case TYPE_bit: {bit low= *(bit*)zone_min(blk), max =*(bit*) zone_max(blk);  mnstr_printf(cntxt->fdout," [%d - %d]\n", low,max); }break;
	case TYPE_int: {int low= *(int*)zone_min(blk), max =*(int*) zone_max(blk);  mnstr_printf(cntxt->fdout," [%d - %d]\n", low,max); }break;
	case TYPE_oid: {oid low= *(oid*)zone_min(blk), max =*(oid*) zone_max(blk);  mnstr_printf(cntxt->fdout," ["LLFMT" - "LLFMT"]\n", low,max); }break;
	case TYPE_lng: {lng low= *(lng*)zone_min(blk), max =*(lng*) zone_max(blk);  mnstr_printf(cntxt->fdout," ["LLFMT" - "LLFMT"]\n", low,max); }break;
	case TYPE_wrd: {wrd low= *(wrd*)zone_min(blk), max =*(wrd*) zone_max(blk);  mnstr_printf(cntxt->fdout," ["SZFMT" - "SZFMT"]\n", low,max); }break;
	case TYPE_flt: {flt low= *(flt*)zone_min(blk), max =*(flt*) zone_max(blk);  mnstr_printf(cntxt->fdout," [%f - %f]\n", low,max); }break;
	case TYPE_dbl: {dbl low= *(dbl*)zone_min(blk), max =*(dbl*) zone_max(blk);  mnstr_printf(cntxt->fdout," [%f - %f]\n", low,max); }break;
	default:
		if( task->type == TYPE_date){
			date low= *(date*)zone_min(blk), max =*(date*) zone_max(blk);  
			mnstr_printf(cntxt->fdout," [%d - %d]\n", low,max); 
		}
		if( task->type == TYPE_daytime){
			daytime low= *(daytime*)zone_min(blk), max =*(daytime*) zone_max(blk);  
			mnstr_printf(cntxt->fdout," [%d - %d]\n", low,max); 
		}
		if( task->type == TYPE_timestamp){
			lng low= *(lng*)zone_min(blk), max =*(lng*) zone_max(blk);  
			mnstr_printf(cntxt->fdout," ["LLFMT" - "LLFMT"]\n", low,max); 
		}
	}
#else
	(void)cntxt;
	(void) task;
#endif
}
void
MOSadvance_zone(MOStask task)
{
	MosaicBlk blk = task->blk;
	switch(task->type){
	case TYPE_bte: task->blk = (MosaicBlk)( ((char*) task->blk) + 3 * MosaicBlkSize + wordaligned(sizeof(bte)* MOScnt(blk),bte)); break ;
	case TYPE_bit: task->blk = (MosaicBlk)( ((char*) task->blk) + 3 * MosaicBlkSize + wordaligned(sizeof(bit)* MOScnt(blk),bit)); break ;
	case TYPE_sht: task->blk = (MosaicBlk)( ((char*) task->blk) + 3 * MosaicBlkSize + wordaligned(sizeof(sht)* MOScnt(blk),sht)); break ;
	case TYPE_int: task->blk = (MosaicBlk)( ((char*) task->blk) + 3 * MosaicBlkSize + wordaligned(sizeof(int)* MOScnt(blk),int)); break ;
	case TYPE_oid: task->blk = (MosaicBlk)( ((char*) task->blk) + 3 * MosaicBlkSize + wordaligned(sizeof(oid)* MOScnt(blk),oid)); break ;
	case TYPE_lng: task->blk = (MosaicBlk)( ((char*) task->blk) + 3 * MosaicBlkSize + wordaligned(sizeof(lng)* MOScnt(blk),lng)); break ;
	case TYPE_wrd: task->blk = (MosaicBlk)( ((char*) task->blk) + 3 * MosaicBlkSize + wordaligned(sizeof(wrd)* MOScnt(blk),wrd)); break ;
	case TYPE_flt: task->blk = (MosaicBlk)( ((char*) task->blk) + 3 * MosaicBlkSize + wordaligned(sizeof(flt)* MOScnt(blk),flt)); break ;
	case TYPE_dbl: task->blk = (MosaicBlk)( ((char*) task->blk) + 3 * MosaicBlkSize + wordaligned(sizeof(dbl)* MOScnt(blk),dbl)); break;
	default:
		if( task->type == TYPE_date)
			task->blk = (MosaicBlk)( ((char*) task->blk) + 3 * MosaicBlkSize + wordaligned(sizeof(date)* MOScnt(blk),date)); 
		if( task->type == TYPE_daytime)
			task->blk = (MosaicBlk)( ((char*) task->blk) + 3 * MosaicBlkSize + wordaligned(sizeof(daytime)* MOScnt(blk),daytime)); 
		if( task->type == TYPE_timestamp)
			task->blk = (MosaicBlk)( ((char*) task->blk) + 3 * MosaicBlkSize + wordaligned(sizeof(timestamp)* MOScnt(blk),timestamp)); 
	}
}

void
MOSskip_zone(MOStask task)
{
	MOSadvance_zone(task);
	if ( MOStag(task->blk) == MOSAIC_EOL)
		task->blk = 0; // ENDOFLIST
}


// append a series of values into the non-compressed block

#define ZONEcompress(TPE)\
{	TPE *min,*max;\
	min = (TPE*)zone_min(blk);\
	max = (TPE*)zone_max(blk);\
	if ( MOScnt(blk) == 0 || *min == TPE##_nil || *min > *(TPE*)task->src) *min = *(TPE*)task->src;\
	if ( MOScnt(blk) == 0 || *max == TPE##_nil || *max < *(TPE*)task->src) *max = *(TPE*)task->src;\
	*(TPE*) task->dst = *(TPE*) task->src;\
	task->src += sizeof(TPE);\
	task->dst += sizeof(TPE);\
	MOSinc(blk,1);\
	task->elm--;\
}

int
MOSestimate_zone(Client cntxt, MOStask task)
{	
	(void) cntxt;
	(void) task;
	return 100;
}

#define ZONEbreak(TPE)\
{	TPE *min,*max;\
	min = (TPE*)zone_min(blk);\
	max = (TPE*)zone_max(blk);\
	if ( task->min ==0 || *(TPE*)task->min > *min) task->min = (void*) min;\
	if ( task->max ==0 || *(TPE*)task->max < *max ) task->max = (void*) max;\
}

// rather expensive simple value non-compressed store
void
MOScompress_zone(Client cntxt, MOStask task)
{
	MosaicBlk blk = (MosaicBlk) task->blk;

	(void) cntxt;
	*blk = MOSzone;

	switch(ATOMstorage(task->type)){
	case TYPE_bte: ZONEcompress(bte); break ;
	case TYPE_bit: ZONEcompress(bit); break ;
	case TYPE_sht: ZONEcompress(sht); break;
	case TYPE_int:
	{	int *min,*max;
		min = (int*)zone_min(blk);
		max = (int*)zone_max(blk);
		if (MOScnt(blk) == 0 || *min == int_nil || *min > *(int*)task->src) *min = *(int*)task->src;
		if (MOScnt(blk) == 0 || *max == int_nil || *max < *(int*)task->src) *max = *(int*)task->src;
		*(int*) task->dst = *(int*) task->src;
		task->src += sizeof(int);
		task->dst += sizeof(int);
		MOSinc(blk,1);
		task->elm--;
	}
		break;
	case TYPE_oid: ZONEcompress(oid); break;
	case TYPE_lng: ZONEcompress(lng); break;
	case TYPE_wrd: ZONEcompress(wrd); break;
	case TYPE_flt: ZONEcompress(flt); break;
	case TYPE_dbl: ZONEcompress(dbl); break;
	}
	// maintain a global min-max pair
	// break the zone when it covers too many elements
	switch(ATOMstorage(task->type)){
	case TYPE_bte: ZONEbreak(bte); break ;
	case TYPE_bit: ZONEbreak(bit); break ;
	case TYPE_sht: ZONEbreak(sht); break;
	case TYPE_int:
	{	int *min,*max;
		min = (int*)zone_min(blk);
		max = (int*)zone_max(blk);
		if ( task->min ==0 || *(int*)task->min > *min) task->min = (void*) min;
		if ( task->max ==0 || *(int*)task->max < *max ) task->max = (void*) max;
	}
		break;
	case TYPE_oid: ZONEbreak(oid); break;
	case TYPE_lng: ZONEbreak(lng); break;
	case TYPE_wrd: ZONEbreak(wrd); break;
	case TYPE_flt: ZONEbreak(flt); break;
	case TYPE_dbl: ZONEbreak(dbl); break;
	}
	
	MOSdump_zone(cntxt, task);
#ifdef _DEBUG_MOSAIC_
#endif
}

// the inverse operator, extend the src
#define ZONEdecompress(TYPE)\
{ BUN lim = MOScnt(blk); for(i = 0; i < lim; i++) \
	((TYPE*)task->src)[i] = ((TYPE*)compressed)[i]; \
	task->src += i * sizeof(TYPE);\
}

void
MOSdecompress_zone(Client cntxt, MOStask task)
{
	MosaicBlk blk = (MosaicBlk) task->blk;
	BUN i;
	char *compressed;
	(void) cntxt;

	compressed = ((char*)blk) + 3 * MosaicBlkSize;
	switch(task->type){
	case TYPE_bte: ZONEdecompress(bte); break ;
	case TYPE_bit: ZONEdecompress(bit); break ;
	case TYPE_sht: ZONEdecompress(sht); break;
	case TYPE_int:
	{	BUN lim = MOScnt(blk);
		for(i = 0; i < lim; i++) 
			((int*)task->src)[i] = ((int*)compressed)[i];
		task->src += i * sizeof(int);
	}
		break;
	case TYPE_oid: ZONEdecompress(oid); break;
	case TYPE_lng: ZONEdecompress(lng); break;
	case TYPE_wrd: ZONEdecompress(wrd); break;
	case TYPE_flt: ZONEdecompress(flt); break;
	case TYPE_dbl: ZONEdecompress(dbl); break;
	default:
		if( task->type == TYPE_date)
			ZONEdecompress(date);
		if( task->type == TYPE_daytime)
			ZONEdecompress(daytime);
		if( task->type == TYPE_timestamp)
			ZONEdecompress(timestamp);
	}
}

// The remainder should provide the minimal algebraic framework
//  to apply the operator to a ZONE compressed chunk

	
#define subselect_zone(TPE) {\
		TPE *val= (TPE*) (((char*) task->blk) + 3 * MosaicBlkSize);\
		TPE *min,*max;\
		min = (TPE*)zone_min(task->blk);\
		max = (TPE*)zone_max(task->blk);\
		if( !*anti){\
			if ( (*(TPE*)low != TPE##_nil && *(TPE *)low > *max) || (*(TPE *)hgh != TPE##_nil && *(TPE *) hgh < *min))\
				break;\
			if( *(TPE*) low == TPE##_nil && *(TPE*) hgh == TPE##_nil){\
				for( ; first < last; first++){\
					MOSskipit();\
					*o++ = (oid) first;\
				}\
			} else\
			if( *(TPE*) low == TPE##_nil ){\
				for( ; first < last; first++, val++){\
					MOSskipit();\
					cmp  =  ((*hi && *(TPE*)val <= * (TPE*)hgh ) || (!*hi && *(TPE*)val < *(TPE*)hgh ));\
					if (cmp )\
						*o++ = (oid) first;\
				}\
			} else\
			if( *(TPE*) hgh == TPE##_nil ){\
				for( ; first < last; first++, val++){\
					MOSskipit();\
					cmp  =  ((*li && *(TPE*)val >= * (TPE*)low ) || (!*li && *(TPE*)val > *(TPE*)low ));\
					if (cmp )\
						*o++ = (oid) first;\
				}\
			} else{\
				for( ; first < last; first++, val++){\
					MOSskipit();\
					cmp  =  ((*hi && *(TPE*)val <= * (TPE*)hgh ) || (!*hi && *(TPE*)val < *(TPE*)hgh )) &&\
							((*li && *(TPE*)val >= * (TPE*)low ) || (!*li && *(TPE*)val > *(TPE*)low ));\
					if (cmp )\
						*o++ = (oid) first;\
				}\
			}\
		} else {\
			if ( *(TPE*)low != TPE##_nil && *(TPE*)low >= *min && *(TPE*)hgh != TPE##_nil && *(TPE*) hgh <= *max)\
				break;\
			if( *(TPE*) low == TPE##_nil && *(TPE*) hgh == TPE##_nil){\
				/* nothing is matching */\
			} else\
			if( *(TPE*) low == TPE##_nil ){\
				for( ; first < last; first++, val++){\
					MOSskipit();\
					cmp  =  ((*hi && *(TPE*)val <= * (TPE*)hgh ) || (!*hi && *(TPE*)val < *(TPE*)hgh ));\
					if ( !cmp )\
						*o++ = (oid) first;\
				}\
			} else\
			if( *(TPE*) hgh == TPE##_nil ){\
				for( ; first < last; first++, val++){\
					MOSskipit();\
					cmp  =  ((*li && *(TPE*)val >= * (TPE*)low ) || (!*li && *(TPE*)val > *(TPE*)low ));\
					if ( !cmp )\
						*o++ = (oid) first;\
				}\
			} else{\
				for( ; first < last; first++, val++){\
					MOSskipit();\
					cmp  =  ((*hi && *(TPE*)val <= * (TPE*)hgh ) || (!*hi && *(TPE*)val < *(TPE*)hgh )) &&\
							((*li && *(TPE*)val >= * (TPE*)low ) || (!*li && *(TPE*)val > *(TPE*)low ));\
					if ( !cmp )\
						*o++ = (oid) first;\
				}\
			}\
		}\
	}

str
MOSsubselect_zone(Client cntxt,  MOStask task, BUN first, BUN last, void *low, void *hgh, bit *li, bit *hi, bit *anti)
{
	oid *o;
	int cmp;
	(void) cntxt;
	(void) low;
	(void) hgh;
	(void) li;
	(void) hi;
	(void) anti;

	if ( first + MOScnt(task->blk) > last)
		last = MOScnt(task->blk);
	o = task->lb;

	switch(task->type){
	case TYPE_bit: subselect_zone(bit); break;
	case TYPE_bte: subselect_zone(bte); break;
	case TYPE_sht: subselect_zone(sht); break;
	case TYPE_oid: subselect_zone(oid); break;
	case TYPE_lng: subselect_zone(lng); break;
	case TYPE_wrd: subselect_zone(wrd); break;
	case TYPE_flt: subselect_zone(flt); break;
	case TYPE_dbl: subselect_zone(dbl); break;
	case TYPE_int:
	// Expanded MOSselect_zone for debugging
		{ 	int *val= (int*) (((char*) task->blk) + 3 * MosaicBlkSize);
			int *min,*max;
			min = (int*)zone_min(task->blk);
			max = (int*)zone_max(task->blk);

			if( !*anti){
				// prefilter based on zone map
				if ( (*(int*) low != int_nil && *(int*)low > *max) || (*(int*)hgh != int_nil && *(int*) hgh < *min))
					break;

				if( *(int*) low == int_nil && *(int*) hgh == int_nil){
					for( ; first < last; first++, val++){
						MOSskipit();
						*o++ = (oid) first;
					}
				} else
				if( *(int*) low == int_nil ){
					for( ; first < last; first++, val++){
						MOSskipit();
						cmp  =  ((*hi && *(int*)val <= * (int*)hgh ) || (!*hi && *(int*)val < *(int*)hgh ));
						if (cmp )
							*o++ = (oid) first;
					}
				} else
				if( *(int*) hgh == int_nil ){
					for( ; first < last; first++, val++){
						MOSskipit();
						cmp  =  ((*li && *(int*)val >= * (int*)low ) || (!*li && *(int*)val > *(int*)low ));
						if (cmp )
							*o++ = (oid) first;
					}
				} else{
					for( ; first < last; first++, val++){
						MOSskipit();
						cmp  =  ((*hi && *(int*)val <= * (int*)hgh ) || (!*hi && *(int*)val < *(int*)hgh )) &&
								((*li && *(int*)val >= * (int*)low ) || (!*li && *(int*)val > *(int*)low ));
						if (cmp )
							*o++ = (oid) first;
					}
				}
			} else {
				// prefilter based on zone map
				if ( *(int*)low != int_nil && *(int*)low >= *min && *(int*)hgh != int_nil && *(int*) hgh <= *max)
					break;
				if( *(int*) low == int_nil && *(int*) hgh == int_nil){
					/* nothing is matching */
				} else
				if( *(int*) low == int_nil ){
					for( ; first < last; first++, val++){
						MOSskipit();
						cmp  =  ((*hi && *(int*)val <= * (int*)hgh ) || (!*hi && *(int*)val < *(int*)hgh ));
						if ( !cmp )
							*o++ = (oid) first;
					}
				} else
				if( *(int*) hgh == int_nil ){
					for( ; first < last; first++, val++){
						MOSskipit();
						cmp  =  ((*li && *(int*)val >= * (int*)low ) || (!*li && *(int*)val > *(int*)low ));
						if ( !cmp )
							*o++ = (oid) first;
					}
				} else{
					for( ; first < last; first++, val++){
						MOSskipit();
						cmp  =  ((*hi && *(int*)val <= * (int*)hgh ) || (!*hi && *(int*)val < *(int*)hgh )) &&
								((*li && *(int*)val >= * (int*)low ) || (!*li && *(int*)val > *(int*)low ));
						if ( !cmp )
							*o++ = (oid) first;
					}
				}
			}
		}
			break;
		default:
			if( task->type == TYPE_date)
				subselect_zone(date); 
			if( task->type == TYPE_daytime)
				subselect_zone(daytime); 

			if( task->type == TYPE_timestamp)
				{ 	lng *val= (lng*) (((char*) task->blk) + 3 * MosaicBlkSize);
					int lownil = timestamp_isnil(*(timestamp*)low);
					int hghnil = timestamp_isnil(*(timestamp*)hgh);

					if( !*anti){
						if( lownil && hghnil){
							for( ; first < last; first++, val++){
								MOSskipit();
								*o++ = (oid) first;
							}
						} else if( lownil){
							for( ; first < last; first++, val++){
								MOSskipit();
								cmp  =  ((*hi && *(lng*)val <= * (lng*)hgh ) || (!*hi && *(lng*)val < *(lng*)hgh ));
								if (cmp )
									*o++ = (oid) first;
							}
						} else
						if( hghnil){
							for( ; first < last; first++, val++){
								MOSskipit();
								cmp  =  ((*li && *(lng*)val >= * (lng*)low ) || (!*li && *(lng*)val > *(lng*)low ));
								if (cmp )
									*o++ = (oid) first;
							}
						} else{
							for( ; first < last; first++, val++){
								MOSskipit();
								cmp  =  ((*hi && *(lng*)val <= * (lng*)hgh ) || (!*hi && *(lng*)val < *(lng*)hgh )) &&
										((*li && *(lng*)val >= * (lng*)low ) || (!*li && *(lng*)val > *(lng*)low ));
								if (cmp )
									*o++ = (oid) first;
							}
						}
					} else {
						if( lownil && hghnil){
							/* nothing is matching */
						} else
						if( lownil){
							for( ; first < last; first++, val++){
								MOSskipit();
								cmp  =  ((*hi && *(lng*)val <= * (lng*)hgh ) || (!*hi && *(lng*)val < *(lng*)hgh ));
								if ( !cmp )
									*o++ = (oid) first;
							}
						} else
						if( hghnil){
							for( ; first < last; first++, val++){
								MOSskipit();
								cmp  =  ((*li && *(lng*)val >= * (lng*)low ) || (!*li && *(lng*)val > *(lng*)low ));
								if ( !cmp )
									*o++ = (oid) first;
							}
						} else{
							for( ; first < last; first++, val++){
								MOSskipit();
								cmp  =  ((*hi && *(lng*)val <= * (lng*)hgh ) || (!*hi && *(lng*)val < *(lng*)hgh )) &&
										((*li && *(lng*)val >= * (lng*)low ) || (!*li && *(lng*)val > *(lng*)low ));
								if ( !cmp )
									*o++ = (oid) first;
							}
						}
					}
				}
	}
	task->lb = o;
	return MAL_SUCCEED;
}

#define thetasubselect_zone(TPE)\
{ 	TPE low,hgh, *v;\
	TPE *min,*max;\
	min = (TPE*)zone_min(task->blk);\
	max = (TPE*)zone_max(task->blk);\
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
	if(!anti){\
		if  ( (hgh != TPE##_nil && *min > hgh) || (low !=TPE##_nil && *max <low) )\
			break;\
	} else{\
		if  ( low != TPE##_nil && *min >= low && hgh != TPE##_nil && hgh <= *max )\
			break;\
	}\
	v = (TPE*) (((char*)task->blk) + 3 * MosaicBlkSize);\
	for( ; first < last; first++, v++){\
		if( (low == TPE##_nil || * v >= low) && (* v <= hgh || hgh == TPE##_nil) ){\
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
MOSthetasubselect_zone(Client cntxt,  MOStask task, BUN first, BUN last, void *val, str oper)
{
	oid *o;
	int anti=0;
	(void) cntxt;
	
	if ( first + MOScnt(task->blk) > last)
		last = MOScnt(task->blk);
	o = task->lb;

	switch(task->type){
	case TYPE_bit: thetasubselect_zone(bit); break;
	case TYPE_bte: thetasubselect_zone(bte); break;
	case TYPE_sht: thetasubselect_zone(sht); break;
	case TYPE_oid: thetasubselect_zone(oid); break;
	case TYPE_lng: thetasubselect_zone(lng); break;
	case TYPE_wrd: thetasubselect_zone(wrd); break;
	case TYPE_flt: thetasubselect_zone(flt); break;
	case TYPE_dbl: thetasubselect_zone(dbl); break;
	case TYPE_int:
		{ 	int low,hgh, *v;
			int *min,*max;
			min = (int*)zone_min(task->blk);
			max = (int*)zone_max(task->blk);

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
			if(!anti){
				if  ( (hgh != int_nil && *min > hgh) || (low !=int_nil && *max <low) )
					break;
			} else{
				if  ( low != int_nil && *min >= low && hgh != int_nil && hgh <= *max )
					break;
			}
			v = (int*) (((char*)task->blk) + 3 * MosaicBlkSize);
			for( ; first < last; first++, v++){
				if( (low == int_nil || * v >= low) && (* v <= hgh || hgh == int_nil) ){
					if ( !anti) {
						MOSskipit();
						*o++ = (oid) first;
					}
				} else
				if( anti){
					MOSskipit();
					*o++ = (oid) first;
				}
			}
		} 
		break;
	default:
			if( task->type == TYPE_date)
				thetasubselect_zone(date); 
			if( task->type == TYPE_daytime)
				thetasubselect_zone(daytime); 
			if( task->type == TYPE_timestamp)
				thetasubselect_zone(lng); 
	}
	task->lb =o;
	return MAL_SUCCEED;
}

#define leftfetchjoin_zone(TPE)\
{	TPE *val, *v;\
	v= (TPE*) task->src;\
	val = (TPE*) (((char*) task->blk) + 3 * MosaicBlkSize);\
	for(; first < last; first++, val++){\
		MOSskipit();\
		*v++ = *val;\
		task->n--;\
	}\
	task->src = (char*) v;\
}

str
MOSleftfetchjoin_zone(Client cntxt,  MOStask task, BUN first, BUN last)
{
	(void) cntxt;

	switch(task->type){
		case TYPE_bit: leftfetchjoin_zone(bit); break;
		case TYPE_bte: leftfetchjoin_zone(bte); break;
		case TYPE_sht: leftfetchjoin_zone(sht); break;
		case TYPE_oid: leftfetchjoin_zone(oid); break;
		case TYPE_lng: leftfetchjoin_zone(lng); break;
		case TYPE_flt: leftfetchjoin_zone(flt); break;
		case TYPE_dbl: leftfetchjoin_zone(dbl); break;
		case TYPE_int:
		{	int *val, *v;
			v= (int*) task->src;
			val = (int*) (((char*) task->blk) + 3 * MosaicBlkSize);
			for(; first < last; first++, val++){
				MOSskipit();
				*v++ = *val;
				task->n--;
			}
			task->src = (char*) v;
		}
		break;
		default:
			if (task->type == TYPE_date)
				leftfetchjoin_zone(date); 
			if (task->type == TYPE_daytime)
				leftfetchjoin_zone(daytime); 
			if (task->type == TYPE_timestamp)
				leftfetchjoin_zone(lng); 
	}
	return MAL_SUCCEED;
}

#define join_zone(TPE)\
{	TPE *v, *w;\
	v = (TPE*) (((char*) task->blk) + 3 * MosaicBlkSize);\
	for(oo= (oid) first; first < last; first++, v++, oo++){\
		w = (TPE*) task->src;\
		for(n = task->elm, o = 0; n -- > 0; w++,o++)\
		if ( *w == *v){\
			BUNappend(task->lbat, &oo, FALSE);\
			BUNappend(task->rbat, &o, FALSE);\
		}\
	}\
}

str
MOSjoin_zone(Client cntxt,  MOStask task, BUN first, BUN last)
{
	BUN n;
	oid o, oo;
	(void) cntxt;

	switch(task->type){
		case TYPE_bit: join_zone(bit); break;
		case TYPE_bte: join_zone(bte); break;
		case TYPE_sht: join_zone(sht); break;
		case TYPE_oid: join_zone(oid); break;
		case TYPE_lng: join_zone(lng); break;
		case TYPE_wrd: join_zone(wrd); break;
		case TYPE_flt: join_zone(flt); break;
		case TYPE_dbl: join_zone(dbl); break;
		case TYPE_int:
		{	int *v, *w;
			v = (int*) (((char*) task->blk) + 3 * MosaicBlkSize);
			for(oo= (oid) first; first < last; first++, v++, oo++){
				w = (int*) task->src;
				for(n = task->elm, o = 0; n -- > 0; w++,o++)
				if ( *w == *v){
					BUNappend(task->lbat, &oo, FALSE);
					BUNappend(task->rbat, &o, FALSE);
				}
			}
		}
		break;
		default:
			if (task->type == TYPE_date)
				join_zone(date); 
			if (task->type == TYPE_daytime)
				join_zone(daytime); 
			if (task->type == TYPE_timestamp)
				join_zone(lng); 
	}
	return MAL_SUCCEED;
}
