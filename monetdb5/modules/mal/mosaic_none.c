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
 * Use a chunk that has not been compressed
 */

#include "monetdb_config.h"
#include "mosaic.h"
#include "mosaic_none.h"

void
MOSdump_none(Client cntxt, MOStask task)
{
	MosaicBlk blk = task->blk;
	mnstr_printf(cntxt->fdout,"#none "BUNFMT"\n", MOScnt(blk));
}

void
MOSadvance_none(MOStask task)
{
	MosaicBlk blk = task->blk;
	switch(task->type){
	case TYPE_bte: task->blk = (MosaicBlk)( ((char*) task->blk) + MosaicBlkSize + wordaligned(sizeof(bte)* MOScnt(blk))); break ;
	case TYPE_bit: task->blk = (MosaicBlk)( ((char*) task->blk) + MosaicBlkSize + wordaligned(sizeof(bit)* MOScnt(blk))); break ;
	case TYPE_sht: task->blk = (MosaicBlk)( ((char*) task->blk) + MosaicBlkSize + wordaligned(sizeof(sht)* MOScnt(blk))); break ;
	case TYPE_int: task->blk = (MosaicBlk)( ((char*) task->blk) + MosaicBlkSize + wordaligned(sizeof(int)* MOScnt(blk))); break ;
	case TYPE_oid: task->blk = (MosaicBlk)( ((char*) task->blk) + MosaicBlkSize + wordaligned(sizeof(oid)* MOScnt(blk))); break ;
	case TYPE_lng: task->blk = (MosaicBlk)( ((char*) task->blk) + MosaicBlkSize + wordaligned(sizeof(lng)* MOScnt(blk))); break ;
	case TYPE_wrd: task->blk = (MosaicBlk)( ((char*) task->blk) + MosaicBlkSize + wordaligned(sizeof(wrd)* MOScnt(blk))); break ;
	case TYPE_flt: task->blk = (MosaicBlk)( ((char*) task->blk) + MosaicBlkSize + wordaligned(sizeof(flt)* MOScnt(blk))); break ;
	case TYPE_dbl: task->blk = (MosaicBlk)( ((char*) task->blk) + MosaicBlkSize + wordaligned(sizeof(dbl)* MOScnt(blk))); break;
	default:
		if( task->type == TYPE_date)
			task->blk = (MosaicBlk)( ((char*) task->blk) + MosaicBlkSize + wordaligned(sizeof(date)* MOScnt(blk))); 
		if( task->type == TYPE_daytime)
			task->blk = (MosaicBlk)( ((char*) task->blk) + MosaicBlkSize + wordaligned(sizeof(daytime)* MOScnt(blk))); 
		if( task->type == TYPE_timestamp)
			task->blk = (MosaicBlk)( ((char*) task->blk) + MosaicBlkSize + wordaligned(sizeof(timestamp)* MOScnt(blk))); 
	}
}

void
MOSskip_none(MOStask task)
{
	MOSadvance_none(task);
	if ( MOStag(task->blk) == MOSAIC_EOL)
		task->blk = 0; // ENDOFLIST
}


// append a series of values into the non-compressed block

#define NONEcompress(TYPE)\
{	*(TYPE*) task->dst = *(TYPE*) task->src;\
	task->src += sizeof(TYPE);\
	task->dst += sizeof(TYPE);\
	MOSinc(blk,1);\
	task->elm--;\
}

// rather expensive simple value non-compressed store
void
MOScompress_none(Client cntxt, MOStask task)
{
	MosaicBlk blk = (MosaicBlk) task->blk;

	(void) cntxt;
	*blk = MOSnone + MOScnt(blk);

	switch(ATOMstorage(task->type)){
	case TYPE_bte: NONEcompress(bte); break ;
	case TYPE_bit: NONEcompress(bit); break ;
	case TYPE_sht: NONEcompress(sht); break;
	case TYPE_int:
	{	*(int*) task->dst = *(int*) task->src;
		task->src += sizeof(int);
		task->dst += sizeof(int);
		MOSinc(blk,1);
		task->elm--;
	}
		break;
	case TYPE_oid: NONEcompress(oid); break;
	case TYPE_lng: NONEcompress(lng); break;
	case TYPE_wrd: NONEcompress(wrd); break;
	case TYPE_flt: NONEcompress(flt); break;
	case TYPE_dbl: NONEcompress(dbl); break;
	}
#ifdef _DEBUG_MOSAIC_
	MOSdump_none_(cntxt, task);
#endif
}

// the inverse operator, extend the src
#define NONEdecompress(TYPE)\
{ BUN lim = MOScnt(blk); \
	for(i = 0; i < lim; i++) \
	((TYPE*)task->src)[i] = ((TYPE*)compressed)[i]; \
	task->src += i * sizeof(TYPE);\
}

void
MOSdecompress_none(Client cntxt, MOStask task)
{
	MosaicBlk blk = (MosaicBlk) task->blk;
	BUN i;
	char *compressed;
	(void) cntxt;

	compressed = ((char*)blk) + MosaicBlkSize;
	switch(task->type){
	case TYPE_bte: NONEdecompress(bte); break ;
	case TYPE_bit: NONEdecompress(bit); break ;
	case TYPE_sht: NONEdecompress(sht); break;
	case TYPE_int:
	{ BUN lim = MOScnt(blk);	
		for(i = 0; i < lim; i++) 
			((int*)task->src)[i] = ((int*)compressed)[i];
		task->src += i * sizeof(int);
	}
		break;
	case TYPE_oid: NONEdecompress(oid); break;
	case TYPE_lng: NONEdecompress(lng); break;
	case TYPE_wrd: NONEdecompress(wrd); break;
	case TYPE_flt: NONEdecompress(flt); break;
	case TYPE_dbl: NONEdecompress(dbl); break;
	default:
		if( task->type == TYPE_date)
			NONEdecompress(date);
		if( task->type == TYPE_daytime)
			NONEdecompress(daytime);
		if( task->type == TYPE_timestamp)
			NONEdecompress(timestamp);
	}
}

// The remainder should provide the minimal algebraic framework
//  to apply the operator to a NONE compressed chunk

	
#define subselect_none(TPE) {\
		TPE *val= (TPE*) (((char*) task->blk) + MosaicBlkSize);\
		if( !*anti){\
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
MOSsubselect_none(Client cntxt,  MOStask task, BUN first, BUN last, void *low, void *hgh, bit *li, bit *hi, bit *anti)
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
	case TYPE_bit: subselect_none(bit); break;
	case TYPE_bte: subselect_none(bte); break;
	case TYPE_sht: subselect_none(sht); break;
	case TYPE_oid: subselect_none(oid); break;
	case TYPE_lng: subselect_none(lng); break;
	case TYPE_wrd: subselect_none(wrd); break;
	case TYPE_flt: subselect_none(flt); break;
	case TYPE_dbl: subselect_none(dbl); break;
	case TYPE_int:
	// Expanded MOSselect_none for debugging
		{ 	int *val= (int*) (((char*) task->blk) + MosaicBlkSize);

			if( !*anti){
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
				subselect_none(date); 
			if( task->type == TYPE_daytime)
				subselect_none(daytime); 
			
			if( task->type == TYPE_timestamp)
				{ 	lng *val= (lng*) (((char*) task->blk) + MosaicBlkSize);
					int lownil = timestamp_isnil(*(timestamp*)low);
					int hghnil = timestamp_isnil(*(timestamp*)hgh);

					if( !*anti){
						if( lownil && hghnil){
							for( ; first < last; first++, val++){
								MOSskipit();
								*o++ = (oid) first;
							}
						} else
						if( lownil){
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

#define thetasubselect_none(TPE)\
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
MOSthetasubselect_none(Client cntxt,  MOStask task, BUN first, BUN last, void *val, str oper)
{
	oid *o;
	int anti=0;
	(void) cntxt;
	
	if ( first + MOScnt(task->blk) > last)
		last = MOScnt(task->blk);
	o = task->lb;

	switch(task->type){
	case TYPE_bit: thetasubselect_none(bit); break;
	case TYPE_bte: thetasubselect_none(bte); break;
	case TYPE_sht: thetasubselect_none(sht); break;
	case TYPE_oid: thetasubselect_none(oid); break;
	case TYPE_lng: thetasubselect_none(lng); break;
	case TYPE_wrd: thetasubselect_none(wrd); break;
	case TYPE_flt: thetasubselect_none(flt); break;
	case TYPE_dbl: thetasubselect_none(dbl); break;
	case TYPE_int:
		{ 	int low,hgh, *v;
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
			v = (int*) (((char*)task->blk) + MosaicBlkSize);
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
				thetasubselect_none(date); 
			if( task->type == TYPE_daytime)
				thetasubselect_none(daytime); 
			if( task->type == TYPE_timestamp)
				thetasubselect_none(lng); 
	}
	task->lb =o;
	return MAL_SUCCEED;
}

#define leftfetchjoin_none(TPE)\
{	TPE *val, *v;\
	v= (TPE*) task->src;\
	val = (TPE*) (((char*) task->blk) + MosaicBlkSize);\
	for(; first < last; first++, val++){\
		MOSskipit();\
		*v++ = *val;\
		task->n--;\
	}\
	task->src = (char*) v;\
}

str
MOSleftfetchjoin_none(Client cntxt,  MOStask task, BUN first, BUN last)
{
	(void) cntxt;

	switch(task->type){
		case TYPE_bit: leftfetchjoin_none(bit); break;
		case TYPE_bte: leftfetchjoin_none(bte); break;
		case TYPE_sht: leftfetchjoin_none(sht); break;
		case TYPE_oid: leftfetchjoin_none(oid); break;
		case TYPE_lng: leftfetchjoin_none(lng); break;
		case TYPE_wrd: leftfetchjoin_none(wrd); break;
		case TYPE_flt: leftfetchjoin_none(flt); break;
		case TYPE_dbl: leftfetchjoin_none(dbl); break;
		case TYPE_int:
		{	int *val, *v;
			v= (int*) task->src;
			val = (int*) (((char*) task->blk) + MosaicBlkSize);
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
				leftfetchjoin_none(date); 
			if (task->type == TYPE_daytime)
				leftfetchjoin_none(daytime); 
			if (task->type == TYPE_timestamp)
				leftfetchjoin_none(lng); 
	}
	return MAL_SUCCEED;
}

#define join_none(TPE)\
{	TPE *v, *w;\
	v = (TPE*) (((char*) task->blk) + MosaicBlkSize);\
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
MOSjoin_none(Client cntxt,  MOStask task, BUN first, BUN last)
{
	BUN n;
	oid o, oo;
	(void) cntxt;

	switch(task->type){
		case TYPE_bit: join_none(bit); break;
		case TYPE_bte: join_none(bte); break;
		case TYPE_sht: join_none(sht); break;
		case TYPE_oid: join_none(oid); break;
		case TYPE_lng: join_none(lng); break;
		case TYPE_wrd: join_none(wrd); break;
		case TYPE_flt: join_none(flt); break;
		case TYPE_dbl: join_none(dbl); break;
		case TYPE_int:
		{	int *v, *w;
			v = (int*) (((char*) task->blk) + MosaicBlkSize);
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
				join_none(date); 
			if (task->type == TYPE_daytime)
				join_none(daytime); 
			if (task->type == TYPE_timestamp)
				join_none(lng); 
	}
	return MAL_SUCCEED;
}
