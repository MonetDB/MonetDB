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
#include "mosaic_literal.h"

void
MOSdump_literal(Client cntxt, MOStask task)
{
	MosaicBlk blk = task->blk;
	mnstr_printf(cntxt->fdout,"#none "BUNFMT"\n", MOScnt(blk));
}

void
MOSadvance_literal(Client cntxt, MOStask task)
{
	MosaicBlk blk = task->blk;
	(void) cntxt;

	task->start += MOScnt(blk);
	switch(task->type){
	case TYPE_bte: task->blk = (MosaicBlk)( ((char*) task->blk) + MosaicBlkSize + wordaligned(sizeof(bte)* MOScnt(blk),bte)); break ;
	case TYPE_bit: task->blk = (MosaicBlk)( ((char*) task->blk) + MosaicBlkSize + wordaligned(sizeof(bit)* MOScnt(blk),bit)); break ;
	case TYPE_sht: task->blk = (MosaicBlk)( ((char*) task->blk) + MosaicBlkSize + wordaligned(sizeof(sht)* MOScnt(blk),sht)); break ;
	case TYPE_int: task->blk = (MosaicBlk)( ((char*) task->blk) + MosaicBlkSize + wordaligned(sizeof(int)* MOScnt(blk),int)); break ;
	case TYPE_oid: task->blk = (MosaicBlk)( ((char*) task->blk) + MosaicBlkSize + wordaligned(sizeof(oid)* MOScnt(blk),oid)); break ;
	case TYPE_lng: task->blk = (MosaicBlk)( ((char*) task->blk) + MosaicBlkSize + wordaligned(sizeof(lng)* MOScnt(blk),lng)); break ;
#ifdef HAVE_HGE
	case TYPE_hge: task->blk = (MosaicBlk)( ((char*) task->blk) + MosaicBlkSize + wordaligned(sizeof(hge)* MOScnt(blk),hge)); break ;
#endif
	case TYPE_wrd: task->blk = (MosaicBlk)( ((char*) task->blk) + MosaicBlkSize + wordaligned(sizeof(wrd)* MOScnt(blk),wrd)); break ;
	case TYPE_flt: task->blk = (MosaicBlk)( ((char*) task->blk) + MosaicBlkSize + wordaligned(sizeof(flt)* MOScnt(blk),flt)); break ;
	case TYPE_dbl: task->blk = (MosaicBlk)( ((char*) task->blk) + MosaicBlkSize + wordaligned(sizeof(dbl)* MOScnt(blk),dbl)); break;
	default:
		if( task->type == TYPE_date)
			task->blk = (MosaicBlk)( ((char*) task->blk) + MosaicBlkSize + wordaligned(sizeof(date)* MOScnt(blk),date)); 
		if( task->type == TYPE_daytime)
			task->blk = (MosaicBlk)( ((char*) task->blk) + MosaicBlkSize + wordaligned(sizeof(daytime)* MOScnt(blk),daytime)); 
		if( task->type == TYPE_timestamp)
			task->blk = (MosaicBlk)( ((char*) task->blk) + MosaicBlkSize + wordaligned(sizeof(timestamp)* MOScnt(blk),timestamp)); 
	}
}

void
MOSskip_literal(Client cntxt, MOStask task)
{
	MOSadvance_literal(cntxt,task);
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
MOScompress_literal(Client cntxt, MOStask task)
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
#ifdef HAVE_HGE
	case TYPE_hge: NONEcompress(hge); break;
#endif
	case TYPE_wrd: NONEcompress(wrd); break;
	case TYPE_flt: NONEcompress(flt); break;
	case TYPE_dbl: NONEcompress(dbl); break;
	}
#ifdef _DEBUG_MOSAIC_
	MOSdump_literal(cntxt, task);
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
MOSdecompress_literal(Client cntxt, MOStask task)
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
#ifdef HAVE_HGE
	case TYPE_hge: NONEdecompress(hge); break;
#endif
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

	
#define subselect_literal(TPE) {\
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
MOSsubselect_literal(Client cntxt,  MOStask task, void *low, void *hgh, bit *li, bit *hi, bit *anti)
{
	oid *o;
	BUN first,last;
	int cmp;
	(void) cntxt;

	// set the oid range covered and advance scan range
	first = task->start;
	last = first + MOScnt(task->blk);

	if (task->cl && *task->cl > last){
		MOSskip_literal(cntxt,task);
		return MAL_SUCCEED;
	}
	o = task->lb;

	switch(task->type){
	case TYPE_bit: subselect_literal(bit); break;
	case TYPE_bte: subselect_literal(bte); break;
	case TYPE_sht: subselect_literal(sht); break;
	case TYPE_oid: subselect_literal(oid); break;
	case TYPE_lng: subselect_literal(lng); break;
#ifdef HAVE_HGE
	case TYPE_hge: subselect_literal(hge); break;
#endif
	case TYPE_wrd: subselect_literal(wrd); break;
	case TYPE_flt: subselect_literal(flt); break;
	case TYPE_dbl: subselect_literal(dbl); break;
	case TYPE_int:
	// Expanded MOSselect_literal for debugging
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
				subselect_literal(date); 
			if( task->type == TYPE_daytime)
				subselect_literal(daytime); 
			
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
	MOSskip_literal(cntxt,task);
	task->lb = o;
	return MAL_SUCCEED;
}

#define thetasubselect_literal(TPE)\
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
MOSthetasubselect_literal(Client cntxt,  MOStask task, void *val, str oper)
{
	oid *o;
	int anti=0;
	BUN first,last;
	(void) cntxt;
	
	// set the oid range covered and advance scan range
	first = task->start;
	last = first + MOScnt(task->blk);

	if (task->cl && *task->cl > last){
		MOSskip_literal(cntxt,task);
		return MAL_SUCCEED;
	}
	if ( first + MOScnt(task->blk) > last)
		last = MOScnt(task->blk);
	o = task->lb;

	switch(task->type){
	case TYPE_bit: thetasubselect_literal(bit); break;
	case TYPE_bte: thetasubselect_literal(bte); break;
	case TYPE_sht: thetasubselect_literal(sht); break;
	case TYPE_oid: thetasubselect_literal(oid); break;
	case TYPE_lng: thetasubselect_literal(lng); break;
#ifdef HAVE_HGE
	case TYPE_hge: thetasubselect_literal(hge); break;
#endif
	case TYPE_wrd: thetasubselect_literal(wrd); break;
	case TYPE_flt: thetasubselect_literal(flt); break;
	case TYPE_dbl: thetasubselect_literal(dbl); break;
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
				thetasubselect_literal(date); 
			if( task->type == TYPE_daytime)
				thetasubselect_literal(daytime); 
			if( task->type == TYPE_timestamp)
				thetasubselect_literal(lng); 
	}
	MOSskip_literal(cntxt,task);
	task->lb =o;
	return MAL_SUCCEED;
}

#define leftfetchjoin_literal(TPE)\
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
MOSleftfetchjoin_literal(Client cntxt,  MOStask task)
{
	BUN first,last;
	(void) cntxt;
	// set the oid range covered and advance scan range
	first = task->start;
	last = first + MOScnt(task->blk);

	switch(task->type){
		case TYPE_bit: leftfetchjoin_literal(bit); break;
		case TYPE_bte: leftfetchjoin_literal(bte); break;
		case TYPE_sht: leftfetchjoin_literal(sht); break;
		case TYPE_oid: leftfetchjoin_literal(oid); break;
		case TYPE_lng: leftfetchjoin_literal(lng); break;
#ifdef HAVE_HGE
		case TYPE_hge: leftfetchjoin_literal(hge); break;
#endif
		case TYPE_wrd: leftfetchjoin_literal(wrd); break;
		case TYPE_flt: leftfetchjoin_literal(flt); break;
		case TYPE_dbl: leftfetchjoin_literal(dbl); break;
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
				leftfetchjoin_literal(date); 
			if (task->type == TYPE_daytime)
				leftfetchjoin_literal(daytime); 
			if (task->type == TYPE_timestamp)
				leftfetchjoin_literal(lng); 
	}
	MOSskip_literal(cntxt,task);
	return MAL_SUCCEED;
}

#define join_literal(TPE)\
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
MOSjoin_literal(Client cntxt,  MOStask task)
{
	BUN n,first,last;
	oid o, oo;
	(void) cntxt;
	// set the oid range covered and advance scan range
	first = task->start;
	last = first + MOScnt(task->blk);

	switch(task->type){
		case TYPE_bit: join_literal(bit); break;
		case TYPE_bte: join_literal(bte); break;
		case TYPE_sht: join_literal(sht); break;
		case TYPE_oid: join_literal(oid); break;
		case TYPE_lng: join_literal(lng); break;
#ifdef HAVE_HGE
		case TYPE_hge: join_literal(hge); break;
#endif
		case TYPE_wrd: join_literal(wrd); break;
		case TYPE_flt: join_literal(flt); break;
		case TYPE_dbl: join_literal(dbl); break;
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
				join_literal(date); 
			if (task->type == TYPE_daytime)
				join_literal(daytime); 
			if (task->type == TYPE_timestamp)
				join_literal(lng); 
	}
	MOSskip_literal(cntxt,task);
	return MAL_SUCCEED;
}
