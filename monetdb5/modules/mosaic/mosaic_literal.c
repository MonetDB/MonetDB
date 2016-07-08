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
 * Use a chunk that has not been compressed
 */

#include "monetdb_config.h"
#include "mosaic.h"
#include "mosaic_literal.h"

void
MOSdump_literal(Client cntxt, MOStask task)
{
	MosaicBlk blk = task->blk;
	mnstr_printf(cntxt->fdout,"#none "BUNFMT"\n", MOSgetCnt(blk));
}

void
MOSlayout_literal(Client cntxt, MOStask task, BAT *btech, BAT *bcount, BAT *binput, BAT *boutput, BAT *bproperties)
{
	MosaicBlk blk = (MosaicBlk) task->blk;
	lng cnt = MOSgetCnt(blk), input=0, output= 0;

	(void) cntxt;
	BUNappend(btech, "literal", FALSE);
	BUNappend(bcount, &cnt, FALSE);
	input = cnt * ATOMsize(task->type);
	switch(ATOMbasetype(task->type)){
	case TYPE_bte: output = wordaligned( MosaicBlkSize + sizeof(bte)* MOSgetCnt(blk),bte); break ;
	case TYPE_bit: output = wordaligned( MosaicBlkSize + sizeof(bit)* MOSgetCnt(blk),bit); break ;
	case TYPE_sht: output = wordaligned( MosaicBlkSize + sizeof(sht)* MOSgetCnt(blk),sht); break ;
	case TYPE_int: output = wordaligned( MosaicBlkSize + sizeof(int)* MOSgetCnt(blk),int); break ;
	case TYPE_oid: output = wordaligned( MosaicBlkSize + sizeof(oid)* MOSgetCnt(blk),oid); break ;
	case TYPE_lng: output = wordaligned( MosaicBlkSize + sizeof(lng)* MOSgetCnt(blk),lng); break ;
#ifdef HAVE_HGE
	case TYPE_hge: output = wordaligned( MosaicBlkSize + sizeof(hge)* MOSgetCnt(blk),hge); break ;
#endif
	case TYPE_flt: output = wordaligned( MosaicBlkSize + sizeof(flt)* MOSgetCnt(blk),flt); break ;
	case TYPE_dbl: output = wordaligned( MosaicBlkSize + sizeof(dbl)* MOSgetCnt(blk),dbl); break;
	case TYPE_str:
		switch(task->bsrc->twidth){
		case 1: output = wordaligned( MosaicBlkSize + sizeof(bte)* MOSgetCnt(blk),bte); break ;
		case 2: output = wordaligned( MosaicBlkSize + sizeof(sht)* MOSgetCnt(blk),sht); break ;
		case 4: output = wordaligned( MosaicBlkSize + sizeof(int)* MOSgetCnt(blk),int); break ;
		case 8: output = wordaligned( MosaicBlkSize + sizeof(lng)* MOSgetCnt(blk),lng); break ;
		}
	}
	BUNappend(binput, &input, FALSE);
	BUNappend(boutput, &output, FALSE);
	BUNappend(bproperties, "", FALSE);
}

void
MOSadvance_literal(Client cntxt, MOStask task)
{
	MosaicBlk blk = task->blk;
	(void) cntxt;

	task->start += MOSgetCnt(blk);
	switch(ATOMbasetype(task->type)){
	case TYPE_bte: task->blk = (MosaicBlk)( ((char*) task->blk) + wordaligned( MosaicBlkSize + sizeof(bte)* MOSgetCnt(blk),bte)); break ;
	case TYPE_bit: task->blk = (MosaicBlk)( ((char*) task->blk) + wordaligned( MosaicBlkSize + sizeof(bit)* MOSgetCnt(blk),bit)); break ;
	case TYPE_sht: task->blk = (MosaicBlk)( ((char*) task->blk) + wordaligned( MosaicBlkSize + sizeof(sht)* MOSgetCnt(blk),sht)); break ;
	case TYPE_int: task->blk = (MosaicBlk)( ((char*) task->blk) + wordaligned( MosaicBlkSize + sizeof(int)* MOSgetCnt(blk),int)); break ;
	case TYPE_oid: task->blk = (MosaicBlk)( ((char*) task->blk) + wordaligned( MosaicBlkSize + sizeof(oid)* MOSgetCnt(blk),oid)); break ;
	case TYPE_lng: task->blk = (MosaicBlk)( ((char*) task->blk) + wordaligned( MosaicBlkSize + sizeof(lng)* MOSgetCnt(blk),lng)); break ;
#ifdef HAVE_HGE
	case TYPE_hge: task->blk = (MosaicBlk)( ((char*) task->blk) + wordaligned( MosaicBlkSize + sizeof(hge)* MOSgetCnt(blk),hge)); break ;
#endif
	case TYPE_flt: task->blk = (MosaicBlk)( ((char*) task->blk) + wordaligned( MosaicBlkSize + sizeof(flt)* MOSgetCnt(blk),flt)); break ;
	case TYPE_dbl: task->blk = (MosaicBlk)( ((char*) task->blk) + wordaligned( MosaicBlkSize + sizeof(dbl)* MOSgetCnt(blk),dbl)); break;
	case TYPE_str:
		switch(task->bsrc->twidth){
		case 1: task->blk = (MosaicBlk)( ((char*) task->blk) + wordaligned( MosaicBlkSize + sizeof(bte)* MOSgetCnt(blk),bte)); break ;
		case 2: task->blk = (MosaicBlk)( ((char*) task->blk) + wordaligned( MosaicBlkSize + sizeof(sht)* MOSgetCnt(blk),sht)); break ;
		case 4: task->blk = (MosaicBlk)( ((char*) task->blk) + wordaligned( MosaicBlkSize + sizeof(int)* MOSgetCnt(blk),int)); break ;
		case 8: task->blk = (MosaicBlk)( ((char*) task->blk) + wordaligned( MosaicBlkSize + sizeof(lng)* MOSgetCnt(blk),lng)); break ;
		}
	}
}

void
MOSskip_literal(Client cntxt, MOStask task)
{
	MOSadvance_literal(cntxt,task);
	if ( MOSgetTag(task->blk) == MOSAIC_EOL)
		task->blk = 0; // ENDOFLIST
}


// append a series of values into the non-compressed block

#define LITERALcompress(TYPE)\
{	*(TYPE*) task->dst = ((TYPE*) task->src)[task->start];\
	hdr->checksum.sum##TYPE += *(TYPE*) task->dst;\
	task->dst += sizeof(TYPE);\
	MOSincCnt(blk,1);\
}

// rather expensive simple value non-compressed store
void
MOScompress_literal(Client cntxt, MOStask task)
{
	MosaicHdr hdr = task->hdr;
	MosaicBlk blk = (MosaicBlk) task->blk;

	(void) cntxt;
	MOSsetTag(blk,MOSAIC_NONE);

	switch(ATOMbasetype(task->type)){
	case TYPE_bte: LITERALcompress(bte); break ;
	case TYPE_bit: LITERALcompress(bit); break ;
	case TYPE_sht: LITERALcompress(sht); break;
	case TYPE_oid: LITERALcompress(oid); break;
	case TYPE_lng: LITERALcompress(lng); break;
	case TYPE_flt: LITERALcompress(flt); break;
	case TYPE_dbl: LITERALcompress(dbl); break;
#ifdef HAVE_HGE
	case TYPE_hge: LITERALcompress(hge); break;
#endif
	case TYPE_int:
	{	*(int*) task->dst = ((int*) task->src)[task->start];
		hdr->checksum.sumint += *(int*) task->dst;
		task->dst += sizeof(int);
		MOSincCnt(blk,1);
	}
	break;
	case TYPE_str:
		switch(task->bsrc->twidth){
		case 1: LITERALcompress(bte); break ;
		case 2: LITERALcompress(sht); break ;
		case 4: LITERALcompress(int); break ;
		case 8: LITERALcompress(lng); break ;
		}
	}
#ifdef _DEBUG_MOSAIC_
	MOSdump_literal(cntxt, task);
#endif
}

// the inverse operator, extend the src
#define LITERALdecompress(TYPE)\
{ BUN lim = MOSgetCnt(blk); \
	for(i = 0; i < lim; i++) {\
	((TYPE*)task->src)[i] = ((TYPE*)compressed)[i]; \
	hdr->checksum2.sum##TYPE += ((TYPE*)compressed)[i]; \
	}\
	task->src += i * sizeof(TYPE);\
}

void
MOSdecompress_literal(Client cntxt, MOStask task)
{
	MosaicHdr hdr = task->hdr;
	MosaicBlk blk = (MosaicBlk) task->blk;
	BUN i;
	char *compressed;
	(void) cntxt;

	compressed = ((char*)blk) + MosaicBlkSize;
	switch(ATOMbasetype(task->type)){
	case TYPE_bte: LITERALdecompress(bte); break ;
	case TYPE_bit: LITERALdecompress(bit); break ;
	case TYPE_sht: LITERALdecompress(sht); break;
	case TYPE_oid: LITERALdecompress(oid); break;
	case TYPE_lng: LITERALdecompress(lng); break;
	case TYPE_flt: LITERALdecompress(flt); break;
	case TYPE_dbl: LITERALdecompress(dbl); break;
#ifdef HAVE_HGE
	case TYPE_hge: LITERALdecompress(hge); break;
#endif
	case TYPE_int:
	{ BUN lim = MOSgetCnt(blk);	
		for(i = 0; i < lim; i++) {
			((int*)task->src)[i] = ((int*)compressed)[i];
			hdr->checksum2.sumint += ((int*)compressed)[i]; \
		}
		task->src += i * sizeof(int);
	}
	break;
	case TYPE_str:
		switch(task->bsrc->twidth){
		case 1: LITERALdecompress(bte); break ;
		case 2: LITERALdecompress(sht); break ;
		case 4: LITERALdecompress(int); break ;
		case 8: LITERALdecompress(lng); break ;
		}
	}
}

// The remainder should provide the minimal algebraic framework
//  to apply the operator to a LITERAL compressed chunk

	
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
	last = first + MOSgetCnt(task->blk);

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
	case TYPE_flt: subselect_literal(flt); break;
	case TYPE_dbl: subselect_literal(dbl); break;
#ifdef HAVE_HGE
	case TYPE_hge: subselect_literal(hge); break;
#endif
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
	case TYPE_str:
		// beware we should look at the value
		switch(task->bsrc->twidth){
		case 1: break ;
		case 2: break ;
		case 4: break ;
		case 8: break ;
		}
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
	last = first + MOSgetCnt(task->blk);

	if (task->cl && *task->cl > last){
		MOSskip_literal(cntxt,task);
		return MAL_SUCCEED;
	}
	if ( first + MOSgetCnt(task->blk) > last)
		last = MOSgetCnt(task->blk);
	o = task->lb;

	switch(ATOMbasetype(task->type)){
	case TYPE_bit: thetasubselect_literal(bit); break;
	case TYPE_bte: thetasubselect_literal(bte); break;
	case TYPE_sht: thetasubselect_literal(sht); break;
	case TYPE_oid: thetasubselect_literal(oid); break;
	case TYPE_lng: thetasubselect_literal(lng); break;
	case TYPE_flt: thetasubselect_literal(flt); break;
	case TYPE_dbl: thetasubselect_literal(dbl); break;
#ifdef HAVE_HGE
	case TYPE_hge: thetasubselect_literal(hge); break;
#endif
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
	case TYPE_str:
		// beware we should look at the value
		switch(task->bsrc->twidth){
		case 1: break ;
		case 2: break ;
		case 4: break ;
		case 8: break ;
		}
	}
	MOSskip_literal(cntxt,task);
	task->lb =o;
	return MAL_SUCCEED;
}

#define projection_literal(TPE)\
{	TPE *val, *v;\
	v= (TPE*) task->src;\
	val = (TPE*) (((char*) task->blk) + MosaicBlkSize);\
	for(; first < last; first++, val++){\
		MOSskipit();\
		*v++ = *val;\
		task->n--;\
		task->cnt++;\
	}\
	task->src = (char*) v;\
}

str
MOSprojection_literal(Client cntxt,  MOStask task)
{
	BUN first,last;
	(void) cntxt;
	// set the oid range covered and advance scan range
	first = task->start;
	last = first + MOSgetCnt(task->blk);

	switch(ATOMbasetype(task->type)){
		case TYPE_bit: projection_literal(bit); break;
		case TYPE_bte: projection_literal(bte); break;
		case TYPE_sht: projection_literal(sht); break;
		case TYPE_oid: projection_literal(oid); break;
		case TYPE_lng: projection_literal(lng); break;
		case TYPE_flt: projection_literal(flt); break;
		case TYPE_dbl: projection_literal(dbl); break;
#ifdef HAVE_HGE
		case TYPE_hge: projection_literal(hge); break;
#endif
		case TYPE_int:
		{	int *val, *v;
			v= (int*) task->src;
			val = (int*) (((char*) task->blk) + MosaicBlkSize);
			for(; first < last; first++, val++){
				MOSskipit();
				*v++ = *val;
				task->n--;
				task->cnt++;
			}
			task->src = (char*) v;
		}
	break;
	case TYPE_str:
		switch(task->bsrc->twidth){
		case 1: projection_literal(bte); break ;
		case 2: projection_literal(sht); break ;
		case 4: projection_literal(int); break ;
		case 8: projection_literal(lng); break ;
		}
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
MOSsubjoin_literal(Client cntxt,  MOStask task)
{
	BUN n,first,last;
	oid o, oo;
	(void) cntxt;
	// set the oid range covered and advance scan range
	first = task->start;
	last = first + MOSgetCnt(task->blk);

	switch(ATOMbasetype(task->type)){
		case TYPE_bit: join_literal(bit); break;
		case TYPE_bte: join_literal(bte); break;
		case TYPE_sht: join_literal(sht); break;
		case TYPE_oid: join_literal(oid); break;
		case TYPE_lng: join_literal(lng); break;
		case TYPE_flt: join_literal(flt); break;
		case TYPE_dbl: join_literal(dbl); break;
#ifdef HAVE_HGE
		case TYPE_hge: join_literal(hge); break;
#endif
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
	case TYPE_str:
		// beware we should look at the value
		switch(task->bsrc->twidth){
		case 1: join_literal(bte); break ;
		case 2: join_literal(sht); break ;
		case 4: join_literal(int); break ;
		case 8: join_literal(lng); break ;
		}
	}
	MOSskip_literal(cntxt,task);
	return MAL_SUCCEED;
}
