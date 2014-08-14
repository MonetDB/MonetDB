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

static void
MOSdump_none(Client cntxt, MOStask task)
{
	MosaicBlk blk = task->blk;
	mnstr_printf(cntxt->fdout,"#none "LLFMT"\n", (lng)(blk->cnt));
}

static void
MOSadvance_none(MOStask task)
{
	MosaicBlk blk = task->blk;
	switch(task->type){
	case TYPE_bte: task->blk = (MosaicBlk)( ((char*) task->blk) + MosaicBlkSize + wordaligned(sizeof(bte)* blk->cnt)); break ;
	case TYPE_bit: task->blk = (MosaicBlk)( ((char*) task->blk) + MosaicBlkSize + wordaligned(sizeof(bit)* blk->cnt)); break ;
	case TYPE_sht: task->blk = (MosaicBlk)( ((char*) task->blk) + MosaicBlkSize + wordaligned(sizeof(sht)* blk->cnt)); break ;
	case TYPE_int: task->blk = (MosaicBlk)( ((char*) task->blk) + MosaicBlkSize + wordaligned(sizeof(int)* blk->cnt)); break ;
	case TYPE_lng: task->blk = (MosaicBlk)( ((char*) task->blk) + MosaicBlkSize + wordaligned(sizeof(lng)* blk->cnt)); break ;
	case TYPE_flt: task->blk = (MosaicBlk)( ((char*) task->blk) + MosaicBlkSize + wordaligned(sizeof(flt)* blk->cnt)); break ;
	case TYPE_dbl: task->blk = (MosaicBlk)( ((char*) task->blk) + MosaicBlkSize + wordaligned(sizeof(dbl)* blk->cnt)); break;
	default:
		if( task->type == TYPE_timestamp)
			task->blk = (MosaicBlk)( ((char*) task->blk) + MosaicBlkSize + wordaligned(sizeof(timestamp)* blk->cnt)); 
	}
}

static void
MOSskip_none(MOStask task)
{
	MOSadvance_none(task);
	if ( task->blk->tag == MOSAIC_EOL)
		task->blk = 0; // ENDOFLIST
}


// append a series of values into the non-compressed block

#define NONEcompress(TYPE)\
{	*(TYPE*) task->dst = *(TYPE*) task->src;\
	task->src += sizeof(TYPE);\
	task->dst += sizeof(TYPE);\
	blk->cnt ++;\
	task->elm--;\
}

// rather expensive simple value non-compressed store
static void
MOScompress_none(Client cntxt, MOStask task)
{
	MosaicBlk blk = (MosaicBlk) task->blk;

	(void) cntxt;
	blk->tag = MOSAIC_NONE;
    task->time[MOSAIC_NONE] = GDKusec();

	switch(ATOMstorage(task->type)){
	case TYPE_bte: NONEcompress(bte); break ;
	case TYPE_bit: NONEcompress(bit); break ;
	case TYPE_sht: NONEcompress(sht); break;
	case TYPE_int:
	{	*(int*) task->dst = *(int*) task->src;
		task->src += sizeof(int);
		task->dst += sizeof(int);
		blk->cnt ++;
		task->elm--;
	}
		break;
	case TYPE_lng: NONEcompress(lng); break;
	case TYPE_flt: NONEcompress(flt); break;
	case TYPE_dbl: NONEcompress(dbl); break;
	}
#ifdef _DEBUG_MOSAIC_
	MOSdump_none_(cntxt, task);
#endif
    task->time[MOSAIC_NONE] = GDKusec() - task->time[MOSAIC_NONE];
}

// the inverse operator, extend the src
#define NONEdecompress(TYPE)\
{ for(i = 0; i < (BUN) blk->cnt; i++) \
	((TYPE*)task->src)[i] = ((TYPE*)compressed)[i]; \
	task->src += i * sizeof(TYPE);\
}

static void
MOSdecompress_none(Client cntxt, MOStask task)
{
	MosaicBlk blk = (MosaicBlk) task->blk;
	BUN i;
	lng clk = GDKusec();
	char *compressed;
	(void) cntxt;

	compressed = ((char*)blk) + MosaicBlkSize;
	switch(task->type){
	case TYPE_bte: NONEdecompress(bte); break ;
	case TYPE_bit: NONEdecompress(bit); break ;
	case TYPE_sht: NONEdecompress(sht); break;
	case TYPE_int:
	{	
		for(i = 0; i < (BUN) blk->cnt; i++) 
			((int*)task->src)[i] = ((int*)compressed)[i];
		task->src += i * sizeof(int);
	}
		break;
	case TYPE_lng: NONEdecompress(lng); break;
	case TYPE_flt: NONEdecompress(flt); break;
	case TYPE_dbl: NONEdecompress(dbl); break;
	default:
		if( task->type == TYPE_timestamp)
			NONEdecompress(timestamp);
	}
    task->time[MOSAIC_NONE] = GDKusec() - clk;
}

// The remainder should provide the minimal algebraic framework
//  to apply the operator to a NONE compressed chunk

	
// skip until you hit a candidate
#define MOSskipit()\
if ( task->cl && task->n){\
	while( *task->cl < (oid) first)\
		{task->cl++; task->n--;}\
	if ( *task->cl > (oid) first  || task->n ==0)\
		continue;\
	if ( *task->cl == (oid) first ){\
		task->cl++; task->n--;\
	}\
} else if (task->cl) continue;

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

static str
MOSsubselect_none(Client cntxt,  MOStask task, lng first, lng last, void *low, void *hgh, bit *li, bit *hi, bit *anti)
{
	oid *o;
	int cmp;
	(void) cntxt;
	(void) low;
	(void) hgh;
	(void) li;
	(void) hi;
	(void) anti;

	if ( first + task->blk->cnt > last)
		last = task->blk->cnt;
	o = task->lb;

	switch(ATOMstorage(task->type)){
	case TYPE_bit: subselect_none(bit); break;
	case TYPE_bte: subselect_none(bte); break;
	case TYPE_sht: subselect_none(sht); break;
	case TYPE_lng: subselect_none(lng); break;
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
		if( task->type == TYPE_timestamp){
			//MOSselect_none(timestamp);
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

static str
MOSthetasubselect_none(Client cntxt,  MOStask task, lng first, lng last, void *val, str oper)
{
	oid *o;
	int anti=0;
	(void) cntxt;
	
	if ( first + task->blk->cnt > last)
		last = task->blk->cnt;
	o = task->lb;

	switch(ATOMstorage(task->type)){
	case TYPE_bit: thetasubselect_none(bit); break;
	case TYPE_bte: thetasubselect_none(bte); break;
	case TYPE_sht: thetasubselect_none(sht); break;
	case TYPE_lng: thetasubselect_none(lng); break;
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
	}
	task->lb =o;
	return MAL_SUCCEED;
}

#define leftfetchjoin_none(TPE)\
{	TPE *val, *v;\
	v= (TPE*) task->src;\
	val = (TPE*) (((char*) task->hdr) + MosaicBlkSize);\
	for(; first < last; first++, val++){\
		MOSskipit();\
		*v++ = *val;\
		task->n--;\
	}\
	task->src = (char*) v;\
}

static str
MOSleftfetchjoin_none(Client cntxt,  MOStask task, BUN first, BUN last)
{
	(void) cntxt;

	switch(ATOMstorage(task->type)){
		case TYPE_bit: leftfetchjoin_none(bit); break;
		case TYPE_bte: leftfetchjoin_none(bte); break;
		case TYPE_sht: leftfetchjoin_none(sht); break;
		case TYPE_lng: leftfetchjoin_none(lng); break;
		case TYPE_flt: leftfetchjoin_none(flt); break;
		case TYPE_dbl: leftfetchjoin_none(dbl); break;
		case TYPE_int:
		{	int *val, *v;
			v= (int*) task->src;
			val = (int*) (((char*) task->hdr) + MosaicBlkSize);
			for(; first < last; first++, val++){
				MOSskipit();
				*v++ = *val;
				task->n--;
			}
			task->src = (char*) v;
		}
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

static str
MOSjoin_none(Client cntxt,  MOStask task, BUN first, BUN last)
{
	BUN n;
	oid o, oo;
	(void) cntxt;

	switch(ATOMstorage(task->type)){
		case TYPE_bit: join_none(bit); break;
		case TYPE_bte: join_none(bte); break;
		case TYPE_sht: join_none(sht); break;
		case TYPE_lng: join_none(lng); break;
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
	}
	return MAL_SUCCEED;
}
