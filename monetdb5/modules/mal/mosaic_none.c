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
MOSskip_none(MOStask task)
{
	MosaicBlk blk = task->blk;
	switch(task->type){
	case TYPE_bte: task->blk = (MosaicBlk)( ((char*) task->blk) + MosaicBlkSize + wordaligned(sizeof(bte)* blk->cnt)); break ;
	case TYPE_bit: task->blk = (MosaicBlk)( ((char*) task->blk) + MosaicBlkSize + wordaligned(sizeof(bit)* blk->cnt)); break ;
	case TYPE_sht: task->blk = (MosaicBlk)( ((char*) task->blk) + MosaicBlkSize + wordaligned(sizeof(sht)* blk->cnt)); break ;
	case TYPE_int: task->blk = (MosaicBlk)( ((char*) task->blk) + MosaicBlkSize + wordaligned(sizeof(int)* blk->cnt)); break ;
	case TYPE_lng: task->blk = (MosaicBlk)( ((char*) task->blk) + MosaicBlkSize + wordaligned(sizeof(lng)* blk->cnt)); break ;
	case TYPE_flt: task->blk = (MosaicBlk)( ((char*) task->blk) + MosaicBlkSize + wordaligned(sizeof(flt)* blk->cnt)); break ;
	case TYPE_dbl: task->blk = (MosaicBlk)( ((char*) task->blk) + MosaicBlkSize + wordaligned(sizeof(dbl)* blk->cnt)); 
	}
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

	switch(task->type){
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
	default:
		if( task->type == TYPE_timestamp)
			NONEcompress(timestamp); 
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
MOSdecompress_none( MOStask task)
{
	MosaicBlk blk = (MosaicBlk) task->blk;
	BUN i;
	lng clk = GDKusec();
	char *compressed;

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

#define MOSselect_none(TPE)/* TBD */

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

	switch(task->type){
	case TYPE_bit: MOSselect_none(bit); break;
	case TYPE_bte: MOSselect_none(bte); break;
	case TYPE_sht: MOSselect_none(sht); break;
	case TYPE_lng: MOSselect_none(lng); break;
	case TYPE_flt: MOSselect_none(flt); break;
	case TYPE_dbl: MOSselect_none(dbl); break;
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

static str
MOSthetasubselect_none(Client cntxt,  MOStask task, lng first, lng last, void *val, str oper)
{
	oid *o;
	int anti=0;
	(void) cntxt;
	
	if ( first + task->blk->cnt > last)
		last = task->blk->cnt;
	o = task->lb;

	switch(task->type){
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
/*
static str
MOSleftfetchjoin_none(Client cntxt,  MOStask task){
	(void) cntxt;
	(void) task;
	return MAL_SUCCEED;
}
static str
MOSjoin_none(Client cntxt,  MOStask task){
	(void) cntxt;
	(void) task;
	return MAL_SUCCEED;
}
*/
