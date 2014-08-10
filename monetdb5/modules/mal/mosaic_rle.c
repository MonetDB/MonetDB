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

/* Beware, the dump routines use the compressed part of the task */
static void
MOSdump_rle(Client cntxt, MOStask task)
{
	MosaicBlk blk= task->blk;
	void *val = (void*)(((char*) blk) + MosaicBlkSize);

	mnstr_printf(cntxt->fdout,"#rle "LLFMT" ", (lng)(blk->cnt));
	switch(task->type){
	case TYPE_bte:
		mnstr_printf(cntxt->fdout,"bte %d", *(int*) val); break;
	case TYPE_sht:
		mnstr_printf(cntxt->fdout,"sht %d", *(int*) val); break;
	case TYPE_int:
		mnstr_printf(cntxt->fdout,"int %d", *(int*) val); break;
	case  TYPE_lng:
		mnstr_printf(cntxt->fdout,"int "LLFMT, *(lng*) val); break;
	case TYPE_flt:
		mnstr_printf(cntxt->fdout,"flt  %f", *(flt*) val); break;
	case TYPE_dbl:
		mnstr_printf(cntxt->fdout,"flt  %f", *(dbl*) val); break;
	default:
		if( task->type == TYPE_timestamp){
		}
	}
	mnstr_printf(cntxt->fdout,"\n");
}

static void
MOSadvance_rle(MOStask task)
{
	switch(task->type){
	case TYPE_bte: task->blk = (MosaicBlk)( ((char*)task->blk) + MosaicBlkSize + wordaligned(sizeof(bte))); break;
	case TYPE_bit: task->blk = (MosaicBlk)( ((char*)task->blk) + MosaicBlkSize + wordaligned(sizeof(bit))); break;
	case TYPE_sht: task->blk = (MosaicBlk)( ((char*)task->blk) + MosaicBlkSize + wordaligned(sizeof(sht))); break;
	case TYPE_int: task->blk = (MosaicBlk)( ((char*)task->blk) + MosaicBlkSize + wordaligned(sizeof(int))); break;
	case TYPE_lng: task->blk = (MosaicBlk)( ((char*)task->blk) + MosaicBlkSize + wordaligned(sizeof(lng))); break;
	case TYPE_flt: task->blk = (MosaicBlk)( ((char*)task->blk) + MosaicBlkSize + wordaligned(sizeof(flt))); break;
	case TYPE_dbl: task->blk = (MosaicBlk)( ((char*)task->blk) + MosaicBlkSize + wordaligned(sizeof(dbl))); break;
	default:
		if( task->type == TYPE_timestamp){
		}
	}
}

static void
MOSskip_rle(MOStask task)
{
	MOSadvance_rle(task);
	if ( task->blk->tag == MOSAIC_EOL)
		task->blk = 0; // ENDOFLIST
}

#define Estimate(TYPE)\
{	TYPE val = *(TYPE*) task->src;\
	for(i =1; i < task->elm; i++)\
	if ( ((TYPE*)task->src)[i] != val)\
		break;\
	chunksize = i;\
}

// calculate the expected reduction using RLE in terms of elements compressed
static lng
MOSestimate_rle(Client cntxt, MOStask task)
{	BUN i = -1;
	lng chunksize = 0;
	(void) cntxt;

	switch(task->type){
	case TYPE_bte: Estimate(bte); break;
	case TYPE_bit: Estimate(bit); break;
	case TYPE_sht: Estimate(sht); break;
	case TYPE_int:
	{	int val = *(int*)task->src;
		for(i =1; i<task->elm; i++)
		if ( ((int*)task->src)[i] != val)
			break;
		chunksize = i;
	}
	break;
	case TYPE_lng: Estimate(lng); break;
	case TYPE_flt: Estimate(flt); break;
	case TYPE_dbl: Estimate(dbl); break;
	default:
		if( task->type == TYPE_timestamp)
		{	timestamp val = *(timestamp*) task->src;
			for(i =1; i<task->elm; i++)
			if ( !( ((timestamp*)task->src)[i].days == val.days && ((timestamp*)task->src)[i].msecs == val.msecs))
				break;
			chunksize = i;
		}
	}
	return chunksize;
}

// insert a series of values into the compressor block using rle.
#define RLEcompress(TYPE)\
	{	TYPE val = *(TYPE*) task->src;\
		TYPE *dst = (TYPE*) task->dst;\
		*dst = val;\
		for(i =1; i<task->elm; i++)\
		if ( ((TYPE*)task->src)[i] != val)\
			break;\
		blk->cnt = i;\
		task->dst +=  sizeof(TYPE);\
		task->src += i * sizeof(TYPE);\
	}

static void
MOScompress_rle(Client cntxt, MOStask task)
{
	BUN i ;
	MosaicBlk blk = task->blk;

	(void) cntxt;
	blk->tag = MOSAIC_RLE;
	blk->cnt =  0;
	task->time[MOSAIC_RLE] = GDKusec();

	switch(task->type){
	case TYPE_bte: RLEcompress(bte); break ;
	case TYPE_bit: RLEcompress(bit); break;
	case TYPE_sht: RLEcompress(sht); break;
	case TYPE_int:
	{	int val = *(int*) task->src;
		int *dst = (int*) task->dst;
		*dst = val;
		for(i =1; i<task->elm; i++)
		if ( ((int*)task->src)[i] != val)
			break;
		blk->cnt = i;
		task->dst +=  sizeof(int);
		task->src += i * sizeof(int);
	}
		break;
	case TYPE_lng: RLEcompress(lng); break;
	case TYPE_flt: RLEcompress(flt); break;
	case TYPE_dbl: RLEcompress(dbl); break;
	default:
		if( task->type == TYPE_timestamp){
			timestamp val = *(timestamp*) task->src;
			timestamp *dst = (timestamp*) task->dst;
			*dst = val;
			for(i =1; i<task->elm; i++)
			if ( !(((timestamp*)task->src)[i].days == val.days && ((timestamp*)task->src)[i].msecs == val.msecs))
				break;
			blk->cnt = i;
			task->src += i * sizeof(timestamp);
		}
	}
#ifdef _DEBUG_MOSAIC_
	MOSdump_rle(cntxt, task);
#endif
	task->time[MOSAIC_RLE] = GDKusec() - task->time[MOSAIC_RLE];
}

// the inverse operator, extend the src
#define RLEdecompress(TYPE)\
{	TYPE val = *(TYPE*) task->dst;\
	for(i = 0; i < (BUN) blk->cnt; i++)\
		((TYPE*)task->src)[i] = val;\
	task->src += i * sizeof(TYPE);\
}

static void
MOSdecompress_rle( MOStask task)
{
	MosaicBlk blk =  ((MosaicBlk) task->blk);
	BUN i;
	lng clk = GDKusec();
	char *compressed;

	compressed = (char*) blk + MosaicBlkSize;
	switch(task->type){
	case TYPE_bte: RLEdecompress(bte); break ;
	case TYPE_bit: RLEdecompress(bit); break ;
	case TYPE_sht: RLEdecompress(sht); break;
	case TYPE_int:
	{	int val = *(int*) compressed ;
		for(i = 0; i < (BUN) blk->cnt; i++)
			((int*)task->src)[i] = val;
		task->src += i * sizeof(int);
	}
		break;
	case TYPE_lng: RLEdecompress(lng); break;
	case TYPE_flt: RLEdecompress(flt); break;
	case TYPE_dbl: RLEdecompress(dbl); break;
	default:
		if( task->type == TYPE_timestamp)
		{	timestamp val = *(timestamp*) compressed;

			for(i = 0; i < (BUN) blk->cnt; i++)
				((timestamp*)task->src)[i] = val;
			task->src += i * sizeof(timestamp);
		}
	}
	task->time[MOSAIC_RLE] = GDKusec() - clk;
}

// perform relational algebra operators over non-compressed chunks
// They are bound by an oid range and possibly a candidate list

#define  MOSselect_rle(TPE) /* TBD */

static str
MOSsubselect_rle(Client cntxt,  MOStask task, BUN first, BUN last, void *low, void *hgh, bit *li, bit *hi, bit *anti){
	oid *o;
	int cmp;
	(void) cntxt;

	if ( first + task->blk->cnt > last)
		last = task->blk->cnt;
	o = task->lb;

	switch(task->type){
	case TYPE_bit: MOSselect_rle(bit); break;
	case TYPE_bte: MOSselect_rle(bte); break;
	case TYPE_sht: MOSselect_rle(sht); break;
	case TYPE_lng: MOSselect_rle(lng); break;
	case TYPE_flt: MOSselect_rle(flt); break;
	case TYPE_dbl: MOSselect_rle(dbl); break;
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
				for( ; first < last; first++, val++){
					MOSskipit();
					*o++ = (oid) first;
				}
			} else{
				cmp  =  ((*hi && *(int*)val <= * (int*)hgh ) || (!*hi && *(int*)val < *(int*)hgh )) &&
						((*li && *(int*)val >= * (int*)low ) || (!*li && *(int*)val > *(int*)low ));
				if (!cmp)
				for( ; first < last; first++, val++){
					MOSskipit();
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

	(void) cntxt;
	(void) task;
	(void) low;
	(void) hgh;
	(void) li;
	(void) hi;
	(void) anti;
	(void) first;
	(void) last;
	switch( task->type){
	case TYPE_int:
		// position the dst reader on first qualifying value
		// inspect all non-compressed value
	default:
		;
	}
	return MAL_SUCCEED;
}
/*
static str
MOSthetasubselect_none(Client cntxt,  MOStask task, void *low, void *hgh, int li, int hi, int anti){
	(void) cntxt;
	(void) task;
	(void) low;
	(void) hgh;
	(void) li;
	(void) hi;
	(void) anti;
	return MAL_SUCCEED;
}
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
