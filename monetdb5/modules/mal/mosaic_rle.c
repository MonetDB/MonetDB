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
MOSdump_rle_(Client cntxt, MOStask task)
{
	MosaicBlk blk= task->hdr;
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
MOSdump_rle(Client cntxt, MOStask task)
{
	MOSdump_rle_(cntxt,task);
	assert(0 <= (lng) task->hdr->cnt && (lng) task->hdr->cnt <= (lng) task->elm);
	task->elm -= (BUN) task->hdr->cnt;
	switch(task->type){
	case TYPE_bte: task->hdr = (MosaicBlk)( ((char*)task->hdr) + MosaicBlkSize + sizeof(bte)); break;
	case TYPE_bit: task->hdr = (MosaicBlk)( ((char*)task->hdr) + MosaicBlkSize + sizeof(bit)); break;
	case TYPE_sht: task->hdr = (MosaicBlk)( ((char*)task->hdr) + MosaicBlkSize + sizeof(sht)); break;
	case TYPE_int: task->hdr = (MosaicBlk)( ((char*)task->hdr) + MosaicBlkSize + sizeof(int)); break;
	case TYPE_lng: task->hdr = (MosaicBlk)( ((char*)task->hdr) + MosaicBlkSize + sizeof(lng)); break;
	case TYPE_flt: task->hdr = (MosaicBlk)( ((char*)task->hdr) + MosaicBlkSize + sizeof(flt)); break;
	case TYPE_dbl: task->hdr = (MosaicBlk)( ((char*)task->hdr) + MosaicBlkSize + sizeof(dbl)); break;
	default:
		if( task->type == TYPE_timestamp){
		}
	}
}

#define Estimate(TYPE)\
{	TYPE val = *(TYPE*) task->src;\
	for(i =1; i < task->elm; i++)\
	if ( ((TYPE*)task->src)[i] != val)\
		break;\
	task->xsize[MOSAIC_RLE] = MosaicBlkSize + sizeof(TYPE); \
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
		task->xsize[MOSAIC_RLE] = MosaicBlkSize + sizeof(int);
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
			task->xsize[MOSAIC_RLE] = MosaicBlkSize +sizeof(timestamp);
		}
	}
	task->elements[MOSAIC_RLE]= i;
	return chunksize;
}

// insert a series of values into the compressor block using rle.
#define RLEcompress(TYPE)\
	{	TYPE val = *(TYPE*) task->src;\
		TYPE *dst = (TYPE*) ((char*)task->hdr + MosaicBlkSize);\
		*dst = val;\
		for(i =1; i<task->elm; i++)\
		if ( ((TYPE*)task->src)[i] != val)\
			break;\
		hdr->cnt = i;\
		wordaligned(task->compressed,sizeof(TYPE));\
		task->src += i * sizeof(TYPE);\
		task->elm -= i;\
	}

static void
MOScompress_rle(Client cntxt, MOStask task)
{
	BUN i ;
	MosaicBlk hdr = task->hdr;

	(void) cntxt;
	hdr->tag = MOSAIC_RLE;
	task->time[MOSAIC_RLE] = GDKusec();

	switch(task->type){
	case TYPE_bte: RLEcompress(bte); break ;
	case TYPE_bit: RLEcompress(bit); break;
	case TYPE_sht: RLEcompress(sht); break;
	case TYPE_int:
	{	int val = *(int*) task->src;
		int *dst = (int*) ((char*)task->hdr + MosaicBlkSize );
		*dst = val;
		for(i =1; i<task->elm; i++)
		if ( ((int*)task->src)[i] != val)
			break;
		hdr->cnt = i;
		wordaligned(task->compressed,sizeof(int));
		//task->compressed += sizeof(int);
		task->src += i * sizeof(int);
		task->elm -= i;
	}
		break;
	case TYPE_lng: RLEcompress(lng); break;
	case TYPE_flt: RLEcompress(flt); break;
	case TYPE_dbl: RLEcompress(dbl); break;
	default:
		if( task->type == TYPE_timestamp){
			timestamp val = *(timestamp*) task->src;
			timestamp *dst = (timestamp*) ((char*)hdr + MosaicBlkSize );
			*dst = val;
			for(i =1; i<task->elm; i++)
			if ( !(((timestamp*)task->src)[i].days == val.days && ((timestamp*)task->src)[i].msecs == val.msecs))
				break;
			hdr->cnt = i;
			task->compressed+= sizeof(timestamp);
			task->src += i * sizeof(timestamp);
			task->elm -= i;
		}
	}
#ifdef _DEBUG_MOSAIC_
	MOSdump_rle_(cntxt, task);
#endif
	task->time[MOSAIC_RLE] = GDKusec() - task->time[MOSAIC_RLE];
}

// the inverse operator, extend the src
#define RLEdecompress(TYPE)\
{	TYPE val = *(TYPE*) task->compressed;\
	wordaligned(task->compressed,sizeof(TYPE));\
	for(i = 0; i < (BUN) hdr->cnt; i++)\
		((TYPE*)task->src)[i] = val;\
	task->src += i * sizeof(TYPE);\
}

static void
MOSdecompress_rle( MOStask task)
{
	BUN i;
	MosaicBlk hdr =  ((MosaicBlk) task->hdr);
	task->compressed += MosaicBlkSize;
	assert(0 <= (lng) hdr->cnt && (lng) hdr->cnt <= (lng) task->elm);
	task->elm -= (BUN) hdr->cnt;

	task->time[MOSAIC_RLE] = GDKusec();
	switch(task->type){
	case TYPE_bte: RLEdecompress(bte); break ;
	case TYPE_bit: RLEdecompress(bit); break ;
	case TYPE_sht: RLEdecompress(sht); break;
	case TYPE_int:
	{	int val = *(int*) task->compressed ;
		wordaligned(task->compressed,sizeof(int));
		for(i = 0; i < (BUN) hdr->cnt; i++)
			((int*)task->src)[i] = val;
		task->src += i * sizeof(int);
	}
		break;
	case TYPE_lng: RLEdecompress(lng); break;
	case TYPE_flt: RLEdecompress(flt); break;
	case TYPE_dbl: RLEdecompress(dbl); break;
	default:
		if( task->type == TYPE_timestamp)
		{	timestamp val = *(timestamp*) task->src;
			task->src += sizeof(timestamp);

			for(i = 0; i < (BUN) hdr->cnt; i++)
				((timestamp*)task->src)[i] = val;
			task->src += i * sizeof(timestamp);
			task->elm -= i;
		}
	}
	task->hdr= (MosaicBlk)task->compressed;
	task->time[MOSAIC_RLE] = GDKusec() - task->time[MOSAIC_RLE];
}


// The remainder should provide the minimal algebraic framework
//  to apply the operator to a RLE compressed chunk
//  To be filled in later
//str MOSrle_table(Client cntxt, MOStask task, BAT *bn){
	//return MAL_SUCCEED;
//}
//str MOSrle_subselect(Client cntxt,  MOStask task, oid *cand, void *low, void *hgh, int li, int ri, int anti){
	//return MAL_SUCCEED;
//}
//str MOSrle_thetaselect(Client cntxt,  MOStask task, oid *cand, void *low, void *hgh, int li, int ri, int anti){
	//return MAL_SUCCEED;
//}
//str MOSrle_leftfetchjoin(Client cntxt,  MOStask task, oid *cand){
	//return MAL_SUCCEED;
//}
//str MOSrle_join(Client cntxt,  MOStask task, oid *cand){
	//return MAL_SUCCEED;
//}
