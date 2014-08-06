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

#ifdef _DEBUG_MOSAIC_
static void
MOSdump_none_(Client cntxt, MOStask task)
{
	MosaicBlk hdr = task->hdr;
	mnstr_printf(cntxt->fdout,"#none "LLFMT"\n", (lng)(hdr->cnt));
}
#endif

static void
MOSdump_none(Client cntxt, MOStask task)
{
	MosaicBlk hdr = task->hdr;
	mnstr_printf(cntxt->fdout,"#none "LLFMT"\n", (lng)(hdr->cnt));
	task->elm -= hdr->cnt;
	switch(task->type){
	case TYPE_bte: task->hdr = (MosaicBlk)( ((char*) task->hdr) + MosaicBlkSize + sizeof(bte)* hdr->cnt); break ;
	case TYPE_bit: task->hdr = (MosaicBlk)( ((char*) task->hdr) + MosaicBlkSize + sizeof(bit)* hdr->cnt); break ;
	case TYPE_sht: task->hdr = (MosaicBlk)( ((char*) task->hdr) + MosaicBlkSize + sizeof(sht)* hdr->cnt); break ;
	case TYPE_int: task->hdr = (MosaicBlk)( ((char*) task->hdr) + MosaicBlkSize + sizeof(int)* hdr->cnt); break ;
	case TYPE_lng: task->hdr = (MosaicBlk)( ((char*) task->hdr) + MosaicBlkSize + sizeof(lng)* hdr->cnt); break ;
	case TYPE_flt: task->hdr = (MosaicBlk)( ((char*) task->hdr) + MosaicBlkSize + sizeof(flt)* hdr->cnt); break ;
	case TYPE_dbl: task->hdr = (MosaicBlk)( ((char*) task->hdr) + MosaicBlkSize + sizeof(dbl)); 
	}
}


// append a series of values into the non-compressed block

#define NONEcompress(TYPE)\
{	*(TYPE*) task->compressed = *(TYPE*) task->src;\
	task->src += sizeof(TYPE);\
	task->compressed += sizeof(TYPE);\
	hdr->cnt ++;\
	task->elm--;\
}

// rather expensive simple value non-compressed store
static void
MOScompress_none(Client cntxt, MOStask task)
{
	MosaicBlk hdr = (MosaicBlk) task->hdr;

	(void) cntxt;
    task->elements[MOSAIC_NONE]++;
    task->time[MOSAIC_NONE] = GDKusec();

	switch(task->type){
	case TYPE_bte: NONEcompress(bte); break ;
	case TYPE_bit: NONEcompress(bit); break ;
	case TYPE_sht: NONEcompress(sht); break;
	case TYPE_int:
	{	*(int*) task->compressed = *(int*) task->src;
		task->src += sizeof(int);
		task->compressed += sizeof(int);
		hdr->cnt ++;
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
{ for(i = 0; i < (BUN) hdr->cnt; i++) \
	((TYPE*)task->src)[i] = ((TYPE*)task->compressed)[i]; \
	task->src += i * sizeof(TYPE);\
	task->compressed += i * sizeof(TYPE);\
	task->hdr = (MosaicBlk) task->compressed; \
}

static void
MOSdecompress_none( MOStask task)
{
	BUN i;
	MosaicBlk hdr = (MosaicBlk) task->hdr;

    task->time[MOSAIC_NONE] = GDKusec();
	task->elm -= hdr->cnt;
	task->compressed += MosaicBlkSize;
	switch(task->type){
	case TYPE_bte: NONEdecompress(bte); break ;
	case TYPE_bit: NONEdecompress(bit); break ;
	case TYPE_sht: NONEdecompress(sht); break;
	case TYPE_int:
	{	
		for(i = 0; i < (BUN) hdr->cnt; i++) 
			((int*)task->src)[i] = ((int*)task->compressed)[i];
		task->src += i * sizeof(int);
		task->compressed += i * sizeof(int);
		task->hdr = (MosaicBlk) task->compressed;
	}
		break;
	case TYPE_lng: NONEdecompress(lng); break;
	case TYPE_flt: NONEdecompress(flt); break;
	case TYPE_dbl: NONEdecompress(dbl); break;
	default:
		if( task->type == TYPE_timestamp)
			NONEdecompress(timestamp);
	}
    task->time[MOSAIC_NONE] = GDKusec() - task->time[MOSAIC_NONE];
}


// The remainder should provide the minimal algebraic framework
//  to apply the operator to a NONE compressed chunk
//  To be filled in later
//str MosaicBlk(Client cntxt, MOStask task, BAT *bn){
	//return MAL_SUCCEED;
//}
//str MosaicBlk(Client cntxt,  MOStask task, oid *cand, void *low, void *hgh, int li, int ri, int anti){
	//return MAL_SUCCEED;
//}
//str MosaicBlk(Client cntxt,  MOStask task, oid *cand, void *low, void *hgh, int li, int ri, int anti){
	//return MAL_SUCCEED;
//}
//str MosaicBlk(Client cntxt,  MOStask task, oid *cand){
	//return MAL_SUCCEED;
//}
//str MosaicBlk(Client cntxt,  MOStask task, oid *cand){
	//return MAL_SUCCEED;
//}
