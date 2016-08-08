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
 * Copyright August 2008-2014 MonetDB B.V.
 * All Rights Reserved.
 */

/*
 * (c) 2014 author Martin Kersten
 * Adaptive compression scheme to reduce the storage footprint for stable persistent data.
 * The permissible compression techniques can be controlled thru an argument list
*/

#include "monetdb_config.h"
#include "mosaic.h"
#include "mosaic_hdr.h"
#include "mosaic_literal.h"
#include "mosaic_runlength.h"
#include "mosaic_dictionary.h"
#include "mosaic_delta.h"
#include "mosaic_linear.h"
#include "mosaic_frame.h"
#include "mosaic_prefix.h"

char *MOSfiltername[]={"literal","runlength","dictionary","delta","linear","frame","prefix","EOL"};
BUN MOSblocklimit = 100000;

str MOScompressInternal(Client cntxt, bat *ret, bat *bid, MOStask task, int debug);

static void
MOSinit(MOStask task, BAT *b){
	char *base;
	if( isVIEW(b))
		b= BATdescriptor(VIEWtparent(b));
	assert(b);
	assert( b->tmosaic);
	base = b->tmosaic->base;
	assert(base);
	task->type = b->ttype;
	task->bsrc = b;
	task->hdr = (MosaicHdr) base;
	base += MosaicHdrSize;
	task->blk = (MosaicBlk)  base;
	task->dst = MOScodevector(task);
}

void MOSblk(MosaicBlk blk)
{
	printf("Block tag %d cnt "BUNFMT"\n", MOSgetTag(blk),MOSgetCnt(blk));
}

static void
MOSdumpTask(Client cntxt,MOStask task)
{
	int i;

	mnstr_printf(cntxt->fdout,"# ");
	mnstr_printf(cntxt->fdout,"clk "LLFMT"\tsizes "SZFMT"\t%3.0fx\t", 
		task->timer, task->bsrc->tmosaic->free, (flt) task->bsrc->theap.free/task->bsrc->tmosaic->free);
	for ( i=0; i < MOSAIC_METHODS -1; i++)
	if( task->filter[i])
		mnstr_printf(cntxt->fdout, "%s["LLFMT ","LLFMT "]\t" , MOSfiltername[i], task->hdr->blks[i], task->hdr->elms[i]);
	mnstr_printf(cntxt->fdout,"\n");
}

str
MOSlayout(Client cntxt, BAT *b, BAT *btech, BAT *bcount, BAT *binput, BAT *boutput, BAT *bproperties)
{
	MOStask task=0;
	int i;
	char buf[BUFSIZ];
	lng zero=0;

	task= (MOStask) GDKzalloc(sizeof(*task));
	if( task == NULL)
		throw(SQL,"mosaic",MAL_MALLOC_FAIL);

	if( b->tmosaic == NULL)
			throw(MAL,"mosaic.layout","Compression heap missing");

	MOSinit(task,b);
	MOSinitializeScan(cntxt,task,0,task->hdr->top);
	// safe the general properties

		BUNappend(btech, "ratio", FALSE);
		BUNappend(bcount, &zero, FALSE);
		BUNappend(binput, &zero, FALSE);
		BUNappend(boutput, &zero , FALSE);
		snprintf(buf,BUFSIZ,"%g", task->hdr->ratio);
		BUNappend(bproperties, buf, FALSE);
	for(i=0; i < MOSAIC_METHODS-1; i++){
		lng zero = 0;
		snprintf(buf,BUFSIZ,"%s blocks", MOSfiltername[i]);
		BUNappend(btech, buf, FALSE);
		BUNappend(bcount, &task->hdr->blks[i], FALSE);
		BUNappend(binput, &task->hdr->elms[i], FALSE);
		BUNappend(boutput, &zero , FALSE);
		BUNappend(bproperties, "", FALSE);
	}
	if( task->hdr->blks[MOSAIC_FRAME])
		MOSlayout_frame_hdr(cntxt,task,btech,bcount,binput,boutput,bproperties);
	if( task->hdr->blks[MOSAIC_DICT])
		MOSlayout_dictionary_hdr(cntxt,task,btech,bcount,binput,boutput,bproperties);

	BUNappend(btech, "========", FALSE);
	BUNappend(bcount, &zero, FALSE);
	BUNappend(binput, &zero, FALSE);
	BUNappend(boutput, &zero , FALSE);
	BUNappend(bproperties, "", FALSE);

	while(task->start< task->stop){
		switch(MOSgetTag(task->blk)){
		case MOSAIC_NONE:
			MOSlayout_literal(cntxt,task,btech,bcount,binput,boutput,bproperties);
			MOSadvance_literal(cntxt,task);
			break;
		case MOSAIC_RLE:
			MOSlayout_runlength(cntxt,task,btech,bcount,binput,boutput,bproperties);
			MOSadvance_runlength(cntxt,task);
			break;
		case MOSAIC_DICT:
			MOSlayout_dictionary(cntxt,task,btech,bcount,binput,boutput,bproperties);
			MOSadvance_dictionary(cntxt,task);
			break;
		case MOSAIC_DELTA:
			MOSlayout_delta(cntxt,task,btech,bcount,binput,boutput,bproperties);
			MOSadvance_delta(cntxt,task);
			break;
		case MOSAIC_LINEAR:
			MOSlayout_linear(cntxt,task,btech,bcount,binput,boutput,bproperties);
			MOSadvance_linear(cntxt,task);
			break;
		case MOSAIC_FRAME:
			MOSlayout_frame(cntxt,task,btech,bcount,binput,boutput,bproperties);
			MOSadvance_frame(cntxt,task);
			break;
		case MOSAIC_PREFIX:
			MOSlayout_prefix(cntxt,task,btech,bcount,binput,boutput,bproperties);
			MOSadvance_prefix(cntxt,task);
			break;
		default:
			assert(0);
		}
	}
	return MAL_SUCCEED;
}

// dump a compressed BAT
static void
MOSdumpInternal(Client cntxt, BAT *b){
	MOStask task=0;

	task= (MOStask) GDKzalloc(sizeof(*task));
	if( task == NULL)
		return;
	MOSinit(task,b);
	MOSinitializeScan(cntxt,task,0,task->hdr->top);
	while(task->start< task->stop){
		switch(MOSgetTag(task->blk)){
		case MOSAIC_NONE:
			MOSdump_literal(cntxt,task);
			MOSadvance_literal(cntxt,task);
			break;
		case MOSAIC_RLE:
			MOSdump_runlength(cntxt,task);
			MOSadvance_runlength(cntxt,task);
			break;
		case MOSAIC_DICT:
			MOSdump_dictionary(cntxt,task);
			MOSadvance_dictionary(cntxt,task);
			break;
		case MOSAIC_DELTA:
			MOSdump_delta(cntxt,task);
			MOSadvance_delta(cntxt,task);
			break;
		case MOSAIC_PREFIX:
			MOSdump_prefix(cntxt,task);
			MOSadvance_prefix(cntxt,task);
			break;
		case MOSAIC_FRAME:
			MOSdump_frame(cntxt,task);
			MOSadvance_frame(cntxt,task);
		}
	}
}

str
MOSdump(Client cntxt, MalBlkPtr mb, MalStkPtr stk, InstrPtr pci)
{	
	bat bid = *getArgReference_bat(stk,pci,1);
	BAT *b;
	str msg = MAL_SUCCEED;

	(void) mb;

	if  ((b = BATdescriptor(bid)) == NULL)
		throw(MAL,"mosaic.dump",INTERNAL_BAT_ACCESS);
	if ( !b->tmosaic){
		BBPunfix(bid);
		return MAL_SUCCEED;
	}
	MOSdumpInternal(cntxt,b);
	BBPunfix(bid);
	return msg;
}

/*
static BAT*
inheritCOL( BAT *bn, COLrec *cn, BAT *b, COLrec *c, bat p )
{
	str nme = cn->id;

	assert((b->H == c && p == VIEWhparent(b)) ||
	       (b->T == c && p == VIEWtparent(b)));
	assert(bn->H == cn || bn->T == cn);
	assert(cn->props == NULL);
	assert(cn->vheap == NULL);
	assert(cn->hash  == NULL);
	assert(bn->S->deleted  == b->S->deleted );
	assert(bn->S->first    == b->S->first   );
	assert(bn->S->inserted == b->S->inserted);
	assert(bn->S->count    == b->S->count   );

	HEAPfree(&cn->heap);

	if (p == 0)
		p = b->batCacheid;
	bn->S->capacity = MIN( bn->S->capacity, b->S->capacity );
	*cn = *c;
	BBPshare(p);
	if (cn->vheap) {
		assert(cn->vheap->parentid > 0);
		BBPshare(cn->vheap->parentid);
	}
	cn->heap.copied = 0;
	cn->props = NULL;
	cn->heap.parentid = p;
	cn->id = nme;
	if (isVIEW(b))
		cn->hash = NULL;
	else
		cn->hash = c->hash;

	return bn;
}
*/

/*
 * Compression is focussed on a single column.
 * Multiple compression techniques are applied at the same time.
 */

#define MOSnewBlk(TASK)\
			MOSsetTag(TASK->blk,MOSAIC_EOL);\
			MOSsetCnt(TASK->blk,0);\
			TASK->dst = MOScodevector(TASK);

/* The compression orchestration is dealt with here.
 * We assume that the estimates for each scheme returns
 * the number of elements it applies to. Moreover, we
 * assume that the compression factor holds for any subsequence.
 * This allows us to avoid expensive estimate calls when a small
 * sequence is found with high compression factor.
 */
static int
MOSoptimizerCost(Client cntxt, MOStask task, int typewidth)
{
	int cand = MOSAIC_NONE;
	float ratio = 1.0, fac = 1.0;

	// select candidate amongst those
	if ( task->filter[MOSAIC_RLE]){
		fac = MOSestimate_runlength(cntxt,task);
		if (fac > ratio){
			cand = MOSAIC_RLE;
			ratio = fac;
		}
	}
	if ( task->filter[MOSAIC_LINEAR]){
		fac = MOSestimate_linear(cntxt,task);
		if ( fac >ratio){
			cand = MOSAIC_LINEAR;
			ratio = fac;
		}
	}
	// max achievable compression factor is 64x
	if (ratio < typewidth && task->filter[MOSAIC_PREFIX]){
		fac = MOSestimate_prefix(cntxt,task);
		if ( fac > ratio ){
			cand = MOSAIC_PREFIX;
			ratio = fac;
		}
		if ( fac  < 0.0)
				task->filter[MOSAIC_PREFIX] = 0;
	}
	// max achievable compression factor is 8x
	if (ratio < 8 && task->filter[MOSAIC_DICT]){
		fac = MOSestimate_dictionary(cntxt,task);
		if (fac > ratio){
			cand = MOSAIC_DICT;
			ratio = fac;
		}
	}
	// max achievable compression factor is 8x
	if (ratio < 8 && task->filter[MOSAIC_FRAME]){
		fac = MOSestimate_frame(cntxt,task);
		if (fac > ratio){
			cand = MOSAIC_FRAME;
			ratio = fac;
		}
	}
	// max achievable compression factor is 8x
	if (ratio < 8 && task->filter[MOSAIC_DELTA]){
		fac = MOSestimate_delta(cntxt,task);
		if ( fac > ratio ){
			cand = MOSAIC_DELTA;
			ratio = fac;
		}
	}
	//mnstr_printf(cntxt->fdout,"#cand %d factor %f\n",cand,ratio);
	return cand;
}

str
MOScompressInternal(Client cntxt, bat *ret, bat *bid, MOStask task, int debug)
{
	BAT *o = NULL, *bsrc;		// the BAT to be augmented with a compressed heap
	str msg = MAL_SUCCEED;
	int cand;
	int tpe, typewidth;
	lng t0,t1;
	
	*ret = 0;

	if ((bsrc = BATdescriptor(*bid)) == NULL)
		throw(MAL, "mosaic.compress", INTERNAL_BAT_ACCESS);

	switch( tpe =ATOMbasetype(bsrc->ttype)){
	case TYPE_bit:
	case TYPE_bte:
	case TYPE_sht:
	case TYPE_int:
	case TYPE_lng:
#ifdef HAVE_HGE
	case TYPE_hge:
#endif
	case TYPE_oid:
	case TYPE_flt:
	case TYPE_dbl:
	case TYPE_str:
		typewidth = ATOMsize(tpe) * 8; // size in bits
		break;
	default:
		// don't compress them
		BBPkeepref(*ret = bsrc->batCacheid);
		return msg;
	}

    if (BATcheckmosaic(bsrc)){
		/* already compressed */
		BBPkeepref(*ret = bsrc->batCacheid);
		return msg;
	}
    assert(bsrc->tmosaic == NULL);

    if (VIEWtparent(bsrc)) {
        bat p = VIEWtparent(bsrc);
        o = bsrc;
        bsrc = BATdescriptor(p);
        if (BATcheckmosaic(bsrc)) {
            BBPunfix(bsrc->batCacheid);
            return MAL_SUCCEED;
        }
        assert(bsrc->timprints == NULL);
    }

	if ( BATcount(bsrc) < MOSAIC_THRESHOLD  ){
		/* no need to compress */
		BBPkeepref(*ret = bsrc->batCacheid);
		return msg;
	}

#ifdef _DEBUG_MOSAIC_
	mnstr_printf(cntxt->fdout,"#compress bat %d \n",*bid);
#endif
    t0 = GDKusec();

	if( bsrc->tmosaic == NULL && BATmosaic(bsrc,  BATcapacity(bsrc) + (MosaicHdrSize + MosaicBlkSize)/Tsize(bsrc)+ BATTINY) == GDK_FAIL){
		// create the mosaic heap if not available.
		// The final size should be smaller then the original
		// It may, however, be the case that we mix a lot of LITERAL and, say, DELTA small blocks
		// Then we total size may go beyond the original size and we should terminate the process.
		// This should be detected before we compress a block, in the estimate functions
		// or when we extend the non-compressed collector block
			throw(MAL,"mosaic.compress", "heap construction failes");
	}
	
	// initialize the non-compressed read pointer
	task->src = Tloc(bsrc, 0);
	task->elm = BATcount(bsrc);
	task->start = 0;
	task->stop = BATcount(bsrc);
	task->timer = GDKusec();

	MOSinit(task,bsrc);
	task->blk->cnt= 0;
	MOSinitHeader(task);

	// claim the server for exclusive use
	msg = MCstartMaintenance(cntxt,1,0);
	if( msg != MAL_SUCCEED){
		GDKfree(task);
		BBPunfix(bsrc->batCacheid);
		throw(MAL, "mosaic.compress", "Can not claim server");
	}
	MOScreateframeDictionary(cntxt,task);
	MOScreatedictionary(cntxt,task);
	// always start with an EOL block
	MOSsetTag(task->blk,MOSAIC_EOL);

	while(task->start < task->stop ){
		// default is to extend the non-compressed block with a single element
		cand = MOSoptimizerCost(cntxt, task, typewidth);
		if( task->dst >= bsrc->tmosaic->base + bsrc->tmosaic->size - 16 ){
			MOSdestroy(bsrc);
			msg= createException(MAL,"mosaic","abort compression due to size");
			task->hdr = 0;
			goto finalize;
		}
		assert (task->dst < bsrc->tmosaic->base + bsrc->tmosaic->size );

		// wrapup previous block
		switch(cand){
		case MOSAIC_RLE:
		case MOSAIC_DICT:
		case MOSAIC_FRAME:
		case MOSAIC_DELTA:
		case MOSAIC_LINEAR:
		case MOSAIC_PREFIX:
			// close the non-compressed part
			if( MOSgetTag(task->blk) == MOSAIC_NONE && MOSgetCnt(task->blk) ){
				task->start -= MOSgetCnt(task->blk);
				MOSupdateHeader(cntxt,task);
				MOSadvance_literal(cntxt,task);
				// always start with an EOL block
				task->dst = MOScodevector(task);
				MOSsetTag(task->blk,MOSAIC_EOL);
				MOSsetCnt(task->blk,0);
			}
			break;
		case MOSAIC_NONE:
			if ( MOSgetCnt(task->blk) == MOSlimit()){
				task->start -= MOSgetCnt(task->blk);
				MOSupdateHeader(cntxt,task);
				MOSadvance_literal(cntxt,task);
				// always start with an EOL block
				task->dst = MOScodevector(task);
				MOSsetTag(task->blk,MOSAIC_EOL);
				MOSsetCnt(task->blk,0);
			}
		}
		// apply the compression to a chunk
		switch(cand){
		case MOSAIC_RLE:
			MOScompress_runlength(cntxt,task);
			MOSupdateHeader(cntxt,task);
			MOSadvance_runlength(cntxt,task);
			MOSnewBlk(task);
			break;
		case MOSAIC_DICT:
			MOScompress_dictionary(cntxt,task);
			MOSupdateHeader(cntxt,task);
			MOSadvance_dictionary(cntxt,task);
			MOSnewBlk(task);
			break;
		case MOSAIC_DELTA:
			MOScompress_delta(cntxt,task);
			MOSupdateHeader(cntxt,task);
			MOSadvance_delta(cntxt,task);
			MOSnewBlk(task);
			break;
		case MOSAIC_LINEAR:
			MOScompress_linear(cntxt,task);
			MOSupdateHeader(cntxt,task);
			MOSadvance_linear(cntxt,task);
			MOSnewBlk(task);
			break;
		case MOSAIC_FRAME:
			MOScompress_frame(cntxt,task);
			MOSupdateHeader(cntxt,task);
			MOSadvance_frame(cntxt,task);
			MOSnewBlk(task);
			break;
		case MOSAIC_PREFIX:
			MOScompress_prefix(cntxt,task);
			MOSupdateHeader(cntxt,task);
			MOSadvance_prefix(cntxt,task);
			MOSnewBlk(task);
			break;
		default :
			// continue to use the last block header.
			MOScompress_literal(cntxt,task);
			task->start++;
		}
	}
	if( MOSgetTag(task->blk) == MOSAIC_NONE && MOSgetCnt(task->blk)){
		MOSupdateHeader(cntxt,task);
		MOSadvance_literal(cntxt,task);
		task->dst = MOScodevector(task);
		MOSsetTag(task->blk,MOSAIC_EOL);
	} else
		task->dst = MOScodevector(task);
	task->bsrc->tmosaic->free = (task->dst - (char*)task->hdr);
	task->timer = GDKusec() - task->timer;
	if(debug) 
		MOSdumpTask(cntxt,task);
	// if we couldnt compress well enough, ignore the result
	// TODO

	bsrc->batDirty = 1;
	task->ratio = task->hdr->ratio = (flt)task->bsrc->theap.free/ task->bsrc->tmosaic->free;
finalize:
	MCexitMaintenance(cntxt);
	*ret= bsrc->batCacheid;
	BBPkeepref(bsrc->batCacheid);

#ifdef _DEBUG_MOSAIC_
	MOSdumpInternal(cntxt,bsrc);
#endif
    t1 = GDKusec();
    ALGODEBUG fprintf(stderr, "#BATmosaic: mosaic construction " LLFMT " usec\n", t1 - t0);
    if (o != NULL) {
        o->tmosaic = NULL;  /* views always keep null pointer and
                       need to obtain the latest mosaic
                       from the parent at query time */
    }
	return msg;
}

str
MOScompress(Client cntxt, MalBlkPtr mb, MalStkPtr stk, InstrPtr pci)
{	
	str prop = NULL;
	int i;
	MOStask task;

#ifdef _DEBUG_MOSAIC_
	int flg = 1;
#else
	int flg = 0;
#endif
	(void) mb;
	task= (MOStask) GDKzalloc(sizeof(*task));
	if( task == NULL)
		throw(MAL, "mosaic.compress", MAL_MALLOC_FAIL);

	if( pci->argc == 3)
		prop = *getArgReference_str(stk,pci,2);
	if( prop && !strstr(prop,"mosaic"))
		for( i = 0; i< MOSAIC_METHODS; i++)
			task->filter[i]= strstr(prop,MOSfiltername[i]) != 0;
	else
		for( i = 0; i< MOSAIC_METHODS; i++)
			task->filter[i]= 1;

	prop= MOScompressInternal(cntxt, getArgReference_bat(stk,pci,0), getArgReference_bat(stk,pci,1), task,  flg);
	GDKfree(task);
	return prop;
}

// recreate the uncompressed heap from its mosaic version
str
MOSdecompressInternal(Client cntxt, bat *ret, bat *bid)
{	
	BAT *bsrc;
	MOStask task;
	str msg;
	int error;
	(void) cntxt;

#ifdef _DEBUG_MOSAIC_
	mnstr_printf(cntxt->fdout,"#decompress bat %d\n",*bid);
#endif
	if ((bsrc = BATdescriptor(*bid)) == NULL)
		throw(MAL, "mosaic.decompress", INTERNAL_BAT_ACCESS);

	if (BATcheckmosaic(bsrc) == 0 ){
		BBPunfix(bsrc->batCacheid);
		BBPkeepref(*ret = bsrc->batCacheid);
		return MAL_SUCCEED;
	}
	if (!bsrc->tmosaic) {
		BBPkeepref(*ret = bsrc->batCacheid);
		return MAL_SUCCEED;
	}

	if (bsrc->tmosaic && VIEWtparent(bsrc)) {
		BBPunfix(bsrc->batCacheid);
		throw(MAL, "mosaic.decompress", "cannot decompress tail-VIEW");
	}

	// use the original heap for reconstruction
	task= (MOStask) GDKzalloc(sizeof(*task));
	if( task == NULL){
		BBPunfix(bsrc->batCacheid);
		throw(MAL, "mosaic.decompress", MAL_MALLOC_FAIL);
	}
	
	// claim the server for exclusive use
	msg = MCstartMaintenance(cntxt,1,0);
	if( msg != MAL_SUCCEED){
		GDKfree(msg);
		BBPunfix(bsrc->batCacheid);
		throw(MAL, "mosaic.decompress", "Can not claim server");
	}

	MOSinit(task,bsrc);
	task->src = Tloc(bsrc, 0);
	task->timer = GDKusec();

	while(task->blk){
		switch(MOSgetTag(task->blk)){
		case MOSAIC_NONE:
			MOSdecompress_literal(cntxt,task);
			MOSskip_literal(cntxt,task);
			break;
		case MOSAIC_RLE:
			MOSdecompress_runlength(cntxt,task);
			MOSskip_runlength(cntxt,task);
			break;
		case MOSAIC_DICT:
			MOSdecompress_dictionary(cntxt,task);
			MOSskip_dictionary(cntxt,task);
			break;
		case MOSAIC_DELTA:
			MOSdecompress_delta(cntxt,task);
			MOSskip_delta(cntxt,task);
			break;
		case MOSAIC_LINEAR:
			MOSdecompress_linear(cntxt,task);
			MOSskip_linear(cntxt,task);
			break;
		case MOSAIC_FRAME:
			MOSdecompress_frame(cntxt,task);
			MOSskip_frame(cntxt,task);
			break;
		case MOSAIC_PREFIX:
			MOSdecompress_prefix(cntxt,task);
			MOSskip_prefix(cntxt,task);
			break;
		default: assert(0);
		}
	}

	
	error = 0;
	switch( ATOMbasetype(task->type)){
	case TYPE_bte:
		error = task->hdr->checksum.sumbte != task->hdr->checksum2.sumbte;
		break;
	case TYPE_sht:
		error = task->hdr->checksum.sumsht != task->hdr->checksum2.sumsht;
		break;
	case TYPE_int:
		error = task->hdr->checksum.sumint != task->hdr->checksum2.sumint;
		break;
	case TYPE_lng:
		error = task->hdr->checksum.sumlng != task->hdr->checksum2.sumlng;
		break;
	case TYPE_flt:
		error = task->hdr->checksum.sumflt != task->hdr->checksum2.sumflt;
		break;
	case TYPE_dbl:
		error = task->hdr->checksum.sumdbl != task->hdr->checksum2.sumdbl;
		break;
	case TYPE_str:
		break;
	default:
		mnstr_printf(cntxt->fdout,"#unknown compression compatibility\n");
	}
	if(error)
		mnstr_printf(cntxt->fdout,"#incompatible compression\n");

	task->timer = GDKusec() - task->timer;

	// remove the compressed mirror
	GDKfree(task);
	// continue with all work
	bsrc->batDirty = 1;
	MOSdestroy(bsrc);
	BATsettrivprop(bsrc);
	BBPkeepref( *ret = bsrc->batCacheid);

	MCexitMaintenance(cntxt);
	return MAL_SUCCEED;
}

str
MOSdecompress(Client cntxt, MalBlkPtr mb, MalStkPtr stk, InstrPtr pci)
{	
	(void) mb;
	return MOSdecompressInternal(cntxt, getArgReference_bat(stk,pci,0), getArgReference_bat(stk,pci,1));
}

str
MOSdecompressStorage(Client cntxt, MalBlkPtr mb, MalStkPtr stk, InstrPtr pci)
{	
	(void) mb;
	return MOSdecompressInternal(cntxt, getArgReference_bat(stk,pci,0), getArgReference_bat(stk,pci,1));
}

// The remainders is cloned from the generator code base
// overload the algebra functions to check for compressed heaps.
static int
isCompressed(bat bid)
{
	BAT *b;
	int r=0;
	if( bid == 0)
		return 0;
	b = BATdescriptor(bid);
	r = BATcheckmosaic(b);
	BBPunfix(bid);
	return r;
}

// locate the corresponding bind operation to determine the partition
static void
MOSgetPartition(Client cntxt, MalBlkPtr mb, MalStkPtr stk, int varid, int *part, int *nrofparts )
{
	int i;
	InstrPtr p;

	*part = 0;
	*nrofparts = 1;
	for( i = 1; i< mb->stop; i++){
		p= getInstrPtr(mb,i);
		if( getModuleId(p)== sqlRef && getFunctionId(p) == bindRef && getArg(p,0) == varid ){
			if( p->argc > 6){
				*part = getVarConstant(mb,getArg(p,6)).val.ival;
				*nrofparts = getVarConstant(mb,getArg(p,7)).val.ival;
			}
		} else
		if( p->token == ASSIGNsymbol && getArg(p,1) == varid)
			MOSgetPartition(cntxt,mb,stk,getArg(p,0),part,nrofparts);
	}
}

/* The algebra operators should fall back to their default
 * when we know that the heap is not compressed
 * The actual decompression should wait until we know that
 * the administration thru SQL layers works properly.
 *
 * The oid-range can be reduced due to partitioning.
 */
str
MOSsubselect(Client cntxt, MalBlkPtr mb, MalStkPtr stk, InstrPtr pci)
{
	bit *li, *hi, *anti;
	void *low, *hgh;
	bat *ret, *bid, *cid= 0;
	int i;
	int startblk,stopblk; // block range to scan
	BUN cnt = 0;
	BAT *b, *bn, *cand = NULL;
	str msg = MAL_SUCCEED;
	MOStask task;
	int part,nrofparts;

	(void) cntxt;
	(void) mb;
	ret = getArgReference_bat(stk, pci, 0);
	bid = getArgReference_bat(stk, pci, 1);

	if (pci->argc == 8) {	/* candidate list included */
		cid = getArgReference_bat(stk, pci, 2);
		i = 3;
	} else
		i = 2;
	low = (void *) getArgReference(stk, pci, i);
	hgh = (void *) getArgReference(stk, pci, i + 1);
	li = getArgReference_bit(stk, pci, i + 2);
	hi = getArgReference_bit(stk, pci, i + 3);
	anti = getArgReference_bit(stk, pci, i + 4);
	//
	// use default implementation if possible
	if( !isCompressed(*bid)){
		if(cid)
			return ALGsubselect2(ret,bid,cid,low,hgh,li,hi,anti);
		else
			return ALGsubselect1(ret,bid,low,hgh,li,hi,anti);
	}

	b= BATdescriptor(*bid);
	if( b == NULL)
			throw(MAL, "mosaic.subselect",RUNTIME_OBJECT_MISSING);

	task= (MOStask) GDKzalloc(sizeof(*task));
	if( task == NULL){
		BBPunfix(b->batCacheid);
		throw(MAL, "mosaic.subselect", RUNTIME_OBJECT_MISSING);
	}

	// accumulator for the oids
	bn = COLnew((oid)0, TYPE_oid, BATcount(b), TRANSIENT);
	if( bn == NULL){
		GDKfree(task);
		BBPunfix(b->batCacheid);
		throw(MAL, "mosaic.subselect", RUNTIME_OBJECT_MISSING);
	}
	task->lb = (oid*) Tloc(bn,0);

	MOSinit(task,b);
	// drag along the candidate list into the task descriptor
	if (cid) {
		cand = BATdescriptor(*cid);
		if (cand == NULL){
			BBPunfix(b->batCacheid);
			BBPunfix(bn->batCacheid);
			GDKfree(task);
			throw(MAL, "mosaic.subselect", RUNTIME_OBJECT_MISSING);
		}
		task->cl = (oid*) Tloc(cand, 0);
		task->n = BATcount(cand);
	}

	// determine block range to scan for partitioned columns
	MOSgetPartition(cntxt, mb, stk, getArg(pci,1), &part, &nrofparts );
	if ( nrofparts > 1){
		// don't use more parallelism than entries in the header
		if( nrofparts > task->hdr->top)
			nrofparts = task->hdr->top;
		if( part > nrofparts){
			*getArgReference_bat(stk, pci, 0) = bn->batCacheid;
			BBPkeepref(bn->batCacheid);
			GDKfree(task);
			return MAL_SUCCEED;
		}
		startblk = task->hdr->top/nrofparts * part;
		if( part == nrofparts -1)
			stopblk  =  task->hdr->top;
		else
			stopblk  =  startblk + task->hdr->top/nrofparts;
	} else{
		startblk =0;
		stopblk = task->hdr->top;
	}
	// position the scan on the first mosaic block to consider
	MOSinitializeScan(cntxt,task,startblk,stopblk);

	while(task->start < task->stop ){
		switch(MOSgetTag(task->blk)){
		case MOSAIC_RLE:
			MOSsubselect_runlength(cntxt,task,low,hgh,li,hi,anti);
			break;
		case MOSAIC_DICT:
			MOSsubselect_dictionary(cntxt,task,low,hgh,li,hi,anti);
			break;
		case MOSAIC_FRAME:
			MOSsubselect_frame(cntxt,task,low,hgh,li,hi,anti);
			break;
		case MOSAIC_DELTA:
			MOSsubselect_delta(cntxt,task,low,hgh,li,hi,anti);
			break;
		case MOSAIC_PREFIX:
			MOSsubselect_prefix(cntxt,task,low,hgh,li,hi,anti);
			break;
		case MOSAIC_LINEAR:
			MOSsubselect_linear(cntxt,task,low,hgh,li,hi,anti);
			break;
		case MOSAIC_NONE:
		default:
			MOSsubselect_literal(cntxt,task,low,hgh,li,hi,anti);
		}
	}
	// derive the filling
	cnt = (BUN) (task->lb - (oid*) Tloc(bn,0));
	assert(bn->batCapacity >= cnt);
	BATsetcount(bn,cnt);
	bn->tnil = 0;
	bn->tnonil = 1;
	bn->tsorted = bn->trevsorted = cnt <=1;
	*getArgReference_bat(stk, pci, 0) = bn->batCacheid;
	GDKfree(task);
	BBPkeepref(bn->batCacheid);
	return msg;
}


str MOSthetasubselect(Client cntxt, MalBlkPtr mb, MalStkPtr stk, InstrPtr pci)
{
	int idx;
	bat *cid =0,  *ret, *bid;
	int startblk, stopblk; // block range to scan
	int part=0,nrofparts=0;
	BAT *b = 0, *cand = 0, *bn = NULL;
	BUN cnt=0;
	str msg= MAL_SUCCEED;
	char **oper;
	void *low;
	MOStask task;

	(void) mb;
	(void) cntxt;
	ret= getArgReference_bat(stk,pci,0);
	bid= getArgReference_bat(stk,pci,1);
	if( pci->argc == 5){ // candidate list included
		cid = getArgReference_bat(stk,pci, 2);
		idx = 3;
	} else idx = 2;
	low= (void*) getArgReference(stk,pci,idx);
	oper= getArgReference_str(stk,pci,idx+1);

	if( !isCompressed(*bid)){
		if( cid)
			return ALGthetasubselect2(ret,bid,cid,low, (const char **)oper);
		else
			return ALGthetasubselect1(ret,bid,low, (const char **)oper);
	}
	
	b = BATdescriptor(*bid);
	if( b == NULL)
		throw(MAL, "mosaic.thetasubselect", RUNTIME_OBJECT_MISSING);
	// determine the elements in the compressed structure

	task= (MOStask) GDKzalloc(sizeof(*task));
	if( task == NULL){
		BBPunfix(b->batCacheid);
		throw(MAL, "mosaic.thetasubselect", RUNTIME_OBJECT_MISSING);
	}

	// accumulator for the oids
	bn = COLnew((oid)0, TYPE_oid, BATcount(b), TRANSIENT);
	if( bn == NULL){
		BBPunfix(b->batCacheid);
		throw(MAL, "mosaic.thetasubselect", RUNTIME_OBJECT_MISSING);
	}
	task->lb = (oid*) Tloc(bn,0);

	MOSinit(task,b);
	// drag along the candidate list into the task descriptor
	if (cid) {
		cand = BATdescriptor(*cid);
		if (cand == NULL){
			BBPunfix(b->batCacheid);
			BBPunfix(bn->batCacheid);
			GDKfree(task);
			throw(MAL, "mosaic.thetasubselect", RUNTIME_OBJECT_MISSING);
		}
		task->cl = (oid*) Tloc(cand, 0);
		task->n = BATcount(cand);
	}

	// determine block range to scan for partitioned columns
	MOSgetPartition(cntxt, mb, stk, getArg(pci,1), &part, &nrofparts );
	if ( nrofparts > 1){
		// don't use more parallelism than entries in the header
		if( nrofparts > task->hdr->top)
			nrofparts = task->hdr->top;
		if( part > nrofparts){
			BBPkeepref(*getArgReference_bat(stk,pci,0)= bn->batCacheid);
			GDKfree(task);
			return MAL_SUCCEED;
		}
		startblk = task->hdr->top/nrofparts * part;
		if( part == nrofparts -1)
			stopblk  =  task->hdr->top;
		else
			stopblk  =  startblk + task->hdr->top/nrofparts;
	} else{
		startblk =0;
		stopblk = task->hdr->top;
	}
	// position the scan on the first mosaic block to consider
	MOSinitializeScan(cntxt,task,startblk,stopblk);

	while(task->start < task->stop ){
		switch(MOSgetTag(task->blk)){
		case MOSAIC_RLE:
			MOSthetasubselect_runlength(cntxt,task,low,*oper);
			break;
		case MOSAIC_DELTA:
			MOSthetasubselect_delta(cntxt,task,low,*oper);
			break;
		case MOSAIC_PREFIX:
			MOSthetasubselect_prefix(cntxt,task,low,*oper);
			break;
		case MOSAIC_LINEAR:
			MOSthetasubselect_linear(cntxt,task,low,*oper);
			break;
		case MOSAIC_DICT:
			MOSthetasubselect_dictionary(cntxt,task,low,*oper);
			break;
		case MOSAIC_FRAME:
			MOSthetasubselect_frame(cntxt,task,low,*oper);
			break;
		case MOSAIC_NONE:
		default:
			MOSthetasubselect_literal(cntxt,task,low,*oper);
		}
	}
	// derive the filling
	cnt = (BUN)( task->lb - (oid*) Tloc(bn,0));
	
	if( cid)
		BBPunfix(*cid);
	if( bn){
		BATsetcount(bn,cnt);
		bn->tnil = 0;
		bn->tnonil = 1;
		bn->tsorted = bn->trevsorted = cnt <= 1;
		BBPkeepref(*getArgReference_bat(stk,pci,0)= bn->batCacheid);
	}
	GDKfree(task);
	return msg;
}

str MOSprojection(Client cntxt, MalBlkPtr mb, MalStkPtr stk, InstrPtr pci)
{
	bat *ret, *lid =0, *rid=0;
	int part, nrofparts;
	int startblk,stopblk;
	BAT *bl = NULL, *br = NULL, *bn;
	BUN cnt;
	oid *ol =0, o = 0;
	str msg= MAL_SUCCEED;
	MOStask task;

	(void) mb;
	(void) cntxt;

	ret = getArgReference_bat(stk,pci,0);
	lid = getArgReference_bat(stk,pci,1);
	rid = getArgReference_bat(stk,pci,2);

	if( !isCompressed(*rid))
		return ALGprojection(ret,lid,rid);

	br = BATdescriptor(*rid);
	if( br == NULL){
		throw(MAL,"mosaic.projection",RUNTIME_OBJECT_MISSING);
	}

	bl = BATdescriptor(*lid);
	if( bl == NULL){
		BBPunfix(*rid);
		throw(MAL,"mosaic.projection",RUNTIME_OBJECT_MISSING);
	}

	cnt = BATcount(bl);
	bn = COLnew((oid)0,br->ttype, cnt, TRANSIENT);
	if ( bn == NULL){
		BBPunfix(*lid);
		BBPunfix(*rid);
		throw(MAL,"mosaic.projection",MAL_MALLOC_FAIL);
	}

	if ( bl->ttype == TYPE_void)
		o = bl->tseqbase;
	else
		ol = (oid*) Tloc(bl,0);

	(void) o;

	task= (MOStask) GDKzalloc(sizeof(*task));
	if( task == NULL){
		BBPunfix(*lid);
		BBPunfix(*rid);
		GDKfree(task);
		throw(MAL, "mosaic.projection", RUNTIME_OBJECT_MISSING);
	}
	MOSinit(task,br);
	task->src = (char*) Tloc(bn,0);

	task->cl = ol;
	task->n = cnt;

	// determine block range to scan for partitioned columns
	MOSgetPartition(cntxt, mb, stk, getArg(pci,1), &part, &nrofparts );
	if ( nrofparts > 1){
		// don't use more parallelism than entries in the header
		if( nrofparts > task->hdr->top)
			nrofparts = task->hdr->top;
		if( part >= nrofparts){
			BBPunfix(*lid);
			BBPunfix(*rid);
			BBPkeepref(*ret = bn->batCacheid);
			GDKfree(task);
			return MAL_SUCCEED;
		}
		startblk = task->hdr->top/nrofparts * part;
		if( part < nrofparts )
			stopblk  =  startblk + task->hdr->top/nrofparts;
		else
			stopblk  =  task->hdr->top;
	} else{
		startblk =0;
		stopblk = task->hdr->top;
	}
	assert(startblk < stopblk);
	// position the scan on the first mosaic block to consider
	MOSinitializeScan(cntxt,task,startblk,stopblk);

	// loop thru all the chunks and fetch all results
	while(task->start<task->stop )
		switch(MOSgetTag(task->blk)){
		case MOSAIC_RLE:
			MOSprojection_runlength(cntxt, task);
			break;
		case MOSAIC_DICT:
			MOSprojection_dictionary(cntxt, task);
			break;
		case MOSAIC_FRAME:
			MOSprojection_frame(cntxt, task);
			break;
		case MOSAIC_DELTA:
			MOSprojection_delta(cntxt, task);
			break;
		case MOSAIC_PREFIX:
			MOSprojection_prefix(cntxt, task);
			break;
		case MOSAIC_LINEAR:
			MOSprojection_linear(cntxt, task);
			break;
		case MOSAIC_NONE:
			MOSprojection_literal(cntxt, task);
			break;
		default:
			assert(0);
		}

	/* adminstrative wrapup of the projection */
	BBPunfix(*lid);
	BBPunfix(*rid);

	BATsetcount(bn,task->cnt);
	bn->tnil = 0;
	bn->tnonil = 1;
	bn->tsorted = bn->trevsorted = cnt <= 1;
	BBPkeepref(*ret = bn->batCacheid);
	GDKfree(task);
	return msg;
}


str
MOSsubjoin(Client cntxt, MalBlkPtr mb, MalStkPtr stk, InstrPtr pci)
{
	bat *ret, *ret2,*lid,*rid, *sl, *sr;
	int part, nrofparts;
	int startblk,stopblk;
	bit *nil_matches= 0;
	lng *estimate;
	BAT  *bl = NULL, *br = NULL, *bln = NULL, *brn= NULL;
	BUN cnt = 0;
	int swapped = 0;
	str msg = MAL_SUCCEED;
	MOStask task;

	(void) cntxt;
	(void) mb;
	ret = getArgReference_bat(stk,pci,0);
	ret2 = getArgReference_bat(stk,pci,1);
	lid = getArgReference_bat(stk,pci,2);
	rid = getArgReference_bat(stk,pci,3);
	nil_matches = getArgReference_bit(stk,pci,6);
	estimate = getArgReference_lng(stk,pci,7);

	sl = (bat*) getArgReference(stk,pci,4) ;
	sr = (bat*) getArgReference(stk,pci,5);
	if( (!isCompressed(*lid) && !isCompressed(*rid)) || (*sl != bat_nil || *sr != bat_nil)) 
		return ALGsubjoin(ret,ret2,lid,rid,sl,sr,nil_matches,estimate);

	bl = BATdescriptor(*lid);
	if( bl == NULL)
		throw(MAL,"mosaic.join",RUNTIME_OBJECT_MISSING);
	br = BATdescriptor(*rid);
	if( br == NULL){
		BBPunfix(bl->batCacheid);
		throw(MAL,"mosaic.join",RUNTIME_OBJECT_MISSING);
	}

	// we assume one compressed argument
	if (bl->tmosaic && br->tmosaic){
		BBPunfix(bl->batCacheid);
		BBPunfix(br->batCacheid);
		throw(MAL,"mosaic.join","Join over generator pairs not supported");
    }

	// result set preparation
	bln = COLnew((oid)0,TYPE_oid, cnt, TRANSIENT);
	brn = COLnew((oid)0,TYPE_oid, cnt, TRANSIENT);
	task= (MOStask) GDKzalloc(sizeof(*task));
	if( bln == NULL || brn == NULL || task == NULL){
		if( bln) BBPunfix(bln->batCacheid);
		if( brn) BBPunfix(brn->batCacheid);
		BBPunfix(bl->batCacheid);
		BBPunfix(br->batCacheid);
		throw(MAL,"mosaic.join",MAL_MALLOC_FAIL);
	}

	if ( bl->tmosaic){
		MOSinit(task,bl);
		//task->elm = BATcount(br);
		//task->src= Tloc(br,0);
	} else {
		MOSinit(task,br);
		//task->elm = BATcount(bl);
		//task->src= Tloc(bl,0);
		swapped=1;
	}
	task->lbat = bln;
	task->rbat = brn;
	// determine block range to scan for partitioned columns
	MOSgetPartition(cntxt, mb, stk, getArg(pci,1), &part, &nrofparts );
	if ( nrofparts > 1){
		startblk = task->hdr->top/nrofparts * part;
		if( part == nrofparts -1)
			stopblk  =  task->hdr->top;
		else
			stopblk  =  startblk + task->hdr->top/nrofparts;
	} else{
		startblk =0;
		stopblk = task->hdr->top;
	}
	// position the scan on the first mosaic block to consider
	MOSinitializeScan(cntxt,task,startblk,stopblk);

	if ( bl->tmosaic){
		task->elm = BATcount(br);
		task->src= Tloc(br,0);
	} else {
		task->elm = BATcount(bl);
		task->src= Tloc(bl,0);
	}
	// loop thru all the chunks and collect the results
	while(task->blk )
		switch(MOSgetTag(task->blk)){
		case MOSAIC_RLE:
			MOSsubjoin_runlength(cntxt, task);
			break;
		case MOSAIC_DICT:
			MOSsubjoin_dictionary(cntxt, task);
			break;
		case MOSAIC_FRAME:
			MOSsubjoin_frame(cntxt, task);
			break;
		case MOSAIC_DELTA:
			MOSsubjoin_delta(cntxt, task);
			break;
		case MOSAIC_PREFIX:
			MOSsubjoin_prefix(cntxt, task);
			break;
		case MOSAIC_LINEAR:
			MOSsubjoin_linear(cntxt, task);
			break;
		case MOSAIC_NONE:
			MOSsubjoin_literal(cntxt, task);
			break;
		default:
			assert(0);
		}

	BATsettrivprop(bln);
	BATsettrivprop(brn);
    if( swapped){
        BBPkeepref(*ret= brn->batCacheid);
        BBPkeepref(*ret2= bln->batCacheid);
    } else {
        BBPkeepref(*ret= bln->batCacheid);
        BBPkeepref(*ret2= brn->batCacheid);
    }
	GDKfree(task);
    return msg;
}

// The analyse routine runs through the BAT dictionary and assess
// all possible compression options.

/*
 * Start searching for a proper compression scheme.
 * Naive creation of all patterns in increasing number of bits
 */

#define STEP MOSAIC_METHODS
static int
makepatterns(int *patterns, int size, str compressions)
{
	int i,j,k, idx, bit=1, step = MOSAIC_METHODS - 1;
	int lim= 8*7*6*5*4*3*2;
	int candidate[MOSAIC_METHODS]= {0};

	for( i = 0; i < MOSAIC_METHODS; i++)
		candidate[i] = compressions == NULL || strstr(compressions,MOSfiltername[i]) != 0;

	for( k=0, i=0; i<lim && k <size; i++){
		patterns[k]=0;
		idx =i;
		while(idx > 0) {
			if( idx % step && candidate[ idx % step]) 
					patterns[k] |= 1 <<(idx % step);
			idx /= step;
		}

		// weed out duplicates
		for( j=0; j< k; j++)
			if(patterns[k] == patterns[j]) break;
		if( j < k ) continue;
		
#ifdef _MOSAIC_DEBUG_
		mnstr_printf(GDKout,"#");
		for(j=0, bit=1; j < MOSAIC_METHODS-1; j++){
			mnstr_printf(GDKout,"%d", (patterns[k] & bit) > 0);
			bit *=2;
		}
		mnstr_printf(GDKout,"\n");
#else
		(void) bit;
#endif
		k++;
	}
#ifdef _MOSAIC_DEBUG_
	mnstr_printf(GDKout,"lim %d k %d\n",lim,k);
#endif
	return k;
}
		
int
MOSanalyseInternal(Client cntxt, int threshold, MOStask task, bat bid)
{
	BAT *b;
	int ret = 0;
	str type;

	b = BATdescriptor(bid);
	if( b == NULL ){
		mnstr_printf(cntxt->fdout,"#nonaccessible %d\n",bid);
		return 0;
	}
	if( b->ttype == TYPE_void ||  BATcount(b) < (BUN) threshold){
		BBPunfix(bid);
		//mnstr_printf(cntxt->fdout,"#too small %d %s\n",bid, BBP_logical(bid));
		return 0;
	}
	if ( isVIEW(b) || VIEWtparent(b)) {
		mnstr_printf(cntxt->fdout,"#ignore view %d %s\n",bid, BBP_logical(bid));
		BBPunfix(bid);
		return 0;
	}
	if ( BATcheckmosaic(b)){
		mnstr_printf(cntxt->fdout,"#already compressed %d %s\n",bid, BBP_logical(bid));
		BBPunfix(bid);
		return 0;
	}
	if ( BATcount(b) < MOSAIC_THRESHOLD  ){
		mnstr_printf(cntxt->fdout,"#ignore small %d %s\n",bid, BBP_logical(bid));
		BBPunfix(bid);
		return 0;
	}
	type = getTypeName(b->ttype);
	switch( b->ttype){
	case TYPE_bit:
	case TYPE_bte:
	case TYPE_sht:
	case TYPE_int:
	case TYPE_lng:
	case TYPE_oid:
	case TYPE_flt:
	case TYPE_dbl:
#ifdef HAVE_HGE
	case TYPE_hge:
#endif
	case TYPE_str:
		mnstr_printf(cntxt->fdout,"#%d\t%-8s\t%s\t"BUNFMT"\t", bid, BBP_physical(bid), type, BATcount(b));
		MOScompressInternal(cntxt, &ret, &bid, task,TRUE);
		MOSdestroy(BBPdescriptor(bid));
		if( ret != b->batCacheid) 
			BBPdecref(ret, TRUE);
		break;
	default:
		if( b->ttype == TYPE_timestamp || b->ttype == TYPE_date || b->ttype == TYPE_daytime){
			mnstr_printf(cntxt->fdout,"#%d\t%-8s\t%s\t"BUNFMT"\t", bid, BBP_physical(bid), type, BATcount(b));
			MOScompressInternal(cntxt, &ret, &bid, task,TRUE);
			MOSdestroy(BBPdescriptor(bid));
			if( ret != b->batCacheid)
				BBPdecref(ret, TRUE);
		} else
			mnstr_printf(cntxt->fdout,"#%d\t%-8s\t%s\t"BUNFMT"\t illegal compression type %s\n", bid, BBP_logical(bid), type, BATcount(b), getTypeName(b->ttype));
		;
	}
	GDKfree(type);
	BBPunfix(bid);
	return 1;
}

#define CANDIDATES 256  /* all three combinations */
void
MOSanalyseReport(Client cntxt, BAT *b, BAT *btech, BAT *boutput, BAT *bratio, BAT *brun, str compressions)
{
	int i,j,k,cases, bit=1, ret, bid= b->batCacheid;
	BUN cnt=  BATcount(b), xsize;
	lng input;
	MOStask task;
	int pattern[CANDIDATES];
	char technique[CANDIDATES]={0}, *t =  technique;
	dbl xf[CANDIDATES], ratio;
	lng clk;

	cases = makepatterns(pattern,CANDIDATES, compressions);
	task = (MOStask) GDKzalloc(sizeof(*task));
	if( task == NULL)
		return;
	for( i = 0; i < CANDIDATES; i++)
		xf[i]= -1;
	input = cnt * ATOMsize(b->ttype);
	for( i = 1; i< cases; i++){
		// filter in-effective sub-patterns
		for( j=0; j < i; j++)
			if ( (pattern[i] & pattern[j]) == pattern[j] && xf[j]== 0) break;
		if( j<i ) continue;
		for(j=0, bit=1; j < MOSAIC_METHODS-1; j++){
			task->filter[j]= (pattern[i] & bit)>0;
			task->range[j]= 0;
			task->factor[j]= 0.0;
			bit *=2;
		}
		clk = GDKms();
		MOScompressInternal(cntxt, &ret, &bid, task, 0);
		clk = GDKms() - clk;
		
		if( task->hdr == NULL){
			// aborted compression
			MOSdestroy(BBPdescriptor(bid));
			continue;
		}
		// analyse result to detect a new combination
		for(k=0, j=0, bit=1; j < MOSAIC_METHODS-1; j++){
			if ( task->filter[j] && task->hdr->blks[j])
				k |= bit;
			bit *=2;
		}
		for( j=0; j < i; j++)
			if (pattern[j] == k )
				break;
		if( j<i){
			MOSdestroy(BBPdescriptor(bid));
			continue;
		}

		xf[i]= task->ratio;
		if( xf[i] == 0){
			MOSdestroy(BBPdescriptor(bid));
			continue;
		}
		xsize = (BUN) task->bsrc->tmosaic->free;
		BUNappend(boutput,&xsize,FALSE);

		t= technique;
		for ( j=0; j < MOSAIC_METHODS -1; j++)
		if( task->hdr->blks[j]) {
			snprintf(t, 1024-strlen(technique),"%s ", MOSfiltername[j]);
			t= technique + strlen(technique);
		}
		BUNappend(btech,technique,FALSE);
		if( xsize)
			ratio = (input + 0.0)/xsize;
		BUNappend(bratio,&ratio,FALSE);
		BUNappend(brun,&clk,FALSE);

		// get rid of mosaic heap
		MOSdestroy(BBPdescriptor(bid));
		if( ret != bid)
			BBPdecref(ret, TRUE);
	}
	GDKfree(task);
}

/* slice a fixed size atom into thin bte-wide columns, used for experiments */
str
MOSsliceInternal(Client cntxt, bat *slices, BUN size, BAT *b)
{
	BUN i;
	BUN cnt= BATcount(b);
	BAT *bats[8];
	bte *thin[8];
	assert(size < 8);
	(void) cntxt;

	for( i = 0; i< size; i++){
		bats[i] = COLnew((oid)0,TYPE_bte, cnt, TRANSIENT);
		if ( bats[i] == NULL){
			for( ;i>0; i--)
				BBPunfix(bats[--i]->batCacheid);
			throw(MAL,"mosaic.slice", MAL_MALLOC_FAIL);
		}
		slices[i] = bats[i]->batCacheid;
		thin[i]= (bte*) Tloc(bats[i],0);
		BATsetcount(bats[i], cnt);
	}
	switch(b->ttype){
	case TYPE_int:
	{ union {
		unsigned int val;
		bte thin[4];
	  } map;
	  unsigned int *val = (unsigned int*) Tloc(b,0);
	  for(i=0; i < cnt; i++, val++){
		map.val = *val;
		*thin[0] = map.thin[0]; thin[0]++;
		*thin[1] = map.thin[1]; thin[1]++;
		*thin[2] = map.thin[2]; thin[2]++;
		*thin[3] = map.thin[3]; thin[3]++;
	  }
	}
	break;
	case TYPE_lng:
	{ union {
		lng val;
		bte thin[8];
	  } map;
	  unsigned int *val = (unsigned int*) Tloc(b,0);
	  for(i=0; i < cnt; i++, val++){
		map.val = *val;
		*thin[0] = map.thin[0]; thin[0]++;
		*thin[1] = map.thin[1]; thin[1]++;
		*thin[2] = map.thin[2]; thin[2]++;
		*thin[3] = map.thin[3]; thin[3]++;
		*thin[4] = map.thin[4]; thin[4]++;
		*thin[5] = map.thin[5]; thin[5]++;
		*thin[6] = map.thin[6]; thin[6]++;
		*thin[7] = map.thin[7]; thin[7]++;
	  }
	}
	break;
	default:
		assert(0);
	}
	return MAL_SUCCEED;
}

str
MOSslice(Client cntxt, MalBlkPtr mb, MalStkPtr stk, InstrPtr pci)
{
	bat slices[8];
	BAT *b;
	BUN s;
	(void) cntxt;

	s = (BUN) ATOMsize(getArgType(mb,pci,pci->retc));
	if( s > 8)
		throw(MAL,"mosaic.slice", "illegal type witdh");
	b = BATdescriptor(* getArgReference_bat(stk,pci, pci->retc));
	if ( b == NULL)
		throw(MAL,"mosaic.slice", RUNTIME_OBJECT_MISSING);
	return MOSsliceInternal(cntxt, slices, s,b);
}

str
MOSanalyse(Client cntxt, MalBlkPtr mb, MalStkPtr stk, InstrPtr pci)
{
	int i,j,k,limit, cand=1;
	bat bid;
	int threshold= 5;
	str properties[32] ={0};
	float xf[32];
	int top=0,x=0,mx;
	char *c;
	MOStask task;
	(void) mb;
	
	task= (MOStask) GDKzalloc(sizeof(*task));
	if( task == NULL)
		throw(MAL, "mosaic.compress", MAL_MALLOC_FAIL);

	if(pci->argc > 1 && getArgType(mb,pci,1) == TYPE_str){
		c= properties[0] = *getArgReference_str(stk,pci,1);
		top++;
		while ( (c=strchr(c,';'))  && top <32){
			*c++ = 0;
			properties[top++] = c;
		}
		cand++;
	}
	if( top == 0) { properties[0]="mosaic"; top++;}


	if( pci->argc > cand ){
		bid = *getArgReference_bat(stk,pci,cand);
		mx = 0;
		for( j = 0; j < top; j++){
			if( properties[j] && !strstr(properties[j],"mosaic"))
				for( k = 0; k< MOSAIC_METHODS; k++){
					task->filter[k]= strstr(properties[j],MOSfiltername[k]) != 0;
					task->range[k] = 0;
					task->factor[k] = 0.0;
				}
			else
				for( k = 0; k< MOSAIC_METHODS; k++){
					task->filter[k]= 1;
					task->range[k] = 0;
					task->factor[k] = 0.0;
				}
			x+= MOSanalyseInternal(cntxt, threshold, task, bid);
			xf[j]= task->hdr? task->ratio: 0;
			if(xf[mx] < xf[j]) mx =j;
			MOSdestroy(BBPdescriptor(bid));
		}
		if(x >1){
			mnstr_printf(cntxt->fdout,"#all %d ",bid);
			for(j=0;j< top; j++)
				mnstr_printf(cntxt->fdout,"%-15s %9.5f ",properties[j], xf[j]);
			mnstr_printf(cntxt->fdout," BEST %-15s %9.5f\n",properties[mx], xf[mx] );
		}
		x=0;
	} else
    for (limit= getBBPsize(),i = 1; i < limit; i++){
		if (BBP_logical(i) && (BBP_refs(i) || BBP_lrefs(i)) ) 
			for( j = 0; j < top; j++){
				if( properties[j] && !strstr(properties[j],"mosaic"))
					for( k = 0; k< MOSAIC_METHODS; k++){
						task->filter[k]= strstr(properties[j],MOSfiltername[k]) != 0;
						task->range[k] = 0;
						task->factor[k] = 0.0;
					}
				else
					for( k = 0; k< MOSAIC_METHODS; k++){
						task->filter[k]= 1;
						task->range[k] = 0;
						task->factor[k] = 0.0;
					}
				x+= MOSanalyseInternal(cntxt, threshold, task, i);
			MOSdestroy(BBPdescriptor(i));
			xf[j]= task->hdr? task->ratio: 0;
		}
		if( x >1){
			mnstr_printf(cntxt->fdout,"#all %d ",i);
			mx =0;
			for(j=0;j< top; j++){
				mnstr_printf(cntxt->fdout,"%-15s %9.5f ",properties[j], xf[j]);
				if(xf[mx] < xf[j]) mx =j;
			}
			mnstr_printf(cntxt->fdout," BEST %-15s %9.5f\n",properties[mx], xf[mx] );
		}
		x=0;
	}
	GDKfree(task);
	return MAL_SUCCEED;
}

str
MOSoptimizer(Client cntxt, MalBlkPtr mb, MalStkPtr stk, InstrPtr pci)
{
	MOStask task;
	int cases;
	int i, j, k, bit, ret;
	bat bid;
	int pattern[CANDIDATES];
	float  xf[CANDIDATES];

	(void) mb;

	cases = makepatterns(pattern,CANDIDATES,NULL);
	task= (MOStask) GDKzalloc(sizeof(*task));
	if( task == NULL)
		throw(MAL, "mosaic.mosaic", MAL_MALLOC_FAIL);

	bid = *getArgReference_int(stk,pci,1);
	MOSblocklimit = 100000;

	for( i = 0; i < CANDIDATES; i++)
		xf[i]= -1;

	for( i = 1; i< cases; i++){
		// filter in-effective sub-patterns
		for( j=0; j < i; j++)
			if ( (pattern[i] & pattern[j]) == pattern[j] && xf[j]== 0) break;
		if( j<i ) continue;
		for(j=0, bit=1; j < MOSAIC_METHODS-1; j++){
			task->filter[j]= (pattern[i] & bit)>0;
			task->range[j] = 0;
			task->factor[j] = 0.0;
			bit *=2;
		}
#define _DEBUG_MOSAIC_
#ifdef _DEBUG_MOSAIC_
		mnstr_printf(cntxt->fdout,"# %d\t%-8s\t", bid, BBP_physical(bid));
		for( j = 0; j < MOSAIC_METHODS -1; j++)
			mnstr_printf(cntxt->fdout,"%d",task->filter[j]?1:0);
		mnstr_printf(cntxt->fdout,"\t");
		mnstr_flush(cntxt->fdout);
#else
		(void) cntxt;
#endif
		MOScompressInternal(cntxt, &ret, &bid, task, TRUE);
		
		// analyse result to detect a new combination
		for(k=0, j=0, bit=1; j < MOSAIC_METHODS-1; j++){
			if ( task->filter[j] && task->hdr->blks[j])
				k |= bit;
			bit *=2;
		}
		for( j=0; j < i; j++)
			if (pattern[j] == k && task->ratio == xf[j])
				break;
		if( j<i){
			MOSdestroy(BBPdescriptor(bid));
			continue;
		}


		xf[i] = task->ratio;
		MOSdestroy(BBPdescriptor(bid));
		if( ret != bid)
			BBPdecref(ret, TRUE);
	}
	GDKfree(task);
	
	return MAL_SUCCEED;
}

