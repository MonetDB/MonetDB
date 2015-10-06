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
 * The header block contains the mapping from OIDs to chunks
 */

#include "monetdb_config.h"
#include "mosaic.h"
#include "mosaic_hdr.h"

void
MOSdumpHeader(Client cntxt, MOStask task)
{
	MosaicHdr hdr = (MosaicHdr) task->hdr;
	int i=0;

#ifdef _DEBUG_MOSAIC_
	mnstr_printf(cntxt->fdout,"#header block version %d\n", hdr->version);
	mnstr_printf(cntxt->fdout,"#index top %d\n", hdr->top);
	for(i= 0; i< hdr->top; i++)
		mnstr_printf(cntxt->fdout,"#[%d] "OIDFMT" " BUNFMT "\n",i, hdr->oidbase[i], hdr->offset[i]);
#else
	(void) cntxt;
	(void) i;
	(void) hdr;
#endif
}

// add the chunk to the index to facilitate 'fast' OID-based access
void
MOSupdateHeader(Client cntxt, MOStask task)
{
	MosaicHdr hdr = (MosaicHdr) task->hdr;
	BUN minsize;
	int i, j;

	(void) cntxt;
    hdr->blks[MOSgetTag(task->blk)]++;
    hdr->elms[MOSgetTag(task->blk)] += MOSgetCnt(task->blk);
	if( hdr->top < MOSAICINDEX-1 ){
		if( hdr->top == 0){
			hdr->oidbase[hdr->top] = 0;
			hdr->offset[hdr->top] = 0;
			hdr->top++;
		}
		hdr->oidbase[hdr->top] = MOSgetCnt(task->blk)+ hdr->oidbase[hdr->top-1];
		hdr->offset[hdr->top] =  (BUN) (task->dst - (char*) task->hdr);
		hdr->top++;
		MOSdumpHeader(cntxt,task);
		return;
	}
	// compress the index by finding the smallest compressed fragment pair
	hdr->oidbase[hdr->top] = MOSgetCnt(task->blk) + hdr->oidbase[hdr->top-1];
	hdr->offset[hdr->top] =  (BUN) (task->dst - (char*) task->hdr);
	minsize = hdr->offset[2];
	j = 1;
	for( i = 1; i+2 <= hdr->top; i++)
	if ( hdr->offset[i+2] - hdr->offset[i] < minsize ){
		minsize = hdr->offset[i+2] - hdr->offset[i];
		j = i+1;
	}
#ifdef _DEBUG_MOSAIC_
	mnstr_printf(cntxt->fdout,"#ditch entry %d\n",j);
#endif
	// simply remove on element
	for( i = j; i < hdr->top; i++){
		hdr->oidbase[i]  = hdr->oidbase[i+1];
		hdr->offset[i] = hdr->offset[i+1];
	}
	MOSdumpHeader(cntxt,task);
}

void
MOSinitHeader(MOStask task)
{
	MosaicHdr hdr = (MosaicHdr) task->hdr;
	int i;
	for(i=0; i < MOSAIC_METHODS; i++){
		hdr->elms[i] = hdr->blks[i] = 0;
		task->range[i]=0;
		task->factor[i]=0.0;
	}
	hdr->ratio = 0;
	hdr->version = MOSAIC_VERSION;
	hdr->top = 0;
	hdr->checksum.sumlng = 0;
	hdr->checksum2.sumlng = 0;
	for(i=0; i < MOSAICINDEX; i++){
		hdr->oidbase[i] = 0;
		hdr->offset[i] = 0;
	}
	for(i=0; i < 256; i++){
		hdr->dictfreq[i]=0;
		hdr->framefreq[i]=0;
	}
}

// position the task on the mosaic blk to be scanned
void
MOSinitializeScan(Client cntxt, MOStask task, int startblk, int stopblk)
{
	MosaicHdr hdr = (MosaicHdr) task->hdr;

	(void) cntxt;
	assert( startblk >= 0 && startblk < hdr->top);
	assert( stopblk > 0 && stopblk <= hdr->top);
	task->blk = (MosaicBlk) (((char*)task->hdr) + MosaicHdrSize + hdr->offset[startblk]);
	// set the oid range covered
	task->start = hdr->oidbase[startblk];
	task->elm = task->stop = hdr->oidbase[stopblk-1];
}

/* limit the number of elements to consider in a block
 * It should always be smaller then: ~(0377<<MOSshift)
*/
BUN 
MOSlimit(void) {
	if( MOSblocklimit > MOSAICMAXCNT)
		MOSblocklimit = MOSAICMAXCNT;
	return MOSblocklimit;
}
/* allow for experiementation using different block sizes */
